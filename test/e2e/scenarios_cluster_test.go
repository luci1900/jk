//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

func intOf(t *testing.T, what, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("%s = %q, want an integer", what, s)
	}
	return n
}

func hasWildcardRule(rules []rbacv1.PolicyRule) bool {
	for _, r := range rules {
		if len(r.APIGroups) == 1 && r.APIGroups[0] == "*" && len(r.Resources) == 1 && r.Resources[0] == "*" && len(r.Verbs) == 1 && r.Verbs[0] == "*" {
			return true
		}
	}
	return false
}

// TestCluster deploys 3 units with trust: namespace and checks peers, app data, the shared secret, ports and trust.
// The subtests share the deployment and run in order.
func TestCluster(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deployWith(t, ns, 3, func(a *v1alpha1.Application) { a.Spec.Trust = v1alpha1.TrustNamespace })
	waitUnitsActive(t, 6*time.Minute, ns, app, 3, "peers: 2")

	// The operator picks the first unit whose container runs as leader, which is not always unit 0 when units start together.
	var lead int
	var others []int
	eventually(t, time.Minute, "a leader", func() (bool, error) {
		a, err := application(ns)
		if err != nil {
			return false, err
		}
		_, err = fmt.Sscanf(a.Status.Leader, app+"/%d", &lead)
		return err == nil, err
	})
	for n := 0; n < 3; n++ {
		if n != lead {
			others = append(others, n)
		}
	}

	var relID int64
	t.Run("peers", func(t *testing.T) {
		eventually(t, time.Minute, "peer relation id", func() (bool, error) {
			id, err := peerRelationID(ns, app, "cluster")
			relID = id
			return err == nil, err
		})
		if relID <= 0 {
			t.Fatalf("relation id %d", relID)
		}
		for n := 0; n < 3; n++ {
			// Statuses apply immediately but relation bookkeeping is committed when the hook ends, so wait for it.
			eventually(t, time.Minute, fmt.Sprintf("unit %d sees 2 peers", n), func() (bool, error) {
				u, err := unitDataOf(ns, app, n)
				if err != nil {
					return false, err
				}
				rs, ok := u.Spec.Relations[relationKey(relID)]
				return ok && len(rs.Members) == 2, nil
			})
			u, err := unitDataOf(ns, app, n)
			if err != nil {
				t.Fatal(err)
			}
			// the same single relation id, as an integer key, on every unit
			if len(u.Spec.Relations) != 1 {
				t.Fatalf("%s relations %v, want only %d", u.Name, keys(u.Spec.Relations), relID)
			}
			rs, ok := u.Spec.Relations[relationKey(relID)]
			if !ok {
				t.Fatalf("%s relations %v, want %d", u.Name, keys(u.Spec.Relations), relID)
			}
			// each sees the two other units
			if len(rs.Members) != 2 {
				t.Errorf("%s members %v, want 2 peers", u.Name, rs.Members)
			}
			if _, self := rs.Members[app+"/"+strconv.Itoa(n)]; self {
				t.Errorf("%s lists itself as a peer: %v", u.Name, rs.Members)
			}
			if rs.Data["address"] == "" {
				t.Errorf("%s published no address: %v", u.Name, rs.Data)
			}
			if c := intOf(t, u.Name+" counter", rs.Data["counter"]); c < 1 {
				t.Errorf("%s counter %d", u.Name, c)
			}
			if !strings.HasPrefix(u.Spec.WorkloadStatus.Message, "greeting: hello, peers: 2,") {
				t.Errorf("%s status %q", u.Name, u.Spec.WorkloadStatus.Message)
			}
		}
		// addresses are distinct
		seen := map[string]int{}
		for n := 0; n < 3; n++ {
			d, _ := peerUnitData(ns, app, n, relID)
			seen[d["address"]]++
		}
		if len(seen) != 3 {
			t.Errorf("addresses not distinct: %v", seen)
		}
		// hook order on a joining unit: created before joined before changed, nothing failing
		names := waitHooks(t, ns, 1, "cluster-relation-created", "cluster-relation-joined", "cluster-relation-changed")
		// juju may run an application-level relation-changed before the first relation-joined (the leader's app
		// data is already there), so require a relation-changed after the join, not before it.
		lastChanged := -1
		for i, h := range names {
			if h == "cluster-relation-changed" {
				lastChanged = i
			}
		}
		if !(index(names, "cluster-relation-created") < index(names, "cluster-relation-joined") && index(names, "cluster-relation-joined") < lastChanged) {
			t.Errorf("unit 1 relation hook order: %v", names)
		}
		for n := 0; n < 3; n++ {
			for _, h := range parseHooks(agentLogs(t, ns, n)) {
				if h.Exit != 0 {
					t.Errorf("unit %d: hook %s exited %d", n, h.Name, h.Exit)
				}
			}
		}
	})

	t.Run("app-data", func(t *testing.T) {
		eventually(t, time.Minute, "leader app data", func() (bool, error) {
			d, err := peerAppData(ns, app, relID)
			if err != nil {
				return false, err
			}
			if d["leader"] != fmt.Sprintf("%s/%d", app, lead) || d["generation"] == "" || !strings.HasPrefix(d["secret-id"], "secret:") {
				return false, fmt.Errorf("app data %v", d)
			}
			return true, nil
		})
		// the other units saw it: they read the secret whose id only the app data carries
		for _, n := range others {
			eventually(t, time.Minute, fmt.Sprintf("unit %d read the shared secret", n), func() (bool, error) {
				d, err := peerUnitData(ns, app, n, relID)
				if err != nil {
					return false, err
				}
				if d["secret-bump"] != "0" {
					return false, fmt.Errorf("unit data %v", d)
				}
				return true, nil
			})
		}
		// only the leader writes app data
		if d, _ := peerAppData(ns, app, relID); d["generation"] == "" {
			t.Errorf("no generation: %v", d)
		}
	})

	t.Run("secret", func(t *testing.T) {
		d, err := peerUnitData(ns, app, lead, relID)
		if err != nil {
			t.Fatal(err)
		}
		if d["secret-bump"] != "0" || d["secret-revision"] != "1" {
			t.Errorf("leader secret data %v, want bump 0 revision 1", d)
		}
		before := map[int]int{}
		for _, n := range others {
			d, _ := peerUnitData(ns, app, n, relID)
			if d["secret-changes"] != "" {
				before[n] = intOf(t, "secret-changes", d["secret-changes"])
			}
		}

		setConfig(t, ns, map[string]string{"bump-secret": "1"})
		eventually(t, 2*time.Minute, "leader wrote revision 2", func() (bool, error) {
			d, err := peerUnitData(ns, app, lead, relID)
			if err != nil {
				return false, err
			}
			if d["secret-bump"] != "1" || d["secret-revision"] != "2" {
				return false, fmt.Errorf("leader data %v", d)
			}
			return true, nil
		})
		for _, n := range others {
			waitHooks(t, ns, n, "secret-changed")
			eventually(t, 2*time.Minute, fmt.Sprintf("unit %d saw revision 2", n), func() (bool, error) {
				d, err := peerUnitData(ns, app, n, relID)
				if err != nil {
					return false, err
				}
				if d["secret-bump"] != "1" || d["secret-changes"] == "" || intOf(t, "secret-changes", d["secret-changes"]) <= before[n] {
					return false, fmt.Errorf("unit data %v", d)
				}
				return true, nil
			})
		}
		// the owner isn't notified of its own change
		if c := count(hookNames(agentLogs(t, ns, lead)), "secret-changed"); c != 0 {
			t.Errorf("leader ran secret-changed %d times", c)
		}
		// each revision is its own immutable k8s Secret, plus the metadata Secret
		var secrets corev1.SecretList
		if err := kube.List(context.Background(), &secrets, client.InNamespace(ns)); err != nil {
			t.Fatal(err)
		}
		t.Logf("%d Secrets in the model", len(secrets.Items))
	})

	t.Run("ports", func(t *testing.T) {
		eventually(t, time.Minute, "Service ports from opened ports", func() (bool, error) {
			var svc corev1.Service
			if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &svc); err != nil {
				return false, err
			}
			if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8080 || svc.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
				return false, fmt.Errorf("service ports %+v", svc.Spec.Ports)
			}
			return true, nil
		})
	})

	t.Run("trust", func(t *testing.T) {
		var role rbacv1.Role
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &role); err != nil {
			t.Fatal(err)
		}
		if !hasWildcardRule(role.Rules) {
			t.Errorf("trust: namespace Role has no full-access rule: %+v", role.Rules)
		}
	})
}

// TestNoTrust checks that without trust the app's Role has no wildcard rule, and that Service ports follow opened ports.
func TestNoTrust(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")
	var role rbacv1.Role
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &role); err != nil {
		t.Fatal(err)
	}
	if hasWildcardRule(role.Rules) {
		t.Errorf("trust none Role has a full-access rule: %+v", role.Rules)
	}
	eventually(t, time.Minute, "Service port 8080", func() (bool, error) {
		var svc corev1.Service
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &svc); err != nil {
			return false, err
		}
		for _, p := range svc.Spec.Ports {
			if p.Port == 65535 {
				return false, fmt.Errorf("placeholder port still there: %+v", svc.Spec.Ports)
			}
		}
		if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8080 {
			return false, fmt.Errorf("ports %+v", svc.Spec.Ports)
		}
		return true, nil
	})
}

// storageMarker returns a unit's storage marker and first event from its peer unit data.
func storageMarker(t *testing.T, ns string, n int, relID int64) (marker, first string) {
	t.Helper()
	eventually(t, 2*time.Minute, fmt.Sprintf("unit %d storage marker", n), func() (bool, error) {
		d, err := peerUnitData(ns, app, n, relID)
		if err != nil {
			return false, err
		}
		marker, first = d["storage-marker"], d["storage-first-event"]
		if marker == "" {
			return false, fmt.Errorf("unit data %v", d)
		}
		return true, nil
	})
	return marker, first
}

// TestStorageScale checks storage-attached right after install (juju's CAAS order), retained PVs on scale-down and a fresh volume on scale-up.
func TestStorageScale(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 3, nil)
	waitUnitsActive(t, 6*time.Minute, ns, app, 3, "peers: 2")
	var relID int64
	eventually(t, time.Minute, "peer relation id", func() (bool, error) {
		var err error
		relID, err = peerRelationID(ns, app, "cluster")
		return err == nil, err
	})

	markers := map[int]string{}
	pvs := map[int][]string{}
	for n := 0; n < 3; n++ {
		m, first := storageMarker(t, ns, n, relID)
		markers[n] = m
		// The volume is mounted from pod start, so install writes the marker; juju's CAAS resolver runs
		// storage-attached after install (after leader-elected on the leader), before config-changed.
		if first != "install" {
			t.Errorf("unit %d: marker first written by %q, want install", n, first)
		}
		names := waitHooks(t, ns, n, "data-storage-attached", "install", "start")
		if index(names, "data-storage-attached") < index(names, "install") || index(names, "data-storage-attached") > index(names, "config-changed") {
			t.Errorf("unit %d: storage-attached should follow install and precede config-changed: %v", n, names)
		}
		pvs[n] = podPVs(t, ns, unitName(n))
		if len(pvs[n]) != 1 {
			t.Fatalf("unit %d PVs %v, want 1", n, pvs[n])
		}
		waitPVsRetained(t, ns, pvs[n])
	}
	// the volume is mounted in the workload container too
	var pod corev1.Pod
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: unitName(0)}, &pod); err != nil {
		t.Fatal(err)
	}
	mounted := false
	for _, c := range pod.Spec.Containers {
		if c.Name != "web" {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.MountPath == "/data" {
				mounted = true
			}
		}
	}
	if !mounted {
		t.Errorf("web container has no /data mount")
	}

	setScale(t, ns, 1)
	eventually(t, readyTimeout, "units 1 and 2 gone", func() (bool, error) {
		for _, n := range []int{1, 2} {
			if ok, err := gone(&corev1.Pod{}, client.ObjectKey{Namespace: ns, Name: unitName(n)})(); !ok {
				return false, err
			}
			if ok, err := gone(&v1alpha1.UnitData{}, client.ObjectKey{Namespace: ns, Name: unitName(n)})(); !ok {
				return false, err
			}
		}
		return true, nil
	})
	waitPVsReleased(t, ns, append(append([]string{}, pvs[1]...), pvs[2]...))
	if pv, err := pvByName(ns, pvs[0][0]); err != nil || pv.Status.Phase != corev1.VolumeBound {
		t.Errorf("unit 0's PV %v err=%v, want Bound", pv.Status.Phase, err)
	}
	waitUnitActive(t, ns, 0, "peers: 0")
	// The status is set while the hook runs; the hook is only logged as finished afterwards.
	var departed int
	eventually(t, time.Minute, "two cluster-relation-departed hooks finished", func() (bool, error) {
		departed = count(hookNames(agentLogs(t, ns, 0)), "cluster-relation-departed")
		return departed >= 2, nil
	})
	if departed != 2 {
		t.Errorf("unit 0 ran cluster-relation-departed %d times, want 2", departed)
	}

	setScale(t, ns, 3)
	waitUnitsActive(t, 6*time.Minute, ns, app, 3, "peers: 2")
	for _, n := range []int{1, 2} {
		m, first := storageMarker(t, ns, n, relID)
		if m == markers[n] {
			t.Errorf("unit %d reattached the old volume (marker %s)", n, m)
		}
		if first != "install" {
			t.Errorf("unit %d: marker first written by %q, want install", n, first)
		}
		now := podPVs(t, ns, unitName(n))
		if len(now) != 1 || now[0] == pvs[n][0] {
			t.Errorf("unit %d PVs %v, old %v: want a new PV", n, now, pvs[n])
		}
		waitPVsRetained(t, ns, now)
	}
	if m, _ := storageMarker(t, ns, 0, relID); m != markers[0] {
		t.Errorf("unit 0's volume changed")
	}
	// the old volumes are still there, released
	waitPVsReleased(t, ns, append(append([]string{}, pvs[1]...), pvs[2]...))
}

// TestDestroyStorage checks that deleting the Application with the destroy-storage annotation deletes the PVs, and without it keeps them.
func TestDestroyStorage(t *testing.T) {
	t.Parallel()
	for _, destroy := range []bool{false, true} {
		t.Run(fmt.Sprintf("destroy=%v", destroy), func(t *testing.T) {
			t.Parallel()
			ns := newModel(t)
			deploy(t, ns, 2, nil)
			waitUnitsActive(t, 6*time.Minute, ns, app, 2, "greeting")
			var names []string
			for n := 0; n < 2; n++ {
				names = append(names, podPVs(t, ns, unitName(n))...)
			}
			waitPVsRetained(t, ns, names)

			a, err := application(ns)
			if err != nil {
				t.Fatal(err)
			}
			if destroy {
				if a.Annotations == nil {
					a.Annotations = map[string]string{}
				}
				a.Annotations["jk.luci1900.github.io/destroy-storage"] = "true"
				if err := kube.Update(context.Background(), a); err != nil {
					t.Fatal(err)
				}
			}
			if err := kube.Delete(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			eventually(t, readyTimeout, "application gone", gone(&v1alpha1.Application{}, client.ObjectKey{Namespace: ns, Name: app}))
			if destroy {
				eventually(t, 2*time.Minute, "PVs deleted", func() (bool, error) {
					for _, n := range names {
						if ok, err := gone(&corev1.PersistentVolume{}, client.ObjectKey{Name: n})(); !ok {
							return false, err
						}
					}
					return true, nil
				})
				return
			}
			waitPVsReleased(t, ns, names)
		})
	}
}

// TestStatefulSetPatchSurvives checks the operator's server-side apply leaves fields a charm patches alone (postgres sets the rollout partition).
func TestStatefulSetPatchSurvives(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")
	patch := []byte(`{"spec":{"updateStrategy":{"type":"RollingUpdate","rollingUpdate":{"partition":5}}}}`)
	sts := &appsv1.StatefulSet{}
	sts.Namespace, sts.Name = ns, app
	if err := kube.Patch(context.Background(), sts, client.RawPatch(types.MergePatchType, patch)); err != nil {
		t.Fatal(err)
	}
	// force reconciles: scale up (the operator applies the StatefulSet again) and a config change
	setScale(t, ns, 2)
	waitUnitActive(t, ns, 1, "hello")
	setConfig(t, ns, map[string]string{"greeting": "hola"})
	waitUnitActive(t, ns, 0, "hola")
	for i := 0; i < 5; i++ {
		// Each round the operator reconciles again for a config change it has just handled (the unit reports it).
		greeting := fmt.Sprintf("round%d", i)
		setConfig(t, ns, map[string]string{"greeting": greeting})
		waitUnitActive(t, ns, 0, greeting)
		var got appsv1.StatefulSet
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &got); err != nil {
			t.Fatal(err)
		}
		ru := got.Spec.UpdateStrategy.RollingUpdate
		if ru == nil || ru.Partition == nil || *ru.Partition != 5 {
			t.Fatalf("partition reverted: %+v", got.Spec.UpdateStrategy)
		}
	}
}
