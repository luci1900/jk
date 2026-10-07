//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// ---- generic (any app name) accessors; the helpers in the main harness are fixed to the jk-test app ----

func unitOf(appName string, n int) string { return fmt.Sprintf("%s-%d", appName, n) }

func applicationOf(ns, appName string) (*v1alpha1.Application, error) {
	var a v1alpha1.Application
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: appName}, &a)
	return &a, err
}

func unitDataOf(ns, appName string, n int) (*v1alpha1.UnitData, error) {
	var u v1alpha1.UnitData
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: unitOf(appName, n)}, &u)
	return &u, err
}

func appDataOf(ns, appName string) (*v1alpha1.AppData, error) {
	var a v1alpha1.AppData
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: appName}, &a)
	return &a, err
}

// setScaleOf sets spec.scale of the named Application, retrying on conflicts with the operator's status writes.
func setScaleOf(t *testing.T, ns, appName string, n int32) {
	t.Helper()
	for i := 0; ; i++ {
		a, err := applicationOf(ns, appName)
		if err != nil {
			t.Fatal(err)
		}
		a.Spec.Scale = &n
		if err = kube.Update(context.Background(), a); err == nil {
			return
		} else if i > 5 {
			t.Fatal(err)
		}
	}
}

// deployCharmhub creates a bare Charmhub Application (only name and channel set; the operator resolves the rest).
func deployCharmhub(t *testing.T, ns, name, channel string, scale int32, trust v1alpha1.Trust) *v1alpha1.Application {
	t.Helper()
	a := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.ApplicationSpec{
			Charm: v1alpha1.CharmSpec{Name: name, Channel: channel},
			Scale: &scale,
			Trust: trust,
		},
	}
	if err := kube.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return a
}

// ---- status ----

// workloadStatus returns unit n's workload status (nil while unset).
func workloadStatus(ns, appName string, n int) (*v1alpha1.WorkloadStatus, error) {
	u, err := unitDataOf(ns, appName, n)
	if err != nil {
		return nil, err
	}
	return u.Spec.WorkloadStatus, nil
}

// unitsStatus returns "state: message" of units 0..scale-1, for failure messages.
func unitsStatus(ns, appName string, scale int) string {
	var parts []string
	for n := 0; n < scale; n++ {
		s, err := workloadStatus(ns, appName, n)
		switch {
		case err != nil:
			parts = append(parts, fmt.Sprintf("%d: %v", n, err))
		case s == nil:
			parts = append(parts, fmt.Sprintf("%d: <unset>", n))
		default:
			parts = append(parts, fmt.Sprintf("%d: %s %q", n, s.State, s.Message))
		}
	}
	return strings.Join(parts, "; ")
}

// waitUnitsActive waits until units 0..scale-1 have ready pods and an active workload status whose message contains msg.
func waitUnitsActive(t *testing.T, timeout time.Duration, ns, appName string, scale int, msg string) {
	t.Helper()
	eventually(t, timeout, fmt.Sprintf("%d units of %s active (%q)", scale, appName, msg), func() (bool, error) {
		for n := 0; n < scale; n++ {
			if ok, err := podReady(ns, unitOf(appName, n)); !ok || err != nil {
				return false, fmt.Errorf("pod %d ready=%v err=%v; %s", n, ok, err, unitsStatus(ns, appName, scale))
			}
			s, err := workloadStatus(ns, appName, n)
			if err != nil {
				return false, err
			}
			if s == nil || s.State != "active" || !strings.Contains(s.Message, msg) {
				return false, fmt.Errorf("%s", unitsStatus(ns, appName, scale))
			}
		}
		return true, nil
	})
}

// ---- relations ----

// peerRelationID returns the id of the peer Relation of appName's endpoint (0 and an error until the operator sets it).
func peerRelationID(ns, appName, endpoint string) (int64, error) {
	var rels v1alpha1.RelationList
	if err := kube.List(context.Background(), &rels, client.InNamespace(ns)); err != nil {
		return 0, err
	}
	for _, r := range rels.Items {
		if len(r.Spec.Endpoints) == 1 && r.Spec.Endpoints[0].Application == appName && r.Spec.Endpoints[0].Endpoint == endpoint {
			if r.Status.ID == 0 {
				return 0, fmt.Errorf("relation %s has no id yet", r.Name)
			}
			return r.Status.ID, nil
		}
	}
	return 0, fmt.Errorf("no peer relation for %s:%s in %s (%d relations)", appName, endpoint, ns, len(rels.Items))
}

// relationKey is how UnitData and AppData key a relation.
func relationKey(id int64) string { return strconv.FormatInt(id, 10) }

// ---- volumes ----

// pvsOf lists the PVs claimed (now or when they were released) by PVCs of the namespace.
func pvsOf(ns string) []corev1.PersistentVolume {
	var all corev1.PersistentVolumeList
	if err := kube.List(context.Background(), &all); err != nil {
		return nil
	}
	var out []corev1.PersistentVolume
	for _, pv := range all.Items {
		if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == ns {
			out = append(out, pv)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func pvByName(ns, name string) (*corev1.PersistentVolume, error) {
	var pv corev1.PersistentVolume
	err := kube.Get(context.Background(), client.ObjectKey{Name: name}, &pv)
	return &pv, err
}

// podPVs returns the names of the PVs bound to the PVCs of the running pod.
func podPVs(t *testing.T, ns, pod string) []string {
	t.Helper()
	var p corev1.Pod
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: pod}, &p); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, v := range p.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		var pvc corev1.PersistentVolumeClaim
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: v.PersistentVolumeClaim.ClaimName}, &pvc); err != nil {
			t.Fatal(err)
		}
		if pvc.Spec.VolumeName != "" {
			out = append(out, pvc.Spec.VolumeName)
		}
	}
	sort.Strings(out)
	return out
}

// waitPVsRetained waits until every named PV exists with reclaim policy Retain.
func waitPVsRetained(t *testing.T, ns string, names []string) {
	t.Helper()
	eventually(t, time.Minute, fmt.Sprintf("PVs %v set to Retain", names), func() (bool, error) {
		for _, n := range names {
			pv, err := pvByName(ns, n)
			if err != nil {
				return false, err
			}
			if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				return false, fmt.Errorf("pv %s reclaim policy %s", n, pv.Spec.PersistentVolumeReclaimPolicy)
			}
		}
		return true, nil
	})
}

// waitPVsReleased waits until every named PV still exists, is Released and Retain.
func waitPVsReleased(t *testing.T, ns string, names []string) {
	t.Helper()
	eventually(t, 2*time.Minute, fmt.Sprintf("PVs %v released", names), func() (bool, error) {
		for _, n := range names {
			pv, err := pvByName(ns, n)
			if err != nil {
				return false, err
			}
			if pv.Status.Phase != corev1.VolumeReleased || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				return false, fmt.Errorf("pv %s phase %s reclaim %s", n, pv.Status.Phase, pv.Spec.PersistentVolumeReclaimPolicy)
			}
		}
		return true, nil
	})
}

// ---- unit and app data views ----

// peerUnitData returns unit n's own data in the peer relation.
func peerUnitData(ns, appName string, n int, relID int64) (map[string]string, error) {
	u, err := unitDataOf(ns, appName, n)
	if err != nil {
		return nil, err
	}
	rs, ok := u.Spec.Relations[relationKey(relID)]
	if !ok {
		return nil, fmt.Errorf("%s has no state for relation %d (has %v)", u.Name, relID, keys(u.Spec.Relations))
	}
	return rs.Data, nil
}

// peerAppData returns the application's data in the peer relation.
func peerAppData(ns, appName string, relID int64) (map[string]string, error) {
	a, err := appDataOf(ns, appName)
	if err != nil {
		return nil, err
	}
	d, ok := a.Spec.Relations[relationKey(relID)]
	if !ok {
		return nil, fmt.Errorf("appdata has no data for relation %d", relID)
	}
	return d, nil
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// setUpdateStatusInterval shortens the model's update-status interval (default 5m). Charms such as postgresql-k8s only
// refresh their status message in hooks, so a scenario that waits on a status after the cluster changes would
// otherwise idle until the next timer.
func setUpdateStatusInterval(t *testing.T, ns, interval string) {
	t.Helper()
	eventually(t, time.Minute, "jk-model ConfigMap", func() (bool, error) {
		var cm corev1.ConfigMap
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: v1alpha1.ModelConfigMap}, &cm); err != nil {
			return false, nil
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["model-config.update-status-hook-interval"] = interval
		return true, kube.Update(context.Background(), &cm)
	})
}
