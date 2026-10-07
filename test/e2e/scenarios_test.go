//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// updateApp changes the Application, retrying on conflicts with the operator's status writes.
func updateApp(t *testing.T, ns string, mutate func(*v1alpha1.Application)) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		a, err := application(ns)
		if err != nil {
			return err
		}
		mutate(a)
		return kube.Update(context.Background(), a)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func setConfig(t *testing.T, ns string, cfg map[string]string) {
	t.Helper()
	updateApp(t, ns, func(a *v1alpha1.Application) { a.Spec.Config = cfg })
}

func setScale(t *testing.T, ns string, n int32) {
	t.Helper()
	updateApp(t, ns, func(a *v1alpha1.Application) { a.Spec.Scale = &n })
}

func leader(t *testing.T, ns string) string {
	t.Helper()
	a, err := application(ns)
	if err != nil {
		t.Fatal(err)
	}
	return a.Status.Leader
}

func TestDeploy(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")

	// status.charm is read from the registry
	eventually(t, time.Minute, "status.charm", func() (bool, error) {
		a, err := application(ns)
		if err != nil {
			return false, err
		}
		return a.Status.Charm != nil && a.Status.Charm.Metadata != nil, nil
	})

	// The container runs a fixed sequence for a fresh leader. The relative order of the Pebble-ready hook
	// is juju's business; assert the set and the ordering constraints the agent guarantees.
	names := waitHooks(t, ns, 0, "install", "leader-elected", "config-changed", "start", "web-pebble-ready")
	for _, h := range []string{"install", "leader-elected", "config-changed", "start", "web-pebble-ready"} {
		if count(names, h) != 1 {
			t.Errorf("hook %s ran %d times: %v", h, count(names, h), names)
		}
	}
	if index(names, "install") != 0 {
		t.Errorf("install is not first: %v", names)
	}
	if index(names, "install") > index(names, "start") || index(names, "config-changed") > index(names, "start") {
		t.Errorf("want install and config-changed before start: %v", names)
	}
	for _, h := range parseHooks(agentLogs(t, ns, 0)) {
		if h.Exit != 0 {
			t.Errorf("hook %s exited %d", h.Name, h.Exit)
		}
	}

	// unit, application and leader status
	// The peer relation can still be running hooks when the workload turns active, so wait for idle.
	eventually(t, time.Minute, "agent idle", func() (bool, error) {
		u, err := unitData(ns, 0)
		if err != nil {
			return false, err
		}
		return u.Spec.AgentStatus != nil && u.Spec.AgentStatus.State == "idle", nil
	})
	u, err := unitData(ns, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The peer relation adds ", peers: N" to the message (the operator creates the peer Relation)
	if !strings.HasPrefix(u.Spec.WorkloadStatus.Message, "greeting: hello") {
		t.Errorf("workload message %q", u.Spec.WorkloadStatus.Message)
	}
	if len(u.Spec.OpenedPorts) != 1 || u.Spec.OpenedPorts[0].From != 8080 {
		t.Errorf("opened ports %+v", u.Spec.OpenedPorts)
	}
	eventually(t, time.Minute, "app status active and leader", func() (bool, error) {
		ad, err := appData(ns)
		if err != nil {
			return false, err
		}
		if ad.Spec.Status == nil || ad.Spec.Status.State != "active" {
			return false, nil
		}
		return leader(t, ns) == app+"/0", nil
	})
}

func TestConfigChange(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")
	before := count(waitHooks(t, ns, 0, "config-changed"), "config-changed")

	setConfig(t, ns, map[string]string{"greeting": "bonjour"})
	waitUnitActive(t, ns, 0, "bonjour")
	// The status is set while the hook runs; the hook is only logged as finished afterwards.
	var names []string
	eventually(t, time.Minute, "config-changed hook finished", func() (bool, error) {
		names = hookNames(agentLogs(t, ns, 0))
		return count(names, "config-changed") >= before+1, nil
	})
	if got := count(names, "config-changed"); got != before+1 {
		t.Errorf("config-changed ran %d times, want %d: %v", got, before+1, names)
	}
	if count(names, "install") != 1 {
		t.Errorf("install reran: %v", names)
	}
}

func TestScale(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")
	eventually(t, time.Minute, "leader app/0", func() (bool, error) { return leader(t, ns) == app+"/0", nil })

	setScale(t, ns, 2)
	waitUnitActive(t, ns, 1, "hello")
	names := waitHooks(t, ns, 1, "install", "config-changed", "start")
	if index(names, "install") != 0 {
		t.Errorf("unit 1: install not first: %v", names)
	}
	if count(names, "leader-elected") != 0 {
		t.Errorf("non-leader unit 1 ran leader-elected: %v", names)
	}
	if l := leader(t, ns); l != app+"/0" {
		t.Errorf("leader moved to %q after scale up", l)
	}
	if n0 := hookNames(agentLogs(t, ns, 0)); count(n0, "install") != 1 || count(n0, "leader-elected") != 1 {
		t.Errorf("unit 0 reran hooks on scale up: %v", n0)
	}

	setScale(t, ns, 1)
	eventually(t, readyTimeout, "unit 1 pod and UnitData gone", func() (bool, error) {
		if ok, _ := gone(&corev1.Pod{}, client.ObjectKey{Namespace: ns, Name: unitName(1)})(); !ok {
			return false, nil
		}
		return gone(&v1alpha1.UnitData{}, client.ObjectKey{Namespace: ns, Name: unitName(1)})()
	})
	if l := leader(t, ns); l != app+"/0" {
		t.Errorf("leader %q after scale down, want %s/0", l, app)
	}
	if ok, err := podReady(ns, unitName(0)); !ok {
		t.Errorf("unit 0 not ready after scale down: %v", err)
	}
}

func TestPodKill(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")
	waitHooks(t, ns, 0, "start")
	oldUID := podUID(t, ns, unitName(0))

	if err := kube.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: unitName(0)}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, readyTimeout, "replacement pod ready", func() (bool, error) {
		var p corev1.Pod
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: unitName(0)}, &p); err != nil {
			return false, err
		}
		if string(p.UID) == oldUID {
			return false, nil
		}
		return podReady(ns, unitName(0))
	})
	waitUnitActive(t, ns, 0, "hello")
	// The new pod's log covers its whole life: the persisted UnitData means install doesn't run again.
	// (The pod is ready before its pebble-ready hook has run.)
	names := waitHooks(t, ns, 0, "web-pebble-ready")
	if count(names, "install") != 0 {
		t.Errorf("install reran after pod kill: %v", names)
	}
	if count(names, "web-pebble-ready") != 1 {
		t.Errorf("want web-pebble-ready on the new pod: %v", names)
	}
}

func TestRemove(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	waitUnitActive(t, ns, 0, "hello")
	waitHooks(t, ns, 0, "start")
	logs := follow(ns, unitName(0))

	a, err := application(ns)
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	eventually(t, readyTimeout, "application gone", gone(&v1alpha1.Application{}, client.ObjectKey{Namespace: ns, Name: app}))
	select {
	case <-logs.done:
	case <-time.After(30 * time.Second):
	}
	if !strings.Contains(logs.String(), "jk-agent: hook stop finished exit=0") {
		t.Errorf("no successful stop hook in agent log:\n%s", logs.String())
	}
	eventually(t, time.Minute, "objects gone", func() (bool, error) {
		key := client.ObjectKey{Namespace: ns, Name: app}
		for _, o := range []client.Object{&appsv1.StatefulSet{}, &corev1.Service{}, &v1alpha1.AppData{}} {
			if ok, err := gone(o, key)(); !ok {
				return false, err
			}
		}
		if ok, err := gone(&corev1.Service{}, client.ObjectKey{Namespace: ns, Name: app + "-endpoints"})(); !ok {
			return false, err
		}
		if ok, err := gone(&v1alpha1.UnitData{}, client.ObjectKey{Namespace: ns, Name: unitName(0)})(); !ok {
			return false, err
		}
		return gone(&corev1.Pod{}, client.ObjectKey{Namespace: ns, Name: unitName(0)})()
	})
}
