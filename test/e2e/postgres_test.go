//go:build e2e && postgres

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// The postgresql-k8s HA scenarios: postgresql-k8s 14/stable from Charmhub, 3 units, a pod kill, scale 3→1→3.
// Run with `make test-e2e-postgres`. Image pulls are large and the kind node may be arm64, so timeouts are generous.
const (
	pg           = "postgresql-k8s"
	pgChannel    = "14/stable"
	pgFirstReady = 30 * time.Minute // charm download, two image pulls, patroni bootstrap
	pgRecover    = 15 * time.Minute // after a pod kill or a scale operation
)

// pgRoles returns the index of the unit with the workload message "Primary", failing unless all `scale` units are active and exactly one is primary.
func pgRoles(ns string, scale int) (int, error) {
	primary, nPrimary := -1, 0
	for n := 0; n < scale; n++ {
		if ok, err := podReady(ns, unitOf(pg, n)); !ok || err != nil {
			return -1, fmt.Errorf("pod %d ready=%v err=%v; %s", n, ok, err, unitsStatus(ns, pg, scale))
		}
		s, err := workloadStatus(ns, pg, n)
		if err != nil {
			return -1, err
		}
		if s == nil || s.State != "active" {
			return -1, fmt.Errorf("%s", unitsStatus(ns, pg, scale))
		}
		if s.Message == "Primary" {
			primary = n
			nPrimary++
		} else if s.Message != "" {
			return -1, fmt.Errorf("unexpected message on unit %d: %s", n, unitsStatus(ns, pg, scale))
		}
	}
	if nPrimary != 1 {
		return -1, fmt.Errorf("%d primaries: %s", nPrimary, unitsStatus(ns, pg, scale))
	}
	return primary, nil
}

// waitPGHealthy waits for `scale` active units with exactly one Primary and returns its ordinal.
func waitPGHealthy(t *testing.T, timeout time.Duration, ns string, scale int) int {
	t.Helper()
	primary := -1
	eventually(t, timeout, fmt.Sprintf("%d healthy %s units with one Primary", scale, pg), func() (bool, error) {
		p, err := pgRoles(ns, scale)
		primary = p
		return err == nil, err
	})
	return primary
}

func pgPVs(t *testing.T, ns string, scale int) map[int][]string {
	out := map[int][]string{}
	for n := 0; n < scale; n++ {
		out[n] = podPVs(t, ns, unitOf(pg, n))
	}
	return out
}

func flatten(m map[int][]string, units ...int) []string {
	var out []string
	for _, n := range units {
		out = append(out, m[n]...)
	}
	return out
}

func TestPostgresHA(t *testing.T) {
	ns := newModel(t)
	begin := time.Now()
	// lap logs the time since the scenario started, so a slow run shows which stage is slow.
	lap := func(what string) { t.Logf("[%s] %s", time.Since(begin).Round(time.Second), what) }
	// The charm refreshes its Primary status message from hooks; do not wait the default 5 minutes for update-status.
	setUpdateStatusInterval(t, ns, "15s")
	deployCharmhub(t, ns, pg, pgChannel, 3, v1alpha1.TrustCluster) // the charm reads nodes: juju's plain --trust is cluster scope

	// 1. The operator resolves the charm from Charmhub.
	eventually(t, 5*time.Minute, "status.charm resolved", func() (bool, error) {
		a, err := applicationOf(ns, pg)
		if err != nil {
			return false, err
		}
		if a.Status.Charm == nil || a.Status.Charm.Revision == 0 || a.Status.Charm.Sha256 == "" || a.Status.Charm.Image == "" {
			return false, fmt.Errorf("status.charm %+v conditions %+v", a.Status.Charm, a.Status.Conditions)
		}
		return true, nil
	})

	// 2. Three units become active, one of them Primary and the others without a message.
	primary := waitPGHealthy(t, pgFirstReady, ns, 3)
	t.Logf("primary is unit %d", primary)
	waitPatroniSynced(t, pgRecover, ns, 3)
	lap("three units healthy after deploy")
	if a, _ := applicationOf(ns, pg); a != nil {
		t.Logf("resolved charm: revision %d base %s", a.Status.Charm.Revision, a.Status.Charm.Base)
	}
	pvs := pgPVs(t, ns, 3)
	waitPVsRetained(t, ns, flatten(pvs, 0, 1, 2))

	// 3. Kill the primary's pod: the cluster recovers with one primary, and the old unit is a replica if another was elected.
	old := primary
	oldUID := podUID(t, ns, unitOf(pg, old))
	if err := kube.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: unitOf(pg, old)}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, pgRecover, "replacement pod of the old primary", func() (bool, error) {
		var p corev1.Pod
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: unitOf(pg, old)}, &p); err != nil {
			return false, err
		}
		return string(p.UID) != oldUID, nil
	})
	primary = waitPGHealthy(t, pgRecover, ns, 3)
	t.Logf("after the kill, primary is unit %d", primary)
	waitPatroniSynced(t, pgRecover, ns, 3)
	lap("recovered after killing the primary")
	// Patroni keeps the old primary as leader when its pod is back before the leader key expires (as on a fast cluster), so a failover is not guaranteed.
	if primary == old {
		t.Logf("unit %d is primary again: its pod returned before the leader key expired", old)
	} else if s, _ := workloadStatus(ns, pg, old); s == nil || s.Message == "Primary" {
		t.Errorf("old primary %d did not rejoin as a replica: %+v", old, s)
	}
	// The data volume survives a pod restart (same PVC), so the unit is not reinstalled.
	if got := podPVs(t, ns, unitOf(pg, old)); strings.Join(got, ",") != strings.Join(pvs[old], ",") {
		t.Errorf("unit %d PVs changed across the pod kill: %v -> %v", old, pvs[old], got)
	}
	if c := count(hookNames(agentLogsOf(t, ns, pg, old)), "install"); c != 0 {
		t.Errorf("install reran on the replacement pod of unit %d", old)
	}

	// 4. Scale 3→1: the removed units' volumes are retained (Released).
	setScaleOf(t, ns, pg, 1)
	eventually(t, pgRecover, "units 1 and 2 gone", func() (bool, error) {
		for _, n := range []int{1, 2} {
			if ok, err := gone(&corev1.Pod{}, client.ObjectKey{Namespace: ns, Name: unitOf(pg, n)})(); !ok {
				return false, err
			}
			if ok, err := gone(&v1alpha1.UnitData{}, client.ObjectKey{Namespace: ns, Name: unitOf(pg, n)})(); !ok {
				return false, err
			}
		}
		return true, nil
	})
	waitPVsReleased(t, ns, flatten(pvs, 1, 2))
	lap("units 1 and 2 removed")
	if p := waitPGHealthy(t, pgRecover, ns, 1); p != 0 {
		t.Fatalf("remaining unit %d", p)
	}

	// 5. Scale 1→3: three healthy units on fresh volumes; the old volumes are untouched.
	setScaleOf(t, ns, pg, 3)
	waitPGHealthy(t, pgFirstReady, ns, 3)
	waitPatroniSynced(t, pgRecover, ns, 3)
	lap("three units healthy again after scaling back up")
	now := pgPVs(t, ns, 3)
	for _, n := range []int{1, 2} {
		for _, name := range now[n] {
			for _, o := range pvs[n] {
				if name == o {
					t.Errorf("unit %d reattached old PV %s", n, name)
				}
			}
		}
	}
	waitPVsRetained(t, ns, flatten(now, 0, 1, 2))
	waitPVsReleased(t, ns, flatten(pvs, 1, 2))
	if strings.Join(now[0], ",") != strings.Join(pvs[0], ",") {
		t.Errorf("unit 0's PVs changed: %v -> %v", pvs[0], now[0])
	}
}

// patroniMembers returns role/state per member as Patroni reports it (queried from the first reachable unit's
// workload container), keyed by ordinal.
func patroniMembers(ns string, scale int) (map[int][2]string, error) {
	const script = `import urllib.request, json
d = json.load(urllib.request.urlopen("http://localhost:8008/cluster", timeout=3))
print(json.dumps([[m["name"], m["role"], m.get("state", "")] for m in d["members"]]))`
	var lastErr error
	for n := 0; n < scale; n++ {
		out, err := exec.Command("kubectl", "--context", kubeContext, "-n", ns, "exec", unitOf(pg, n), "-c", "postgresql", "--", "python3", "-c", script).Output()
		if err != nil {
			lastErr = err
			continue
		}
		var members [][3]string
		if err := json.Unmarshal(out, &members); err != nil {
			lastErr = err
			continue
		}
		res := map[int][2]string{}
		for _, m := range members {
			i, err := strconv.Atoi(m[0][strings.LastIndex(m[0], "-")+1:])
			if err != nil {
				return nil, fmt.Errorf("member %q: %w", m[0], err)
			}
			res[i] = [2]string{m[1], m[2]}
		}
		return res, nil
	}
	return nil, fmt.Errorf("no unit answered Patroni's API: %v", lastErr)
}

// waitPatroniSynced waits until Patroni sees `scale` members: one leader and the rest streaming synchronous standbys.
// The charm's status can say "Primary" while a restarted replica is still catching up; in synchronous mode only a
// synchronous standby can take over, so scaling down before this settles can leave nobody to promote.
func waitPatroniSynced(t *testing.T, timeout time.Duration, ns string, scale int) {
	t.Helper()
	eventually(t, timeout, fmt.Sprintf("Patroni: %d members, one leader, the rest streaming sync standbys", scale), func() (bool, error) {
		ms, err := patroniMembers(ns, scale)
		if err != nil {
			return false, nil
		}
		leaders, standbys := 0, 0
		for _, m := range ms {
			switch {
			case m[0] == "leader" && m[1] == "running":
				leaders++
			case m[0] == "sync_standby" && m[1] == "streaming":
				standbys++
			}
		}
		return len(ms) == scale && leaders == 1 && standbys == scale-1, nil
	})
}
