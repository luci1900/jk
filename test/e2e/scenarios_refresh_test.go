//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
)

// TestRefresh deploys revision 1 of jk-test as a local charm (digest from the registry), pins revision 2's digest and checks the rollout:
// status.charm follows the pin, every unit gets new pods, runs upgrade-charm once before config-changed, and reports the new revision.
func TestRefresh(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 2, nil)
	waitUnitsActive(t, 6*time.Minute, ns, app, 2, "rev: 1")
	// The status shows revision 1 before the first hook sequence (install ... config-changed, start) is over, and a
	// queued sequence is never pre-empted by upgrade-charm (as in juju), so let both units finish it first.
	for n := 0; n < 2; n++ {
		waitHooks(t, ns, n, "start")
	}
	var relID int64
	eventually(t, time.Minute, "peer relation id", func() (bool, error) {
		var err error
		relID, err = peerRelationID(ns, app, "cluster")
		return err == nil, err
	})
	before, err := application(ns)
	if err != nil {
		t.Fatal(err)
	}
	if before.Status.Charm == nil || !sameDigest(before.Status.Charm.Sha256, digest) {
		t.Fatalf("status.charm before: %+v, want digest %s", before.Status.Charm, digest)
	}
	uids := map[int]string{}
	charmURLs := map[int]string{}
	for n := 0; n < 2; n++ {
		uids[n] = podUID(t, ns, unitName(n))
		u, err := unitData(ns, n)
		if err != nil {
			t.Fatal(err)
		}
		charmURLs[n] = u.Spec.CharmURL
		waitData(t, fmt.Sprintf("unit %d recorded revision 1", n), unitRelData(ns, app, n, relID), map[string]string{"revision": "1"})
		if d, _ := peerUnitData(ns, app, n, relID); d["upgrades"] != "" {
			t.Errorf("unit %d upgraded before the refresh: %v", n, d)
		}
	}

	updateApp(t, ns, func(a *v1alpha1.Application) { a.Spec.Charm.Sha256 = strings.TrimPrefix(rev2Digest, "sha256:") })

	// status.charm follows the pin (re-resolved from the registry, so the metadata is there too)
	eventually(t, 2*time.Minute, "status.charm at revision 2", func() (bool, error) {
		a, err := application(ns)
		if err != nil {
			return false, err
		}
		c := a.Status.Charm
		if c == nil || !sameDigest(c.Sha256, rev2Digest) || !strings.Contains(c.Image, strings.TrimPrefix(rev2Digest, "sha256:")) || c.Metadata == nil {
			return false, fmt.Errorf("status.charm %+v", c)
		}
		return true, nil
	})

	// the StatefulSet rolls: new pods with the new charm, every unit reports revision 2
	waitUnitsActive(t, 6*time.Minute, ns, app, 2, "rev: 2")
	for n := 0; n < 2; n++ {
		if podUID(t, ns, unitName(n)) == uids[n] {
			t.Errorf("unit %d still runs the old pod", n)
		}
		// The new pod's log covers its whole life: upgrade-charm ran once, before config-changed, and not install. start does
		// not run again: the unit's state (it started long ago) survives the new pod, as in juju.
		names := waitHooks(t, ns, n, "upgrade-charm", "config-changed")
		if c := count(names, "upgrade-charm"); c != 1 {
			t.Errorf("unit %d ran upgrade-charm %d times: %v", n, c, names)
		}
		if c := count(names, "install"); c != 0 {
			t.Errorf("unit %d ran install on the refreshed pod: %v", n, names)
		}
		if !(index(names, "upgrade-charm") < index(names, "config-changed")) {
			t.Errorf("unit %d: want upgrade-charm before config-changed: %v", n, names)
		}
		for _, h := range parseHooks(agentLogs(t, ns, n)) {
			if h.Exit != 0 {
				t.Errorf("unit %d: hook %s exited %d", n, h.Name, h.Exit)
			}
		}
		// the charm saw the change: it recorded old and new revision
		waitData(t, fmt.Sprintf("unit %d upgrade record", n), unitRelData(ns, app, n, relID), map[string]string{
			"upgraded-from": "1",
			"upgraded-to":   "2",
			"upgrades":      "1",
			"revision":      "2",
		})
		eventually(t, time.Minute, fmt.Sprintf("unit %d charm URL", n), func() (bool, error) {
			u, err := unitData(ns, n)
			if err != nil {
				return false, err
			}
			if u.Spec.CharmURL == "" || u.Spec.CharmURL == charmURLs[n] {
				return false, fmt.Errorf("charmURL %q (was %q)", u.Spec.CharmURL, charmURLs[n])
			}
			return true, nil
		})
	}

	// status tells which charm each unit runs
	eventually(t, time.Minute, "status.units up to date", func() (bool, error) {
		a, err := application(ns)
		if err != nil {
			return false, err
		}
		if len(a.Status.Units) != 2 {
			return false, fmt.Errorf("status.units %+v", a.Status.Units)
		}
		for _, u := range a.Status.Units {
			if !u.UpToDate || u.Image != a.Status.Charm.Image {
				return false, fmt.Errorf("status.units %+v, charm image %s", a.Status.Units, a.Status.Charm.Image)
			}
		}
		return true, nil
	})

	// Nothing re-resolves on its own: a config change does not upgrade again.
	setConfig(t, ns, map[string]string{"greeting": "after"})
	waitUnitsActive(t, 2*time.Minute, ns, app, 2, "greeting: after")
	for n := 0; n < 2; n++ {
		if c := count(hookNames(agentLogs(t, ns, n)), "upgrade-charm"); c != 1 {
			t.Errorf("unit %d ran upgrade-charm %d times after a config change", n, c)
		}
	}
}

// sameDigest compares digests with or without the "sha256:" prefix.
func sameDigest(a, b string) bool {
	return strings.TrimPrefix(a, "sha256:") == strings.TrimPrefix(b, "sha256:")
}
