//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

func waitRelationGone(t *testing.T, ns, name string) {
	t.Helper()
	eventually(t, 3*time.Minute, "relation "+name+" gone", gone(&v1alpha1.Relation{}, client.ObjectKey{Namespace: ns, Name: name}))
}

// unitNames returns "<app>/0,<app>/1,..." for n units.
func unitNames(appName string, n int) string {
	var out []string
	for i := 0; i < n; i++ {
		out = append(out, jujuUnit(appName, i))
	}
	return strings.Join(out, ",")
}

// TestRelation relates jk-test (2 units, provider) to jk-test-client (2 units, requirer): hooks and their order, remote data both ways, JUJU_REMOTE_* in the hook environment, and scale changes.
func TestRelation(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 2, nil)
	deployClient(t, ns, 2)
	waitUnitsActive(t, 6*time.Minute, ns, app, 2, "clients: 0")
	waitUnitsActive(t, 6*time.Minute, ns, clientApp, 2, "providers: 0")

	rel := relate(t, ns, app, providerEP, clientApp, requirerEP)
	relID := relationID(t, ns, rel.Name)
	if c, _, err := relationValid(ns, rel.Name); err != nil || c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("Valid condition %+v err %v", c, err)
	}
	waitUnitsActive(t, 3*time.Minute, ns, app, 2, "clients: 2")
	waitUnitsActive(t, 3*time.Minute, ns, clientApp, 2, "providers: 2")

	t.Run("hooks", func(t *testing.T) {
		for _, side := range []struct {
			app, prefix string
			remotes     int
		}{{app, providerHooks, 2}, {clientApp, requirerHooks, 2}} {
			for n := 0; n < 2; n++ {
				names := waitHooksOf(t, ns, side.app, n, side.prefix+"created", side.prefix+"joined", side.prefix+"changed")
				// A hook is logged as finished after its effects show, so the last join may still be running.
				eventually(t, time.Minute, "one join per remote unit", func() (bool, error) {
					names = hookNames(agentLogsOf(t, ns, side.app, n))
					return count(withPrefix(names, side.prefix), side.prefix+"joined") >= side.remotes, nil
				})
				rh := withPrefix(names, side.prefix)
				if c := count(rh, side.prefix+"created"); c != 1 {
					t.Errorf("%s ran %screated %d times: %v", unitOf(side.app, n), side.prefix, c, rh)
				}
				if c := count(rh, side.prefix+"joined"); c != side.remotes {
					t.Errorf("%s ran %sjoined %d times, want one per remote unit (%d): %v", unitOf(side.app, n), side.prefix, c, side.remotes, rh)
				}
				if c := count(rh, side.prefix+"departed") + count(rh, side.prefix+"broken"); c != 0 {
					t.Errorf("%s ran departed or broken: %v", unitOf(side.app, n), rh)
				}
				// created first, the joins before the last changed (juju queues a changed right after each joined)
				if !(index(rh, side.prefix+"created") == 0 && index(rh, side.prefix+"created") < index(rh, side.prefix+"joined") && index(rh, side.prefix+"joined") < lastIndex(rh, side.prefix+"changed")) {
					t.Errorf("%s relation hook order: %v", unitOf(side.app, n), rh)
				}
				checkHooksSucceeded(t, ns, side.app, n)
			}
		}
	})

	t.Run("remote-env", func(t *testing.T) {
		// The charm logs JUJU_REMOTE_UNIT and JUJU_REMOTE_APP for every event; the agent forwards juju-log to its container log.
		for n := 0; n < 2; n++ {
			logs := agentLogsOf(t, ns, app, n)
			for c := 0; c < 2; c++ {
				want := fmt.Sprintf("remote=%s/%d env JUJU_REMOTE_UNIT=%s/%d JUJU_REMOTE_APP=%s JUJU_DEPARTING_UNIT= JUJU_RELATION=%s JUJU_RELATION_ID=%s:%d", clientApp, c, clientApp, c, clientApp, providerEP, providerEP, relID)
				if !strings.Contains(logs, "RelationJoinedEvent") || !strings.Contains(logs, want) {
					t.Errorf("%s: no log line with %q:\n%s", unitOf(app, n), want, logs)
				}
			}
			logs = agentLogsOf(t, ns, clientApp, n)
			for p := 0; p < 2; p++ {
				want := fmt.Sprintf("remote=%s/%d env JUJU_REMOTE_UNIT=%s/%d JUJU_REMOTE_APP=%s JUJU_DEPARTING_UNIT= JUJU_RELATION=%s JUJU_RELATION_ID=%s:%d", app, p, app, p, app, requirerEP, requirerEP, relID)
				if !strings.Contains(logs, want) {
					t.Errorf("%s: no log line with %q:\n%s", unitOf(clientApp, n), want, logs)
				}
			}
		}
	})

	t.Run("data", func(t *testing.T) {
		for n := 0; n < 2; n++ {
			waitData(t, fmt.Sprintf("provider unit %d data", n), unitRelData(ns, app, n, relID), map[string]string{
				"provider-unit": jujuUnit(app, n),
				"clients":       unitNames(clientApp, 2),
				"joins":         "2",
			})
			waitNonEmpty(t, fmt.Sprintf("provider unit %d address", n), unitRelData(ns, app, n, relID), "address")
			waitData(t, fmt.Sprintf("client unit %d data", n), unitRelData(ns, clientApp, n, relID), map[string]string{
				"client-unit":       jujuUnit(clientApp, n),
				"hello":             "hello from " + jujuUnit(clientApp, n),
				"seen-units":        unitNames(app, 2),
				"seen-provider-app": app,
			})
			waitNonEmpty(t, fmt.Sprintf("client unit %d reads the app generation", n), unitRelData(ns, clientApp, n, relID), "address", "seen-generation")
			// each side lists the other side's units as members
			rs, err := relationUnit(ns, app, n, relID)
			if err != nil {
				t.Fatal(err)
			}
			if len(rs.Members) != 2 {
				t.Errorf("provider unit %d members %v, want the 2 client units", n, rs.Members)
			}
			rs, err = relationUnit(ns, clientApp, n, relID)
			if err != nil {
				t.Fatal(err)
			}
			if len(rs.Members) != 2 {
				t.Errorf("client unit %d members %v, want the 2 provider units", n, rs.Members)
			}
		}
		// application data is written by each leader only
		waitData(t, "provider app data", appRelData(ns, app, relID), map[string]string{"provider-app": app, "client-app-seen": clientApp})
		waitNonEmpty(t, "provider app generation", appRelData(ns, app, relID), "generation")
		waitData(t, "client app data", appRelData(ns, clientApp, relID), map[string]string{"client-app": clientApp})
	})

	t.Run("scale", func(t *testing.T) {
		setScaleOf(t, ns, clientApp, 3)
		setScaleOf(t, ns, app, 3)
		waitUnitsActive(t, 6*time.Minute, ns, app, 3, "clients: 3")
		waitUnitsActive(t, 6*time.Minute, ns, clientApp, 3, "providers: 3")
		// the existing units ran joined for the new ones; the new ones joined everybody
		for n := 0; n < 3; n++ {
			waitData(t, fmt.Sprintf("provider unit %d sees 3 clients", n), unitRelData(ns, app, n, relID), map[string]string{"clients": unitNames(clientApp, 3)})
			waitData(t, fmt.Sprintf("client unit %d sees 3 providers", n), unitRelData(ns, clientApp, n, relID), map[string]string{"seen-units": unitNames(app, 3)})
		}
		if logs := agentLogsOf(t, ns, app, 0); !strings.Contains(logs, "remote=client/2 env JUJU_REMOTE_UNIT=client/2") {
			t.Errorf("jk-test/0 saw no join of client/2:\n%s", logs)
		}
		if logs := agentLogsOf(t, ns, clientApp, 0); !strings.Contains(logs, "remote=jk-test/2 env JUJU_REMOTE_UNIT=jk-test/2") {
			t.Errorf("client/0 saw no join of jk-test/2:\n%s", logs)
		}
		names := waitHooksOf(t, ns, app, 2, providerHooks+"created", providerHooks+"joined")
		if c := count(names, providerHooks+"joined"); c != 3 {
			t.Errorf("new provider unit ran joined %d times, want 3: %v", c, names)
		}

		// scale the clients down to 1: the providers run departed for each leaving unit, with JUJU_DEPARTING_UNIT set
		setScaleOf(t, ns, clientApp, 1)
		waitUnitsActive(t, 6*time.Minute, ns, app, 3, "clients: 1")
		for n := 0; n < 3; n++ {
			waitData(t, fmt.Sprintf("provider unit %d sees 1 client", n), unitRelData(ns, app, n, relID), map[string]string{"clients": unitNames(clientApp, 1)})
			names := hookNames(agentLogsOf(t, ns, app, n))
			if c := count(names, providerHooks+"departed"); c != 2 {
				t.Errorf("provider unit %d ran departed %d times, want 2: %v", n, c, names)
			}
			logs := agentLogsOf(t, ns, app, n)
			for _, left := range []string{"client/1", "client/2"} {
				if !strings.Contains(logs, "remote="+left+" departing="+left+" env JUJU_REMOTE_UNIT="+left+" JUJU_REMOTE_APP=client JUJU_DEPARTING_UNIT="+left) {
					t.Errorf("provider unit %d: no departed event for %s with JUJU_DEPARTING_UNIT:\n%s", n, left, logs)
				}
			}
		}
		waitUnitsActive(t, time.Minute, ns, clientApp, 1, "providers: 3")
	})
}

// jujuUnit is the unit name charms see (the pod is named <app>-<n>).
func jujuUnit(appName string, n int) string { return fmt.Sprintf("%s/%d", appName, n) }

// TestRelationRemoval deletes the Relation: every unit runs departed for each remote unit and then broken, then the object goes.
func TestRelationRemoval(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	deployClient(t, ns, 2)
	waitUnitsActive(t, 6*time.Minute, ns, app, 1, "clients: 0")
	waitUnitsActive(t, 6*time.Minute, ns, clientApp, 2, "providers: 0")
	rel := relate(t, ns, app, providerEP, clientApp, requirerEP)
	relationID(t, ns, rel.Name)
	waitUnitsActive(t, 3*time.Minute, ns, app, 1, "clients: 2")
	waitUnitsActive(t, 3*time.Minute, ns, clientApp, 2, "providers: 1")

	if err := kube.Delete(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	waitRelationGone(t, ns, rel.Name)

	// The Relation is gone only after the units acknowledged, so the logs are complete.
	for _, side := range []struct {
		app, prefix string
		units       int
		remotes     int
	}{{app, providerHooks, 1, 2}, {clientApp, requirerHooks, 2, 1}} {
		for n := 0; n < side.units; n++ {
			rh := withPrefix(hookNames(agentLogsOf(t, ns, side.app, n)), side.prefix)
			if c := count(rh, side.prefix+"departed"); c != side.remotes {
				t.Errorf("%s ran departed %d times, want one per remote unit (%d): %v", unitOf(side.app, n), c, side.remotes, rh)
			}
			if c := count(rh, side.prefix+"broken"); c != 1 {
				t.Errorf("%s ran broken %d times: %v", unitOf(side.app, n), c, rh)
			}
			if lastIndex(rh, side.prefix+"departed") > index(rh, side.prefix+"broken") {
				t.Errorf("%s: departed after broken: %v", unitOf(side.app, n), rh)
			}
			checkHooksSucceeded(t, ns, side.app, n)
		}
	}
	waitUnitsActive(t, time.Minute, ns, app, 1, "clients: 0")
	waitUnitsActive(t, time.Minute, ns, clientApp, 2, "providers: 0")
	// the applications go on running
	for _, a := range []string{app, clientApp} {
		if _, err := applicationOf(ns, a); err != nil {
			t.Errorf("application %s: %v", a, err)
		}
	}
}

// TestRelationRemovedWithApplication deletes the requirer Application: the Relation goes the same orderly way.
func TestRelationRemovedWithApplication(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 1, nil)
	deployClient(t, ns, 1)
	waitUnitsActive(t, 6*time.Minute, ns, app, 1, "clients: 0")
	waitUnitsActive(t, 6*time.Minute, ns, clientApp, 1, "providers: 0")
	rel := relate(t, ns, app, providerEP, clientApp, requirerEP)
	relationID(t, ns, rel.Name)
	waitUnitsActive(t, 3*time.Minute, ns, app, 1, "clients: 1")
	waitUnitsActive(t, 3*time.Minute, ns, clientApp, 1, "providers: 1")
	clientLogs := follow(ns, unitOf(clientApp, 0))

	a, err := applicationOf(ns, clientApp)
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	eventually(t, readyTimeout, "client application gone", gone(&v1alpha1.Application{}, client.ObjectKey{Namespace: ns, Name: clientApp}))
	waitRelationGone(t, ns, rel.Name)

	rh := withPrefix(hookNames(agentLogsOf(t, ns, app, 0)), providerHooks)
	if count(rh, providerHooks+"departed") != 1 || count(rh, providerHooks+"broken") != 1 || lastIndex(rh, providerHooks+"departed") > index(rh, providerHooks+"broken") {
		t.Errorf("provider relation hooks after the client was removed: %v", rh)
	}
	waitUnitsActive(t, time.Minute, ns, app, 1, "clients: 0")
	select {
	case <-clientLogs.done:
	case <-time.After(30 * time.Second):
	}
	names := hookNames(clientLogs.String())
	if count(names, requirerHooks+"departed") != 1 || count(names, requirerHooks+"broken") != 1 || lastIndex(names, requirerHooks+"departed") > index(names, requirerHooks+"broken") {
		t.Errorf("client relation hooks while it was removed: %v", names)
	}
	if index(names, "stop") < 0 {
		t.Errorf("client did not run stop: %v", names)
	}
}

// relationValid returns the Valid condition of the Relation (nil until the operator has judged it) and its id.
func relationValid(ns, name string) (*metav1.Condition, int64, error) {
	var r v1alpha1.Relation
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &r); err != nil {
		return nil, 0, err
	}
	return meta.FindStatusCondition(r.Status.Conditions, v1alpha1.RelationValid), r.Status.ID, nil
}

// TestRelationValidation: the operator reports why a Relation cannot be established in status, and gives it no id (so no hook runs). No pods are needed.
func TestRelationValidation(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	zero := int32(0)
	deployWith(t, ns, 0, nil)
	deployClient(t, ns, 0)
	other := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: ns},
		Spec: v1alpha1.ApplicationSpec{
			Charm: v1alpha1.CharmSpec{Name: charmName, Source: "local", Sha256: strings.TrimPrefix(digest, "sha256:")},
			Scale: &zero,
		},
	}
	if err := kube.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{app, clientApp, "other"} {
		eventually(t, time.Minute, "status.charm of "+name, func() (bool, error) {
			a, err := applicationOf(ns, name)
			if err != nil {
				return false, err
			}
			return a.Status.Charm != nil && a.Status.Charm.Metadata != nil, nil
		})
	}

	good := relate(t, ns, app, providerEP, clientApp, requirerEP)
	relationID(t, ns, good.Name)
	if c, _, err := relationValid(ns, good.Name); err != nil || c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("valid relation: condition %+v err %v", c, err)
	}

	for _, c := range []struct {
		what, reason string
		a, aep       string
		b, bep       string
	}{
		{"unknown endpoint", "EndpointNotFound", app, providerEP, clientApp, "nope"},
		{"unknown application", "ApplicationNotFound", app, providerEP, "ghost", requirerEP},
		{"both sides provide", "IncompatibleEndpoints", app, providerEP, "other", providerEP},
		{"relation to itself", "SameApplication", app, providerEP, app, providerEP},
		{"duplicate of an existing relation", "Duplicate", app, providerEP, clientApp, requirerEP},
		{"over the endpoint's limit", "LimitExceeded", "other", providerEP, clientApp, requirerEP},
	} {
		name := fmt.Sprintf("%s.%s-%s.%s", c.a, c.aep, c.b, c.bep)
		if c.reason == "Duplicate" {
			name = "second" // the same endpoints under another name
		}
		rel := relateNamed(t, ns, name, c.a, c.aep, c.b, c.bep)
		eventually(t, time.Minute, c.what+" reported as "+c.reason, func() (bool, error) {
			cond, id, err := relationValid(ns, rel.Name)
			if err != nil {
				return false, err
			}
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != c.reason || cond.Message == "" {
				return false, fmt.Errorf("condition %+v", cond)
			}
			if id != 0 {
				return false, fmt.Errorf("relation was given id %d", id)
			}
			return true, nil
		})
	}
}
