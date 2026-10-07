//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
)

// The relation scenarios relate jk-test (the provider, app "jk-test", endpoint "data") to jk-test-client (the requirer, app "client", endpoint "source").
const (
	clientApp     = "client"
	providerEP    = "data"
	requirerEP    = "source"
	providerHooks = "data-relation-"
	requirerHooks = "source-relation-"
)

// deployClient creates the jk-test-client Application.
func deployClient(t *testing.T, ns string, scale int32) *v1alpha1.Application {
	t.Helper()
	a := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: clientApp, Namespace: ns},
		Spec: v1alpha1.ApplicationSpec{
			Charm: v1alpha1.CharmSpec{Name: "jk-test-client", Source: "local", Sha256: strings.TrimPrefix(clientDigest, "sha256:")},
			Scale: &scale,
		},
	}
	if err := kube.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return a
}

// updateAppNamed changes the named Application, retrying on conflicts with the operator's status writes.
func updateAppNamed(t *testing.T, ns, name string, mutate func(*v1alpha1.Application)) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		a, err := applicationOf(ns, name)
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

// setConfigOf sets one config option of the named Application, keeping the others.
func setConfigOf(t *testing.T, ns, name, key, value string) {
	t.Helper()
	updateAppNamed(t, ns, name, func(a *v1alpha1.Application) {
		if a.Spec.Config == nil {
			a.Spec.Config = map[string]string{}
		}
		a.Spec.Config[key] = value
	})
}

// relate creates the Relation between provider:providerEndpoint and requirer:requirerEndpoint, as a user would with YAML.
func relate(t *testing.T, ns, provider, providerEndpoint, requirer, requirerEndpoint string) *v1alpha1.Relation {
	t.Helper()
	name := strings.ReplaceAll(fmt.Sprintf("%s.%s-%s.%s", provider, providerEndpoint, requirer, requirerEndpoint), "_", "-")
	return relateNamed(t, ns, name, provider, providerEndpoint, requirer, requirerEndpoint)
}

// relateNamed is relate with the Relation's name chosen by the caller.
func relateNamed(t *testing.T, ns, name, provider, providerEndpoint, requirer, requirerEndpoint string) *v1alpha1.Relation {
	t.Helper()
	manifest := fmt.Sprintf(`apiVersion: jk.luci1900.github.io/v1alpha1
kind: Relation
metadata:
  name: %s
  namespace: %s
spec:
  endpoints:
    - {namespace: %s, application: %s, endpoint: %s}
    - {namespace: %s, application: %s, endpoint: %s}
`, name, ns, ns, provider, providerEndpoint, ns, requirer, requirerEndpoint)
	var rel v1alpha1.Relation
	if err := yaml.UnmarshalStrict([]byte(manifest), &rel); err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(context.Background(), &rel); err != nil {
		t.Fatal(err)
	}
	return &rel
}

// relationID waits for the operator to assign the Relation its id.
func relationID(t *testing.T, ns, name string) int64 {
	t.Helper()
	var id int64
	eventually(t, time.Minute, "id of relation "+name, func() (bool, error) {
		var r v1alpha1.Relation
		if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &r); err != nil {
			return false, err
		}
		id = r.Status.ID
		if id == 0 {
			return false, fmt.Errorf("status %+v", r.Status)
		}
		return true, nil
	})
	return id
}

// relationUnit returns what unit n of the application wrote and saw in a relation.
func relationUnit(ns, appName string, n int, relID int64) (v1alpha1.UnitRelationState, error) {
	u, err := unitDataOf(ns, appName, n)
	if err != nil {
		return v1alpha1.UnitRelationState{}, err
	}
	rs, ok := u.Spec.Relations[relationKey(relID)]
	if !ok {
		return rs, fmt.Errorf("%s has no state for relation %d (has %v)", u.Name, relID, keys(u.Spec.Relations))
	}
	return rs, nil
}

// waitData waits until every key=value pair is in the data returned by get.
func waitData(t *testing.T, what string, get func() (map[string]string, error), want map[string]string) {
	t.Helper()
	eventually(t, 2*time.Minute, what, func() (bool, error) {
		d, err := get()
		if err != nil {
			return false, err
		}
		for k, v := range want {
			if d[k] != v {
				return false, fmt.Errorf("%s = %q, want %q (data %v)", k, d[k], v, d)
			}
		}
		return true, nil
	})
}

// waitNonEmpty waits until every key is set to something in the data returned by get.
func waitNonEmpty(t *testing.T, what string, get func() (map[string]string, error), wantKeys ...string) {
	t.Helper()
	eventually(t, 2*time.Minute, what, func() (bool, error) {
		d, err := get()
		if err != nil {
			return false, err
		}
		for _, k := range wantKeys {
			if d[k] == "" {
				return false, fmt.Errorf("%s is empty (data %v)", k, d)
			}
		}
		return true, nil
	})
}

func unitRelData(ns, appName string, n int, relID int64) func() (map[string]string, error) {
	return func() (map[string]string, error) { return peerUnitData(ns, appName, n, relID) }
}

func appRelData(ns, appName string, relID int64) func() (map[string]string, error) {
	return func() (map[string]string, error) { return peerAppData(ns, appName, relID) }
}

// waitHooksOf is waitHooks for any application.
func waitHooksOf(t *testing.T, ns, appName string, n int, want ...string) []string {
	t.Helper()
	var names []string
	eventually(t, readyTimeout, fmt.Sprintf("hooks %v on %s", want, unitOf(appName, n)), func() (bool, error) {
		names = hookNames(agentLogsOf(t, ns, appName, n))
		for _, w := range want {
			if index(names, w) < 0 {
				return false, fmt.Errorf("have %v", names)
			}
		}
		return true, nil
	})
	return names
}

func lastIndex(names []string, name string) int {
	for i := len(names) - 1; i >= 0; i-- {
		if names[i] == name {
			return i
		}
	}
	return -1
}

// withPrefix keeps the hooks whose name starts with prefix.
func withPrefix(names []string, prefix string) []string {
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	return out
}

// checkHooksSucceeded fails the test for every hook of the unit that exited non-zero.
func checkHooksSucceeded(t *testing.T, ns, appName string, n int) {
	t.Helper()
	for _, h := range parseHooks(agentLogsOf(t, ns, appName, n)) {
		if h.Exit != 0 {
			t.Errorf("%s: hook %s exited %d", unitOf(appName, n), h.Name, h.Exit)
		}
	}
}

// ---- actions ----

// createAction creates an Action from YAML, the way a user would. extra is more spec fields (indented two spaces), e.g. "  parameters: {text: hi}".
func createAction(t *testing.T, ns, unit, name, extra string) *v1alpha1.Action {
	t.Helper()
	manifest := fmt.Sprintf(`apiVersion: jk.luci1900.github.io/v1alpha1
kind: Action
metadata:
  generateName: act-
  namespace: %s
spec:
  unit: %s
  name: %s
%s
`, ns, unit, name, extra)
	var a v1alpha1.Action
	if err := yaml.UnmarshalStrict([]byte(manifest), &a); err != nil {
		t.Fatalf("%v\n%s", err, manifest)
	}
	if err := kube.Create(context.Background(), &a); err != nil {
		t.Fatal(err)
	}
	return &a
}

func terminal(s v1alpha1.ActionState) bool {
	switch s {
	case "completed", "failed", "cancelled", "aborted":
		return true
	}
	return false
}

func getAction(ns, name string) (*v1alpha1.Action, error) {
	var a v1alpha1.Action
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &a)
	return &a, err
}

// waitAction waits for the Action to end and returns it.
func waitAction(t *testing.T, ns, name string) *v1alpha1.Action {
	t.Helper()
	var a *v1alpha1.Action
	eventually(t, 3*time.Minute, "action "+name+" to end", func() (bool, error) {
		var err error
		a, err = getAction(ns, name)
		if err != nil {
			return false, err
		}
		if !terminal(a.Status.State) {
			return false, fmt.Errorf("state %q", a.Status.State)
		}
		return true, nil
	})
	return a
}

// runAction creates an Action and waits for it to end.
func runAction(t *testing.T, ns, unit, name, extra string) *v1alpha1.Action {
	t.Helper()
	return waitAction(t, ns, createAction(t, ns, unit, name, extra).Name)
}

// results decodes status.results (nil when empty).
func results(t *testing.T, a *v1alpha1.Action) map[string]any {
	t.Helper()
	if a.Status.Results == nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(a.Status.Results.Raw, &m); err != nil {
		t.Fatalf("results of %s: %v", a.Name, err)
	}
	return m
}

// result looks up a nested key and renders the value as a string (action-set stores strings; numbers and booleans compare by their text).
func result(res map[string]any, path ...string) string {
	var cur any = res
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		if cur, ok = m[p]; !ok {
			return ""
		}
	}
	if cur == nil {
		return ""
	}
	if f, ok := cur.(float64); ok {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return fmt.Sprint(cur)
}

func logMessages(a *v1alpha1.Action) []string {
	var out []string
	for _, l := range a.Status.Log {
		out = append(out, l.Message)
	}
	return out
}
