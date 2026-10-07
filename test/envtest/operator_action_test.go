package envtest

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
)

const digestActions = "sha256:bb01"

var actionCharmOnce sync.Once

func addActionCharm() {
	actionCharmOnce.Do(func() {
		opCharms.add(digestActions, &registry.Charm{
			Metadata: map[string]any{
				"name":       "acts",
				"containers": map[string]any{"workload": map[string]any{"resource": "img"}},
				"resources":  map[string]any{"img": map[string]any{"type": "oci-image", "upstream-source": "busybox:1"}},
			},
			Actions: map[string]any{
				"echo": map[string]any{
					"description": "echo",
					"params": map[string]any{
						"msg": map[string]any{"type": "string", "default": "hi"},
						"n":   map[string]any{"type": "integer", "minimum": 1},
					},
					"required": []any{"n"},
				},
			},
			Base: registry.Base{Name: "ubuntu", Channel: "22.04"},
		})
	})
}

func newAction(t *testing.T, c client.Client, ns, name, unit, action, params string, labels map[string]string) {
	t.Helper()
	a := &v1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels},
		Spec:       v1alpha1.ActionSpec{Unit: unit, Name: action},
	}
	if params != "" {
		a.Spec.Parameters = &v1alpha1.JSON{Raw: []byte(params)}
	}
	if err := c.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
}

func admitted(t *testing.T, c client.Client, ns, name string) *v1alpha1.Action {
	t.Helper()
	var a v1alpha1.Action
	eventually(t, "action "+name+" admitted", func() bool { return get(c, ns, name, &a) == nil && a.Status.ID != 0 })
	return &a
}

func TestActionAdmission(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addActionCharm()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "acts", digestActions, 1)
	eventually(t, "charm", func() bool {
		a := ready(c, ns, "acts")
		return a != nil && a.Status.Charm != nil && len(a.Finalizers) == 1
	})
	app := ready(c, ns, "acts")

	newAction(t, c, ns, "ok1", "acts/0", "echo", `{"n":2}`, nil)
	a1 := admitted(t, c, ns, "ok1")
	if a1.Status.State != "pending" || a1.Status.Completed != nil {
		t.Errorf("status %+v", a1.Status)
	}
	var params map[string]any
	if err := json.Unmarshal(a1.Spec.Parameters.Raw, &params); err != nil || params["msg"] != "hi" || params["n"] != float64(2) {
		t.Errorf("parameters %s: defaults not filled", a1.Spec.Parameters.Raw)
	}
	if len(a1.OwnerReferences) != 1 || a1.OwnerReferences[0].UID != app.UID || a1.OwnerReferences[0].Kind != "Application" {
		t.Errorf("owners %+v", a1.OwnerReferences)
	}
	op1, err := strconv.Atoi(a1.Labels[v1alpha1.OperationLabel])
	if err != nil || op1 < 1 || int64(op1) >= a1.Status.ID {
		t.Errorf("operation label %q, task id %d", a1.Labels[v1alpha1.OperationLabel], a1.Status.ID)
	}

	// A label set by the creator (one run on several units) is kept, and ids stay unique.
	newAction(t, c, ns, "ok2", "acts/0", "echo", `{"n":1,"msg":"x"}`, map[string]string{v1alpha1.OperationLabel: "99"})
	a2 := admitted(t, c, ns, "ok2")
	if a2.Labels[v1alpha1.OperationLabel] != "99" || a2.Status.ID == a1.Status.ID {
		t.Errorf("op %q id %d (first %d)", a2.Labels[v1alpha1.OperationLabel], a2.Status.ID, a1.Status.ID)
	}

	for _, tc := range []struct{ name, unit, action, params, msg string }{
		{"unknown", "acts/0", "nope", "", `"nope" is not defined`},
		{"required", "acts/0", "echo", "", "n is required"},
		{"type", "acts/0", "echo", `{"n":"two"}`, "expected integer"},
		{"range", "acts/0", "echo", `{"n":0}`, "greater than or equal to 1"},
		{"extra", "acts/0", "echo", `{"n":1,"zz":1}`, "zz is not allowed"},
		{"not-object", "acts/0", "echo", `[1]`, "must be an object"},
		{"no-unit", "acts/5", "echo", `{"n":1}`, "not found"},
		{"no-app", "ghost/0", "echo", `{"n":1}`, `application "ghost" not found`},
		{"bad-unit", "acts", "echo", `{"n":1}`, "invalid unit"},
		{"exec-empty", "acts/0", "juju-exec", `{}`, "command is required"},
	} {
		newAction(t, c, ns, tc.name, tc.unit, tc.action, tc.params, nil)
		a := admitted(t, c, ns, tc.name)
		if a.Status.State != "failed" || !strings.Contains(a.Status.Message, tc.msg) || a.Status.Completed == nil || a.Labels[v1alpha1.OperationLabel] == "" {
			t.Errorf("%s: %+v", tc.name, a.Status)
		}
	}
	newAction(t, c, ns, "exec", "acts/0", "juju-exec", `{"command":"ls"}`, nil)
	if a := admitted(t, c, ns, "exec"); a.Status.State != "pending" {
		t.Errorf("exec: %+v", a.Status)
	}

	// Ids are unique across all of them.
	var list v1alpha1.ActionList
	if err := c.List(context.Background(), &list, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	seen := map[int64]string{}
	for _, a := range list.Items {
		if prev, dup := seen[a.Status.ID]; dup {
			t.Errorf("task id %d used by %s and %s", a.Status.ID, prev, a.Name)
		}
		seen[a.Status.ID] = a.Name
	}
	// A finished action is left alone.
	var done v1alpha1.Action
	_ = get(c, ns, "no-unit", &done)
	consistently(t, "finished action changed", func() bool {
		var again v1alpha1.Action
		return get(c, ns, "no-unit", &again) == nil && again.Status.ID == done.Status.ID && again.Status.Message == done.Status.Message
	})
}

func TestActionsOfUnitsThatGoAway(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addActionCharm()
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "acts", digestActions, 3)
	eventually(t, "charm", func() bool { a := ready(c, ns, "acts"); return a != nil && a.Status.Charm != nil })
	createPod(t, c, ns, "acts", 1, true)
	createPod(t, c, ns, "acts", 2, true)

	// Pending on units 1 and 2, and a running one on unit 2 (as the agent would have left it).
	newAction(t, c, ns, "p1", "acts/1", "echo", `{"n":1}`, nil)
	newAction(t, c, ns, "p2", "acts/2", "echo", `{"n":1}`, nil)
	newAction(t, c, ns, "r2", "acts/2", "echo", `{"n":1}`, nil)
	for _, n := range []string{"p1", "p2", "r2"} {
		admitted(t, c, ns, n)
	}
	r2 := admitted(t, c, ns, "r2")
	r2.Status.State = "running"
	if err := c.Status().Update(ctx, r2); err != nil {
		t.Fatal(err)
	}

	// Scale to 2: unit 2 is dying (its pod is still there), so its pending action fails and its running one is left alone.
	two := int32(2)
	updateApp(t, c, ns, "acts", func(a *v1alpha1.Application) { a.Spec.Scale = &two })
	eventually(t, "pending action of the dying unit failed", func() bool {
		var a v1alpha1.Action
		return get(c, ns, "p2", &a) == nil && a.Status.State == "failed" && strings.Contains(a.Status.Message, "dying")
	})
	newAction(t, c, ns, "new2", "acts/2", "echo", `{"n":1}`, nil)
	if a := admitted(t, c, ns, "new2"); a.Status.State != "failed" || !strings.Contains(a.Status.Message, "dying") {
		t.Errorf("new action on a dying unit: %+v", a.Status)
	}
	consistently(t, "other actions touched", func() bool {
		var p1, r2 v1alpha1.Action
		return get(c, ns, "p1", &p1) == nil && p1.Status.State == "pending" && get(c, ns, "r2", &r2) == nil && r2.Status.State == "running"
	})

	// The pod goes: unit 2 is gone, so the running action is aborted.
	if err := c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "acts-2"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "running action of the removed unit aborted", func() bool {
		var a v1alpha1.Action
		return get(c, ns, "r2", &a) == nil && a.Status.State == "aborted" && a.Status.Completed != nil
	})
	// Unit 1 within the scale but without a pod yet takes a pending action; once it is scaled away it is gone, so the
	// action is cancelled. Unit 0 stays.
	if err := c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "acts-1"}}); err != nil {
		t.Fatal(err)
	}
	updateApp(t, c, ns, "acts", func(a *v1alpha1.Application) { a.Spec.Scale = &two })
	newAction(t, c, ns, "c1", "acts/1", "echo", `{"n":1}`, nil)
	newAction(t, c, ns, "p0", "acts/0", "echo", `{"n":1}`, nil)
	for _, n := range []string{"c1", "p0"} {
		if a := admitted(t, c, ns, n); a.Status.State != "pending" {
			t.Fatalf("%s: %+v", n, a.Status)
		}
	}
	one := int32(1)
	updateApp(t, c, ns, "acts", func(a *v1alpha1.Application) { a.Spec.Scale = &one })
	eventually(t, "pending action of the removed unit cancelled", func() bool {
		var a v1alpha1.Action
		return get(c, ns, "c1", &a) == nil && a.Status.State == "cancelled"
	})
	if a := admitted(t, c, ns, "p0"); a.Status.State != "pending" {
		t.Errorf("action of a live unit: %+v", a.Status)
	}
}

func TestFinishedActionsAreRemovedAfterTheirAge(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	eventually(t, "model config", func() bool { return exists(c, ns, v1alpha1.ModelConfigMap, &corev1.ConfigMap{}) })
	var cm corev1.ConfigMap
	if err := get(c, ns, v1alpha1.ModelConfigMap, &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data[operator.MaxActionResultsAgeKey] = "2s"
	if err := c.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	newAction(t, c, ns, "old", "ghost/0", "echo", "", nil) // fails at once: no such application
	if a := admitted(t, c, ns, "old"); a.Status.State != "failed" {
		t.Fatalf("%+v", a.Status)
	}
	eventually(t, "finished action removed", func() bool { return gone(c, ns, "old", &v1alpha1.Action{}) })
	if time.Since(start) < 2*time.Second {
		t.Error("removed before its age")
	}
}
