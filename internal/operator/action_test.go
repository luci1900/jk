package operator

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

func TestParseUnit(t *testing.T) {
	for in, want := range map[string]int{"a/0": 0, "pg-k8s/12": 12} {
		if app, n, ok := ParseUnit(in); !ok || n != want || strings.Contains(app, "/") {
			t.Errorf("%q: %q %d %v", in, app, n, ok)
		}
	}
	for _, in := range []string{"", "a", "/0", "a/", "a/01", "a/-1", "a/x", "a/1/2"} {
		if _, _, ok := ParseUnit(in); ok {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestClassifyUnit(t *testing.T) {
	tests := []struct {
		name              string
		n                 int
		scale             int32
		deleting, pod, ud bool
		want              UnitLife
	}{
		{"within scale, nothing yet", 0, 1, false, false, false, UnitLive},
		{"within scale, running", 1, 2, false, true, true, UnitLive},
		{"above scale, stopping", 2, 2, false, true, false, UnitDying},
		{"above scale, only data left", 2, 2, false, false, true, UnitDying},
		{"above scale, gone", 2, 2, false, false, false, UnitGone},
		{"app deleting, running", 0, 1, true, true, true, UnitDying},
		{"app deleting, gone", 0, 1, true, false, false, UnitGone},
	}
	for _, tt := range tests {
		if got := ClassifyUnit(tt.n, tt.scale, tt.deleting, tt.pod, tt.ud); got != tt.want {
			t.Errorf("%s: %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestAdmitAction(t *testing.T) {
	schemas, _ := ActionSchemas([]byte(`{"echo":{"params":{"msg":{"type":"string","default":"hi"},"n":{"type":"integer"}},"required":["n"]}}`))
	tests := []struct {
		name   string
		life   UnitLife
		action string
		params map[string]any
		state  string
		msg    string
	}{
		{"ok with defaults", UnitLive, "echo", map[string]any{"n": 1.0}, "pending", ""},
		{"unit gone", UnitGone, "echo", map[string]any{"n": 1.0}, "failed", "not found"},
		{"unit dying", UnitDying, "echo", map[string]any{"n": 1.0}, "failed", "dying"},
		{"unknown action", UnitLive, "nope", nil, "failed", "not defined by the charm (actions: echo, juju-exec)"},
		{"missing parameter", UnitLive, "echo", nil, "failed", "validation failed: (root): n is required"},
		{"exec", UnitLive, "juju-exec", map[string]any{"command": "ls"}, "pending", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := AdmitAction(tt.life, "app/0", tt.action, tt.params, schemas)
			if string(a.State) != tt.state || !strings.Contains(a.Message, tt.msg) {
				t.Errorf("%+v", a)
			}
			if tt.name == "ok with defaults" && a.Parameters["msg"] != "hi" {
				t.Errorf("defaults not filled: %v", a.Parameters)
			}
		})
	}
}

func TestSettleAction(t *testing.T) {
	tests := []struct {
		life  UnitLife
		state v1alpha1.ActionState
		want  v1alpha1.ActionState
	}{
		{UnitLive, StatePending, ""},
		{UnitDying, StatePending, StateFailed},
		{UnitDying, StateRunning, ""},
		{UnitGone, StatePending, StateCancelled},
		{UnitGone, StateRunning, StateAborted},
		{UnitGone, StateAborting, StateAborted},
	}
	for _, tt := range tests {
		got, msg, ok := SettleAction(tt.life, tt.state, "a/0")
		if got != tt.want || ok != (tt.want != "") || (ok && msg == "") {
			t.Errorf("%v %s: %q %q %v", tt.life, tt.state, got, msg, ok)
		}
	}
	for _, s := range []v1alpha1.ActionState{StateCompleted, StateFailed, StateCancelled, StateAborted} {
		if !ActionTerminal(s) {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []v1alpha1.ActionState{"", StatePending, StateRunning, StateAborting} {
		if ActionTerminal(s) {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestPinKey(t *testing.T) {
	rev := 5
	base := func() *v1alpha1.Application {
		return &v1alpha1.Application{Spec: v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: "x"}}}
	}
	key := func(mut func(*v1alpha1.Application)) string { a := base(); mut(a); return PinKey(a) }
	same := [][2]string{
		{key(func(*v1alpha1.Application) {}), key(func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "latest/stable" })},
		{key(func(*v1alpha1.Application) {}), key(func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "stable"; a.Spec.Charm.Source = "charmhub" })},
		{key(func(a *v1alpha1.Application) { a.Spec.Charm.Source, a.Spec.Charm.Sha256 = "local", "abc" }), key(func(a *v1alpha1.Application) { a.Spec.Charm.Source, a.Spec.Charm.Sha256 = "local", "sha256:abc" })},
		{key(func(a *v1alpha1.Application) { a.Spec.Config = map[string]string{"a": "b"} }), key(func(*v1alpha1.Application) {})},
	}
	for i, p := range same {
		if p[0] != p[1] {
			t.Errorf("case %d: %q != %q", i, p[0], p[1])
		}
	}
	differ := []func(*v1alpha1.Application){
		func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "14/stable" },
		func(a *v1alpha1.Application) { a.Spec.Charm.Revision = &rev },
		func(a *v1alpha1.Application) { a.Spec.Charm.Base = "ubuntu@24.04" },
		func(a *v1alpha1.Application) { a.Spec.Charm.URL, a.Spec.Charm.Sha256 = "https://x", "ab" },
		func(a *v1alpha1.Application) { a.Spec.Constraints = &v1alpha1.Constraints{Arch: "arm64"} },
		func(a *v1alpha1.Application) { a.Spec.Charm.Source, a.Spec.Charm.Sha256 = "local", "abc" },
	}
	seen := map[string]bool{key(func(*v1alpha1.Application) {}): true}
	for i, m := range differ {
		k := key(m)
		if seen[k] {
			t.Errorf("pin %d repeats %q", i, k)
		}
		seen[k] = true
	}
}

func TestUnitCharmStatuses(t *testing.T) {
	app := &v1alpha1.Application{ObjectMeta: metav1.ObjectMeta{Name: "pg"}, Status: v1alpha1.ApplicationStatus{Charm: &v1alpha1.ResolvedCharm{Image: "reg/charms@sha256:new"}}}
	pod := func(name, image, rev string) corev1.Pod {
		p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if image != "" {
			p.Annotations = map[string]string{v1alpha1.CharmImageAnnotation: image, v1alpha1.CharmRevisionAnnotation: rev}
		}
		return p
	}
	legacy := pod("pg-10", "", "")
	legacy.Spec.InitContainers = []corev1.Container{{Args: []string{"init", "--charm-image=reg/charms@sha256:old"}}}
	pods := []corev1.Pod{pod("pg-2", "reg/charms@sha256:new", "8"), legacy, pod("pg-0", "reg/charms@sha256:new", "8"), pod("other-0", "x", "1"), pod("pg-1", "reg/charms@sha256:old", "7")}
	data := map[string]*v1alpha1.UnitData{
		"pg-0": {Spec: v1alpha1.UnitDataSpec{CharmURL: "reg/charms@sha256:old"}}, // pod rolled, upgrade-charm not run yet
		"pg-2": {Spec: v1alpha1.UnitDataSpec{CharmURL: "reg/charms@sha256:new"}},
	}
	got := UnitCharmStatuses(app, pods, data)
	want := []v1alpha1.UnitCharmStatus{
		{Name: "pg/0", Revision: 8, Image: "reg/charms@sha256:new", UpToDate: false},
		{Name: "pg/1", Revision: 7, Image: "reg/charms@sha256:old"},
		{Name: "pg/2", Revision: 8, Image: "reg/charms@sha256:new", UpToDate: true},
		{Name: "pg/10", Image: "reg/charms@sha256:old"},
	}
	if len(got) != len(want) {
		t.Fatalf("%+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%d: %+v, want %+v", i, got[i], want[i])
		}
	}
}
