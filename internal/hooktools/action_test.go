// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import (
	"encoding/json"
	"strings"
	"testing"
)

// fakeActions implements ActionBackend; outside an action (running false) every method fails as the agent's does.
type fakeActions struct {
	running    bool
	params     map[string]any
	results    map[string]any
	actionLogs []string
	message    string
	failed     bool
}

func (a *fakeActions) ActionParams() (map[string]any, error) {
	if !a.running {
		return nil, ErrNotAction
	}
	return a.params, nil
}

func (a *fakeActions) LogActionMessage(m string) error {
	if !a.running {
		return ErrNotAction
	}
	a.actionLogs = append(a.actionLogs, m)
	return nil
}

func (a *fakeActions) SetActionMessage(m string) error {
	if !a.running {
		return ErrNotAction
	}
	a.message = m
	return nil
}

func (a *fakeActions) SetActionFailed() error {
	if !a.running {
		return ErrNotAction
	}
	a.failed = true
	return nil
}

func (a *fakeActions) UpdateActionResults(keys []string, value string) error {
	if !a.running {
		return ErrNotAction
	}
	if a.results == nil {
		a.results = map[string]any{}
	}
	next := a.results
	for i, k := range keys {
		if i == len(keys)-1 {
			next[k] = value
			return nil
		}
		m, ok := next[k].(map[string]any)
		if !ok {
			m = map[string]any{}
			next[k] = m
		}
		next = m
	}
	return nil
}

func TestActionGet(t *testing.T) {
	r, f := newReal(t)
	f.running = true
	f.params = map[string]any{"name": "x", "count": float64(3), "on": true,
		"nested": map[string]any{"deep": map[string]any{"v": "yes"}, "n": float64(1)}}
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"name"}, "x\n"},
		{[]string{"count"}, "3\n"},
		{[]string{"on"}, "True\n"},
		{[]string{"on", "--format=json"}, "true\n"},
		{[]string{"nested.deep.v"}, "yes\n"},
		{[]string{"nested.deep"}, "v: \"yes\"\n"},
		{[]string{"nested.deep", "--format=json"}, "{\"v\":\"yes\"}\n"},
		// Missing keys and paths through non-maps print nothing (null in json), and are not errors.
		{[]string{"missing"}, ""},
		{[]string{"missing.deeper"}, ""},
		{[]string{"name.deeper"}, ""},
		{[]string{"missing", "--format=json"}, "null\n"},
		{[]string{"--format", "yaml", "count"}, "3\n"},
	}
	for _, tt := range tests {
		if got := rcall(r, "", "action-get", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("action-get %v = %+v, want %q", tt.args, got, tt.want)
		}
	}
	// All parameters.
	if got := rcall(r, "", "action-get", "--format=json"); got.Code != 0 || !strings.HasPrefix(got.Stdout, `{"count":3,"`) {
		t.Errorf("%+v", got)
	}
	// No parameters print an empty object.
	f.params = nil
	if got := rcall(r, "", "action-get", "--format=json"); got.Stdout != "{}\n" {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "action-get"); got.Code != 0 || got.Stdout != "{}\n" {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "action-get", "a", "b"); got.Code != 1 || !strings.Contains(got.Stderr, `unrecognized args: ["b"]`) {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "action-get", "--bogus"); got.Code != 1 {
		t.Errorf("%+v", got)
	}
}

func TestActionSet(t *testing.T) {
	r, f := newReal(t)
	f.running = true
	// juju's documentation example, including overwriting a value with a map and a map with a value.
	for _, kv := range []string{"outfile.size=10G", "foo.bar=2", "foo.baz.val=3", "foo.bar.zab=4", "foo.baz=1"} {
		if got := rcall(r, "", "action-set", kv); got.Code != 0 || got.Stdout != "" || got.Stderr != "" {
			t.Fatalf("action-set %s: %+v", kv, got)
		}
	}
	want := map[string]any{"outfile": map[string]any{"size": "10G"}, "foo": map[string]any{"bar": map[string]any{"zab": "4"}, "baz": "1"}}
	if !jsonEqual(f.results, want) {
		t.Fatalf("results %v, want %v", f.results, want)
	}
	// Several pairs in one call; values keep their text, even with equals signs and spaces; empty values are allowed.
	if got := rcall(r, "", "action-set", "a=b=c", "x-y.z0=hello world", "e="); got.Code != 0 {
		t.Fatal(got)
	}
	if f.results["a"] != "b=c" || f.results["x-y"].(map[string]any)["z0"] != "hello world" || f.results["e"] != "" {
		t.Fatalf("%v", f.results)
	}
	// Nothing is set when any argument is invalid.
	f.results = nil
	invalid := []struct{ arg, msg string }{
		{"noequals", `argument "noequals" must be of the form key...=value`},
		{"-foo=1", `flag provided but not defined: -foo`},
		{"foo-=1", `key "foo-" must start`},
		{"Foo=1", `key "Foo" must start`},
		{"foo!bar=1", `key "foo!bar"`},
		{"foo..bar=1", `key ""`},
		{".foo=1", `key ""`},
		{"foo.=1", `key ""`},
		{"=1", `key ""`},
		{"stdout=1", `cannot set reserved action key "stdout"`},
		{"a.stderr=1", `cannot set reserved action key "stderr"`},
		{"stdout-encoding=1", `reserved`},
		{"stderr-encoding.x=1", `reserved`},
	}
	for _, tt := range invalid {
		got := rcall(r, "", "action-set", "ok=1", tt.arg)
		if got.Code != 1 || !strings.Contains(got.Stderr, tt.msg) {
			t.Errorf("action-set %q = %+v, want error with %q", tt.arg, got, tt.msg)
		}
		if f.results != nil {
			t.Errorf("action-set %q set %v before failing", tt.arg, f.results)
		}
	}
	for _, ok := range []string{"foo", "500", "5-o-0", "foo.bar.baz", "foo-bar.baz", "a1"} {
		if got := rcall(r, "", "action-set", ok+"=1"); got.Code != 0 {
			t.Errorf("valid key %q rejected: %+v", ok, got)
		}
	}
	// With no arguments nothing happens.
	if got := rcall(r, "", "action-set"); got.Code != 0 {
		t.Errorf("%+v", got)
	}
	f.running = false
	if got := rcall(r, "", "action-set", "a=1"); got.Code != 1 || !strings.Contains(got.Stderr, "not running an action") {
		t.Errorf("%+v", got)
	}
}

func TestActionFailAndLog(t *testing.T) {
	r, f := newReal(t)
	f.running = true
	if got := rcall(r, "", "action-fail"); got.Code != 0 || f.message != "action failed without reason given, check action for errors" || !f.failed {
		t.Fatalf("%+v %q %v", got, f.message, f.failed)
	}
	f.failed = false
	if got := rcall(r, "", "action-fail", "unable to contact remote service"); got.Code != 0 || f.message != "unable to contact remote service" || !f.failed {
		t.Fatalf("%+v %q", got, f.message)
	}
	if got := rcall(r, "", "action-fail", "a", "b"); got.Code != 1 || !strings.Contains(got.Stderr, `unrecognized args: ["b"]`) {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "action-log"); got.Code != 1 || !strings.Contains(got.Stderr, "no message specified") {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "action-log", "step", "one"); got.Code != 0 || got.Stdout != "" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "action-log", "step two"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if len(f.actionLogs) != 2 || f.actionLogs[0] != "step one" || f.actionLogs[1] != "step two" {
		t.Fatalf("logs %q", f.actionLogs)
	}
	f.running = false
	for _, args := range [][]string{{"action-fail", "x"}, {"action-log", "x"}} {
		if got := rcall(r, "", args[0], args[1:]...); got.Code != 1 || !strings.Contains(got.Stderr, "not running an action") {
			t.Errorf("%v: %+v", args, got)
		}
	}
}

func jsonEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}
