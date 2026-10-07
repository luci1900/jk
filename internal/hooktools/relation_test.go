// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func relReal(t *testing.T) (*Real, *fakeBackend) {
	r, f := newReal(t)
	rel := f.addRel(7, "database-peers", "app", "app/1", "app/2")
	rel.settings["app/0"] = map[string]string{"ingress-address": "10.0.0.1", "private-address": "10.0.0.1"}
	rel.settings["app/1"] = map[string]string{"key": "one", "other": "x"}
	rel.settings["app/2"] = map[string]string{}
	rel.app["app"] = map[string]string{"leader-key": "lv"}
	f.addRel(8, "database-peers", "app")
	f.addRel(9, "restart", "app", "app/1")
	f.leader = true
	return r, f
}

func TestRelationIDs(t *testing.T) {
	r, f := relReal(t)
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"database-peers"}, "database-peers:7\ndatabase-peers:8\n"},
		{[]string{"database-peers", "--format=json"}, `["database-peers:7","database-peers:8"]` + "\n"},
		{[]string{"restart", "--format", "json"}, `["restart:9"]` + "\n"},
		{[]string{"nope", "--format=json"}, "[]\n"},
		{[]string{"nope"}, ""},
	}
	for _, tt := range tests {
		if got := rcall(r, "", "relation-ids", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("relation-ids %v = %+v, want %q", tt.args, got, tt.want)
		}
	}
	if got := rcall(r, "", "relation-ids"); got.Code == 0 || !strings.Contains(got.Stderr, "no endpoint name specified") {
		t.Errorf("%+v", got)
	}
	// In a relation hook the endpoint defaults to the hook's.
	f.hookRel = &HookRelation{ID: 9, Endpoint: "restart", RemoteApp: "app"}
	if got := rcall(r, "", "relation-ids"); got.Stdout != "restart:9\n" {
		t.Errorf("%+v", got)
	}
	// Broken relations are not listed.
	f.broken = map[int]bool{9: true}
	if got := rcall(r, "", "relation-ids", "restart"); got.Stdout != "" {
		t.Errorf("%+v", got)
	}
	if rcall(r, "", "relation-ids", "a", "b").Code == 0 || rcall(r, "", "relation-ids", "--bogus", "a").Code == 0 {
		t.Error("bad args accepted")
	}
}

func TestRelationList(t *testing.T) {
	r, f := relReal(t)
	tests := []struct {
		args []string
		code int
		want string
	}{
		{[]string{"-r", "7"}, 0, "app/1\napp/2\n"},
		{[]string{"-r", "database-peers:7", "--format=json"}, 0, `["app/1","app/2"]` + "\n"},
		{[]string{"--relation=8", "--format=json"}, 0, "[]\n"},
		{[]string{"-r", "7", "--app"}, 0, "app\n"},
		{[]string{"-r", "99"}, 1, ""},
		{[]string{"-r", "x"}, 1, ""},
		{[]string{}, 1, ""},
	}
	for _, tt := range tests {
		got := rcall(r, "", "relation-list", tt.args...)
		if got.Code != tt.code || (tt.code == 0 && got.Stdout != tt.want) {
			t.Errorf("relation-list %v = %+v, want %d %q", tt.args, got, tt.code, tt.want)
		}
	}
	// The hook's relation is the default.
	f.hookRel = &HookRelation{ID: 7, Endpoint: "database-peers", RemoteUnit: "app/1", RemoteApp: "app"}
	if got := rcall(r, "", "relation-list"); got.Stdout != "app/1\napp/2\n" {
		t.Errorf("%+v", got)
	}
	if rcall(r, "", "relation-list", "extra").Code == 0 {
		t.Error("extra args accepted")
	}
}

func TestRelationGet(t *testing.T) {
	r, f := relReal(t)
	tests := []struct {
		name string
		args []string
		want string
		code int
		err  string
	}{
		{"all keys of a unit", []string{"-r", "7", "-", "app/1"}, "key: one\nother: x\n", 0, ""},
		{"all keys json", []string{"-r", "7", "--format=json", "-", "app/1"}, `{"key":"one","other":"x"}` + "\n", 0, ""},
		{"flags after positionals as ops sends them", []string{"-", "app/1", "-r", "database-peers:7", "--format=json"}, `{"key":"one","other":"x"}` + "\n", 0, ""},
		{"one key", []string{"-r", "7", "key", "app/1"}, "one\n", 0, ""},
		{"missing key is null in json and empty in smart", []string{"-r", "7", "--format=json", "nokey", "app/1"}, "null\n", 0, ""},
		{"missing key smart", []string{"-r", "7", "nokey", "app/1"}, "", 0, ""},
		{"an unset peer is {} and never null", []string{"-r", "7", "--format=json", "-", "app/2"}, "{}\n", 0, ""},
		{"an unset peer smart", []string{"-r", "7", "-", "app/2"}, "{}\n", 0, ""},
		{"own settings", []string{"-r", "7", "--format=json", "-", "app/0"}, `{"ingress-address":"10.0.0.1","private-address":"10.0.0.1"}` + "\n", 0, ""},
		{"application data by app name", []string{"-r", "7", "--app", "-", "app"}, "leader-key: lv\n", 0, ""},
		{"application data by a unit of the app", []string{"-r", "7", "--app", "leader-key", "app/1"}, "lv\n", 0, ""},
		{"the app flag with the default remote app", []string{"-r", "7", "--app", "--format=json"}, "", 1, "no unit or application specified"},
		{"an application name without --app", []string{"-r", "7", "-", "app"}, "", 1, `expected unit name, got application name "app"`},
		{"an invalid name", []string{"-r", "7", "-", "///"}, "", 1, `invalid unit name "///"`},
		{"unknown unit", []string{"-r", "7", "-", "app/9"}, "", 1, "not found"},
		{"unknown relation", []string{"-r", "70", "-", "app/1"}, "", 1, "relation not found"},
		{"no unit", []string{"-r", "7"}, "", 1, "no unit or application specified"},
		{"no relation", []string{"-", "app/1"}, "", 1, "no relation id specified"},
		{"extra args", []string{"-r", "7", "-", "app/1", "x"}, "", 1, "unrecognized args"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rcall(r, "", "relation-get", tt.args...)
			if got.Code != tt.code || (tt.code == 0 && got.Stdout != tt.want) || !strings.Contains(got.Stderr, tt.err) {
				t.Fatalf("relation-get %v = %+v, want code %d %q (%s)", tt.args, got, tt.code, tt.want, tt.err)
			}
		})
	}

	// Defaults inside a relation hook: the remote unit, or the remote application for application hooks.
	f.hookRel = &HookRelation{ID: 7, Endpoint: "database-peers", RemoteUnit: "app/1", RemoteApp: "app"}
	if got := rcall(r, "", "relation-get", "key"); got.Stdout != "one\n" {
		t.Errorf("default unit: %+v", got)
	}
	if got := rcall(r, "", "relation-get", "--app", "leader-key"); got.Stdout != "lv\n" {
		t.Errorf("default app with --app: %+v", got)
	}
	f.hookRel = &HookRelation{ID: 7, Endpoint: "database-peers", RemoteApp: "app"}
	if got := rcall(r, "", "relation-get", "leader-key"); got.Stdout != "lv\n" {
		t.Errorf("application hook: %+v", got)
	}
}

func TestRelationSet(t *testing.T) {
	r, f := relReal(t)
	check := func(want ...setCall) {
		t.Helper()
		if !reflect.DeepEqual(f.sets, want) && !(len(f.sets) == 0 && len(want) == 0) {
			t.Fatalf("sets %+v, want %+v", f.sets, want)
		}
		f.sets = nil
	}
	if got := rcall(r, "", "relation-set", "-r", "7", "a=1", "b=", "c=x=y"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	check(setCall{7, false, map[string]string{"a": "1", "b": "", "c": "x=y"}})

	// YAML from stdin: arguments override the file; an empty value deletes.
	if got := rcall(r, "k: v\nz: \"\"\nn: 5\nover: file\n", "relation-set", "-r", "database-peers:7", "--file", "-", "over=arg"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	check(setCall{7, false, map[string]string{"k": "v", "z": "", "n": "5", "over": "arg"}})

	// JSON is valid YAML, which is what ops sends.
	rcall(r, `{"x":"y"}`, "relation-set", "--file=-", "-r", "7")
	check(setCall{7, false, map[string]string{"x": "y"}})

	// From a file on disk.
	r.ReadFile = func(p string) ([]byte, error) {
		if p != "/tmp/f.yaml" {
			t.Errorf("read %q", p)
		}
		return []byte("from: disk\n"), nil
	}
	rcall(r, "", "relation-set", "--file", "/tmp/f.yaml", "-r", "7")
	check(setCall{7, false, map[string]string{"from": "disk"}})

	// Application settings: leader only.
	if got := rcall(r, "", "relation-set", "-r", "7", "--app", "lk=lv"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	check(setCall{7, true, map[string]string{"lk": "lv"}})
	f.leader = false
	if got := rcall(r, "", "relation-set", "-r", "7", "--app", "lk=lv"); got.Code == 0 || !strings.Contains(got.Stderr, "cannot write relation settings") {
		t.Fatalf("%+v", got)
	}
	check()
	// Unit settings are fine for non-leaders; --format is accepted and deprecated.
	if got := rcall(r, "", "relation-set", "-r", "7", "--format=json", "a=b"); got.Code != 0 || !strings.Contains(got.Stderr, "deprecated") {
		t.Fatalf("%+v", got)
	}
	check(setCall{7, false, map[string]string{"a": "b"}})

	// Errors.
	for _, args := range [][]string{{"a=1"}, {"-r", "70", "a=1"}, {"-r", "7", "novalue"}, {"-r", "7", "--file", "-"}} {
		stdin := ""
		if len(args) > 2 && args[2] == "--file" {
			stdin = "- not\n- a map\n"
		}
		if got := rcall(r, stdin, "relation-set", args...); got.Code == 0 {
			t.Errorf("relation-set %v accepted", args)
		}
	}
	r.ReadFile = func(string) ([]byte, error) { return nil, errors.New("no such file") }
	if got := rcall(r, "", "relation-set", "-r", "7", "--file", "/missing"); got.Code == 0 {
		t.Error("missing file accepted")
	}
	check()
	f.hookRel = &HookRelation{ID: 7, Endpoint: "database-peers", RemoteApp: "app"}
	rcall(r, "", "relation-set", "default=rel")
	check(setCall{7, false, map[string]string{"default": "rel"}})
}

func TestRelationModelGet(t *testing.T) {
	r, f := relReal(t)
	if got := rcall(r, "", "relation-model-get", "-r", "7", "--format=json"); got.Stdout != `{"uuid":"`+f.modelUUID+`"}`+"\n" {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "relation-model-get", "-r", "7"); got.Stdout != "uuid: "+f.modelUUID+"\n" {
		t.Errorf("%+v", got)
	}
	for _, args := range [][]string{{}, {"-r", "70"}, {"-r", "7", "x"}} {
		if rcall(r, "", "relation-model-get", args...).Code == 0 {
			t.Errorf("%v accepted", args)
		}
	}
}
