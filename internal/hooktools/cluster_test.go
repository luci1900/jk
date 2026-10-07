// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import (
	"strings"
	"testing"
	"time"
)

func TestGoalState(t *testing.T) {
	r, f := newReal(t)
	if got := rcall(r, "", "goal-state"); got.Code != 0 || got.Stdout != "relations: {}\nunits: {}\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "goal-state", "--format=json"); got.Stdout != `{"relations":{},"units":{}}`+"\n" {
		t.Fatalf("%+v", got)
	}
	since := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	f.goal = GoalState{
		Units:     map[string]GoalStatus{"app/0": {Status: "active", Since: since}, "app/1": {Status: "dying"}},
		Relations: map[string]map[string]GoalStatus{"db": {"other": {Status: "joined", Since: since}, "other/0": {Status: "active", Since: since}}},
	}
	want := `{"relations":{"db":{"other":{"status":"joined","since":"2026-03-04 05:06:07Z"},"other/0":{"status":"active","since":"2026-03-04 05:06:07Z"}}},` +
		`"units":{"app/0":{"status":"active","since":"2026-03-04 05:06:07Z"},"app/1":{"status":"dying","since":"1970-01-01 00:00:00Z"}}}` + "\n"
	if got := rcall(r, "", "goal-state", "--format=json"); got.Stdout != want {
		t.Fatalf("%s\nwant %s", got.Stdout, want)
	}
	yaml := rcall(r, "", "goal-state")
	if !strings.Contains(yaml.Stdout, "app/1:\n    since: 1970-01-01 00:00:00Z\n    status: dying") {
		t.Fatalf("%s", yaml.Stdout)
	}
	if rcall(r, "", "goal-state", "--format=xml").Code == 0 || rcall(r, "", "goal-state", "x").Code == 0 {
		t.Fatal("bad input accepted")
	}
	f.goalErr = NotFoundf("application")
	if rcall(r, "", "goal-state").Code == 0 {
		t.Fatal("backend error swallowed")
	}
}

func TestNetworkGetBindingsAndRelation(t *testing.T) {
	r, f := newReal(t)
	f.bindings = []string{"database-peers", "restart", "db"}
	f.addRel(7, "database-peers", "app")
	if got := rcall(r, "", "network-get", "database-peers", "--ingress-address"); got.Code != 0 || got.Stdout != "10.1.2.3\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "network-get", "unknown"); got.Code == 0 || !strings.Contains(got.Stderr, `binding name "unknown" not defined`) {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "network-get", "db", "-r", "database-peers:7", "--bind-address"); got.Stdout != "10.1.2.3\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "network-get", "db", "-r", "70"); got.Code == 0 {
		t.Fatalf("%+v", got)
	}
	// The full output has the interface and MAC of the pod.
	got := rcall(r, "", "network-get", "db")
	for _, want := range []string{"interface-name: eth0", "mac-address: aa:bb:cc:dd:ee:ff", "value: 10.1.2.3", "cidr: 10.1.2.3/32", "- 10.1.2.3/32", "- 10.1.2.3"} {
		if !strings.Contains(got.Stdout, want) {
			t.Errorf("missing %q in\n%s", want, got.Stdout)
		}
	}
}
