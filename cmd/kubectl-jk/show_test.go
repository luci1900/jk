package main

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/luci1900/jk/api/v1alpha1"
)

func TestConstraintsCommands(t *testing.T) {
	e := newEnv(t, app("pg", 1))
	if out, _, err := e.run("constraints", "pg"); err != nil || out != "\n" {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err := e.run("set-constraints", "pg", "mem=2G", "arch=arm64"); err != nil {
		t.Fatal(err)
	}
	if out, _, err := e.run("constraints", "pg"); err != nil || out != "arch=arm64 mem=2G\n" {
		t.Fatalf("%q %v", out, err)
	}
	// set-constraints replaces them, and with none it clears them.
	if _, _, err := e.run("set-constraints", "pg", "cpu-power=500"); err != nil {
		t.Fatal(err)
	}
	if out, _, _ := e.run("constraints", "pg"); out != "cpu-power=500\n" {
		t.Fatalf("%q", out)
	}
	if _, _, err := e.run("set-constraints", "pg"); err != nil {
		t.Fatal(err)
	}
	if out, _, _ := e.run("constraints", "pg"); out != "\n" {
		t.Fatalf("%q", out)
	}
	if _, _, err := e.run("set-constraints", "pg", "tags=x=y"); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("%v", err)
	}
	if _, _, err := e.run("constraints", "nope"); err == nil || !strings.Contains(err.Error(), `application "nope" not found`) {
		t.Fatalf("%v", err)
	}
}

func TestShowCommands(t *testing.T) {
	a := app("pg", 1)
	a.Spec.Constraints = &v1alpha1.Constraints{Mem: "2G"}
	a.Spec.Config = map[string]string{"profile": "testing"}
	a.Status.Charm.Metadata = &v1alpha1.JSON{Raw: []byte(`{"name":"pg","provides":{"db":{"interface":"pgsql"}},"peers":{"rep":{"interface":"pg-peer"}}}`)}
	e := newEnv(t, a)
	out, _, err := e.run("show-application", "pg")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"pg:", "charm-rev: 5", "constraints: mem=2G", "profile: testing", "role: provides", "interface: pgsql", "role: peers"} {
		if !strings.Contains(out, want) {
			t.Errorf("show-application lacks %q:\n%s", want, out)
		}
	}
	out, _, err = e.run("show-unit", "pg/0")
	if err != nil || !strings.Contains(out, "pod: pg-0") || !strings.Contains(out, "application: pg") {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("show-unit", "pg/9"); err == nil || !strings.Contains(err.Error(), `unit "pg/9" not found`) {
		t.Fatalf("%v", err)
	}
	out, _, err = e.run("show-model", "--format", "json")
	if err != nil || !strings.Contains(out, `"applications": 1`) {
		t.Fatalf("%q %v", out, err)
	}
}

func TestAccessCommands(t *testing.T) {
	e := newEnv(t)
	if _, _, err := e.run("ssh", "pg/0", "ls", "-l", "/tmp"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.run("ssh", "--container", "pg", "pg/1", "date"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.run("scp", "./a.txt", "pg/0:/tmp/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.run("scp", "pg/2:/etc/hosts", "."); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"-n", "m", "exec", "-i", "-c", "charm", "pg-0", "--", "ls", "-l", "/tmp"},
		{"-n", "m", "exec", "-i", "-c", "pg", "pg-1", "--", "date"},
		{"cp", "-c", "charm", "./a.txt", "m/pg-0:/tmp/a.txt"},
		{"cp", "-c", "charm", "m/pg-2:/etc/hosts", "."},
	}
	if !reflect.DeepEqual(e.kubectl, want) {
		t.Fatalf("got %q\nwant %q", e.kubectl, want)
	}
	if _, _, err := e.run("scp", "a", "b"); err == nil {
		t.Fatal("scp of two local paths succeeded")
	}
	if _, _, err := e.run("ssh", "pg"); err == nil || !strings.Contains(err.Error(), "not a valid unit name") {
		t.Fatalf("%v", err)
	}
}

func TestSSHCommand(t *testing.T) {
	for _, tc := range []struct {
		container string
		tty       bool
		term      string
		command   []string
		want      string
	}{
		{"charm", false, "xterm", nil, "/bin/sh"},
		{"charm", false, "xterm", []string{"ls"}, "ls"},
		{"charm", true, "tmux-256color", []string{"ls"}, "env TERM=tmux-256color ls"},
		{"charm", true, "", []string{"ls"}, "ls"},
		{"charm", true, "xterm", nil, "env TERM=xterm bash --login"},
		{"pg", true, "xterm", nil, "/bin/sh"},
		{"pg", true, "xterm", []string{"date"}, "date"},
	} {
		if got := strings.Join(sshCommand(tc.container, tc.tty, tc.term, tc.command), " "); got != tc.want {
			t.Errorf("%+v: got %q", tc, got)
		}
	}
}

func TestDebugLog(t *testing.T) {
	pod := func(name, app string) runtime.Object {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "m", Labels: map[string]string{v1alpha1.AppLabel: app}}}
	}
	e := newEnv(t)
	e.pods = []runtime.Object{pod("pg-0", "pg"), pod("pg-1", "pg"), pod("web-0", "web"),
		// The registry and other pods have no app label, and jk's own operator is not in the model.
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "m"}}}
	out, _, err := e.run("debug-log", "--no-tail", "--include", "pg/1", "--include", "web")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "unit-pg-1: ") || !strings.Contains(out, "unit-web-0: ") || strings.Contains(out, "unit-pg-0") {
		t.Fatalf("%q", out)
	}
	if _, _, err = e.run("debug-log", "--no-tail", "--include", "nope"); err == nil {
		t.Fatal("no pods matched, want an error")
	}
}

func TestCompletion(t *testing.T) {
	e := newEnv(t, app("pg", 2), app("web", 1))
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{"show-unit", ""}, []string{"pg/0", "pg/1", "web/0"}},
		{[]string{"show-unit", "pg/1"}, []string{"pg/1"}},
		{[]string{"ssh", "w"}, []string{"web/0"}},
		{[]string{"run", "pg/l"}, []string{"pg/leader"}},
		{[]string{"scale-application", ""}, []string{"pg", "web"}},
		{[]string{"status", "w"}, []string{"web", "web/0"}},
		{[]string{"switch", ""}, []string{"m"}},
		{[]string{"exec", "--unit", "p"}, []string{"pg/0", "pg/1"}},
		{[]string{"debug-log", "--include", "we"}, []string{"web", "web/0"}},
	} {
		out, _, err := e.run(append([]string{"__complete"}, c.args...)...)
		if err != nil {
			t.Fatalf("%v: %v", c.args, err)
		}
		var got []string
		for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
			if l != "" && !strings.HasPrefix(l, ":") && !strings.HasPrefix(l, "Completion ended") {
				got = append(got, l)
			}
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v: got %q, want %q\n%s", c.args, got, c.want, out)
		}
	}
	// A command that takes one application stops offering names after it.
	out, _, _ := e.run("__complete", "scale-application", "pg", "")
	if strings.Contains(out, "web") {
		t.Errorf("second argument completed an application:\n%s", out)
	}
}

func TestModelConfigResetAndSet(t *testing.T) {
	e := newEnv(t)
	if _, _, err := e.run("model-config", "update-status-hook-interval=20s", "logging-config=x"); err != nil {
		t.Fatal(err)
	}
	// --reset with key=value pairs resets the first and sets the rest.
	if _, _, err := e.run("model-config", "--reset", "update-status-hook-interval", "logging-config=y"); err != nil {
		t.Fatal(err)
	}
	if out, _, _ := e.run("model-config", "update-status-hook-interval"); out != "5m\n" {
		t.Errorf("not reset: %q", out)
	}
	if out, _, _ := e.run("model-config", "logging-config"); out != "y\n" {
		t.Errorf("not set: %q", out)
	}
}
