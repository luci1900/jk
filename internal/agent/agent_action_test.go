package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

// actionDispatch runs the script action-<name> for actions and does what dispatchScript does for hooks.
const actionDispatch = `#!/bin/sh
A="$CHARM_DIR/.."
case "$JUJU_DISPATCH_PATH" in
actions/*)
  n="${JUJU_DISPATCH_PATH#actions/}"
  env | sort > "$A/aenv-$n"
  echo "$n" >> "$A/actions.log"
  [ -f "$A/action-$n" ] && . "$A/action-$n"
  exit 0;;
esac
echo "$JUJU_HOOK_NAME" >> "$A/hooks.log"
[ -f "$A/script-$JUJU_HOOK_NAME" ] && . "$A/script-$JUJU_HOOK_NAME"
exit 0
`

func actionHarness(t *testing.T) *harness {
	h := newHarness(t)
	h.write("charm/dispatch", actionDispatch)
	h.a.cfg.ActionPollInterval = 20 * time.Millisecond
	h.cfg.ActionPollInterval = 20 * time.Millisecond
	h.reconcile()
	h.hooks()
	return h
}

func (h *harness) results(name string) map[string]any {
	h.t.Helper()
	a := h.kube.action(name)
	m := map[string]any{}
	if a.Status.Results != nil {
		if err := json.Unmarshal(a.Status.Results.Raw, &m); err != nil {
			h.t.Fatal(err)
		}
	}
	return m
}

func (h *harness) wantAction(name string, state v1alpha1.ActionState, msg string) v1alpha1.Action {
	h.t.Helper()
	a := h.kube.action(name)
	if a.Status.State != state || a.Status.Message != msg {
		h.t.Fatalf("action %s: %s %q, want %s %q\nlog:\n%s", name, a.Status.State, a.Status.Message, state, msg, h.log.String())
	}
	return a
}

func TestActionSuccessResultsLogAndEnvironment(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.write("action-backup", `action-log "step one"
action-log two
P=$(action-get target)
N=$(action-get opts.level)
action-set out.path="/tmp/$P" level="$N" a.b=1 a.c=2
echo hello-out
echo hello-err >&2
state-set from-action=yes
`)
	h.kube.addAction("act-1", "backup", 7, `{"target":"db","opts":{"level":5}}`)
	h.reconcile()
	a := h.wantAction("act-1", "completed", "")
	if a.Status.Started == nil || a.Status.Completed == nil {
		t.Fatalf("times %+v", a.Status)
	}
	if len(a.Status.Log) != 2 || a.Status.Log[0].Message != "step one" || a.Status.Log[1].Message != "two" {
		t.Fatalf("log %+v", a.Status.Log)
	}
	res := h.results("act-1")
	if res["return-code"] != float64(0) || res["stdout"] != "hello-out\n" || res["stderr"] != "hello-err\n" {
		t.Fatalf("results %v", res)
	}
	if res["level"] != "5" || res["out"].(map[string]any)["path"] != "/tmp/db" || res["a"].(map[string]any)["c"] != "2" {
		t.Fatalf("results %v", res)
	}
	env := map[string]string{}
	for _, l := range strings.Split(h.read("aenv-backup"), "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			env[k] = v
		}
	}
	if env["JUJU_ACTION_NAME"] != "backup" || env["JUJU_ACTION_UUID"] != "7" || env["JUJU_ACTION_TAG"] != "action-7" ||
		env["JUJU_DISPATCH_PATH"] != "actions/backup" || env["JUJU_HOOK_NAME"] != "" {
		t.Fatalf("env %v", env)
	}
	// The action's other side effects were committed, the agent is idle and the action does not run again.
	if h.kube.spec().State["from-action"] != "yes" {
		t.Fatalf("state %v", h.kube.spec().State)
	}
	wantStatus(t, h.kube.spec().AgentStatus, AgentIdle, "")
	h.reconcile()
	if got := strings.Fields(h.read("actions.log")); len(got) != 1 {
		t.Fatalf("ran %v", got)
	}
}

func TestActionsRunInIDOrderOneAtATime(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.kube.addAction("b", "second", 2, "")
	h.kube.addAction("a", "third", 3, "")
	h.kube.addAction("c", "first", 1, "")
	h.kube.addAction("unprocessed", "ignored", 0, "")
	h.kube.actions["unprocessed"].Status.State = ""
	h.kube.addAction("noid", "ignored", 0, "")
	h.reconcile()
	if got := strings.Join(strings.Fields(h.read("actions.log")), ","); got != "first,second,third" {
		t.Fatalf("order %s", got)
	}
	h.wantAction("unprocessed", "", "")
	h.wantAction("noid", "pending", "")
}

func TestActionFailures(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.write("action-bad", "echo oops >&2\nexit 3\n")
	h.write("action-soft", "action-set x=1\naction-fail 'did not work'\nstate-set soft=1\n")
	h.write("action-default", "action-fail\n")
	h.kube.addAction("bad", "bad", 1, "")
	h.kube.addAction("soft", "soft", 2, "")
	h.kube.addAction("default", "default", 3, "")
	h.kube.addAction("missing", "nothere", 4, "")
	h.kube.addAction("badparams", "bad", 5, `[1,2]`)
	h.kube.addAction("nullparams", "bad", 6, `null`)
	h.reconcile()
	// A script that exits non-zero fails and commits nothing; it still reports its output.
	h.wantAction("bad", "failed", "exit status 3")
	if r := h.results("bad"); r["return-code"] != float64(3) || r["stderr"] != "oops\n" {
		t.Fatalf("%v", r)
	}
	// action-fail with the script exiting 0 fails with the message and commits other changes.
	h.wantAction("soft", "failed", "did not work")
	if h.results("soft")["x"] != "1" || h.kube.spec().State["soft"] != "1" {
		t.Fatalf("%v %v", h.results("soft"), h.kube.spec().State)
	}
	h.wantAction("default", "failed", "action failed without reason given, check action for errors")
	// A charm that has no handler: the dispatch script does nothing for it, so it is not a failure.
	h.wantAction("missing", "completed", "")
	h.wantAction("badparams", "failed", "invalid action parameters: not an object")
	h.wantAction("nullparams", "failed", "exit status 3")
}

func TestActionWithoutDispatchIsNotImplemented(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.remove("charm/dispatch")
	h.kube.addAction("x", "anything", 1, "")
	h.reconcile()
	h.wantAction("x", "failed", `action not implemented on unit "app/0"`)
}

func TestActionTimeout(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.write("action-slow", "echo started\nsleep 30\n")
	a := h.kube.addAction("slow", "slow", 1, "")
	a.Spec.TimeoutSeconds = 1
	start := time.Now()
	h.reconcile()
	h.wantAction("slow", "failed", "action timed out after 1s")
	if time.Since(start) > 15*time.Second {
		t.Fatalf("took %v", time.Since(start))
	}
	if h.results("slow")["stdout"] != "started\n" {
		t.Fatalf("%v", h.results("slow"))
	}
}

func TestActionAbort(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.write("action-slow", "echo started\nsleep 30\n")
	h.kube.addAction("slow", "slow", 1, "")
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.reconcile()
	}()
	deadline := time.Now().Add(10 * time.Second)
	for h.kube.action("slow").Status.State != "running" {
		if time.Now().After(deadline) {
			t.Fatal("never running")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.kube.action("slow").Status.Started == nil {
		t.Fatal("no start time")
	}
	h.kube.mu.Lock()
	h.kube.actions["slow"].Status.State = "aborting"
	h.kube.mu.Unlock()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("abort did not stop the action")
	}
	h.wantAction("slow", "aborted", "action aborted")
}

func TestActionInterruptedIsFailedNotRerun(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.write("action-once", "echo ran\n")
	h.kube.addAction("crashed", "once", 1, "")
	h.kube.actions["crashed"].Status.State = "running"
	h.reconcile()
	h.wantAction("crashed", "failed", "action terminated")
	if h.read("actions.log") != "" {
		t.Fatalf("rerun: %q", h.read("actions.log"))
	}
}

func TestActionCancelledWhilePendingIsLeftAlone(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.kube.addAction("c", "x", 1, "")
	h.kube.actions["c"].Status.State = "cancelled"
	h.reconcile()
	h.wantAction("c", "cancelled", "")
	if h.read("actions.log") != "" {
		t.Fatal("ran")
	}
}

func TestActionsOfADyingUnitFail(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.kube.addAction("d", "x", 1, "")
	h.a.Terminate()
	h.reconcile()
	h.wantAction("d", "failed", resolver.ActionDyingMessage)
	if h.read("actions.log") != "" {
		t.Fatal("ran")
	}
}

func TestActionRunsDuringHookError(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.write("script-config-changed", "exit 1\n")
	h.kube.setConfig(map[string]string{"greeting": "new"})
	h.reconcile()
	h.kube.addAction("e", "echo", 1, "")
	h.write("action-echo", "echo hi\n")
	h.reconcile()
	h.wantAction("e", "completed", "")
	wantStatus(t, h.kube.spec().AgentStatus, AgentError, "hook failed: config-changed")
}

func TestJujuExec(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	h.kube.addAction("ok", v1alpha1.ActionExec, 1, `{"command":"echo out; echo err >&2; is-leader; echo $JUJU_ACTION_NAME $JUJU_UNIT_NAME; pwd"}`)
	h.kube.addAction("code", v1alpha1.ActionExec, 2, `{"command":"exit 4"}`)
	h.kube.addAction("nocmd", v1alpha1.ActionExec, 3, `{}`)
	h.kube.addAction("bin", v1alpha1.ActionExec, 4, `{"command":"printf '\\377\\376'"}`)
	h.kube.addAction("slow", v1alpha1.ActionExec, 5, `{"command":"sleep 30","timeout":1000000000}`)
	h.kube.addAction("slow2", v1alpha1.ActionExec, 6, `{"command":"sleep 30"}`)
	h.kube.actions["slow2"].Spec.TimeoutSeconds = 1
	h.reconcile()
	h.wantAction("ok", "completed", "")
	r := h.results("ok")
	if r["return-code"] != float64(0) || r["stderr"] != "err\n" || !strings.HasPrefix(r["stdout"].(string), "out\nFalse\njuju-exec app/0\n") ||
		!strings.HasSuffix(strings.TrimSpace(r["stdout"].(string)), "/charm") {
		t.Fatalf("%v", r)
	}
	// A failing command still completes, with its exit code.
	h.wantAction("code", "completed", "")
	if h.results("code")["return-code"] != float64(4) {
		t.Fatalf("%v", h.results("code"))
	}
	h.wantAction("nocmd", "failed", "no command parameter to juju-exec action")
	h.wantAction("bin", "completed", "")
	if r := h.results("bin"); r["stdout-encoding"] != "base64" || r["stdout"] != "//4=" {
		t.Fatalf("%v", r)
	}
	h.wantAction("slow", "failed", "action timed out after 1s")
	h.wantAction("slow2", "failed", "action timed out after 1s")
}

func TestActionWritesStayOffForOtherUnitsAndOldStates(t *testing.T) {
	t.Parallel()
	h := actionHarness(t)
	a := h.kube.addAction("other", "x", 1, "")
	a.Spec.Unit = "app/9"
	h.reconcile()
	h.wantAction("other", "pending", "")
}
