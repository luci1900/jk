//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/util/retry"

	"github.com/luci1900/jk/api/v1alpha1"
)

func wantState(t *testing.T, a *v1alpha1.Action, want v1alpha1.ActionState) {
	t.Helper()
	if a.Status.State != want {
		t.Errorf("action %s (%s on %s) state %q message %q, want %q", a.Name, a.Spec.Name, a.Spec.Unit, a.Status.State, a.Status.Message, want)
	}
}

// setActionState writes status.state, as the CLI's cancel does.
func setActionState(t *testing.T, ns, name string, state v1alpha1.ActionState) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		a, err := getAction(ns, name)
		if err != nil {
			return err
		}
		a.Status.State = state
		return kube.Status().Update(context.Background(), a)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestActions runs Action CRs against the two units of jk-test. The subtests share the deployment and run in order.
func TestActions(t *testing.T) {
	t.Parallel()
	ns := newModel(t)
	deploy(t, ns, 2, nil)
	waitUnitsActive(t, 6*time.Minute, ns, app, 2, "greeting")
	var lead int
	eventually(t, time.Minute, "a leader", func() (bool, error) {
		a, err := application(ns)
		if err != nil {
			return false, err
		}
		_, err = fmt.Sscanf(a.Status.Leader, app+"/%d", &lead)
		return err == nil, err
	})
	u0 := jujuUnit(app, 0)

	t.Run("echo", func(t *testing.T) {
		a := runAction(t, ns, u0, "echo", "  parameters:\n    text: hi\n")
		wantState(t, a, "completed")
		res := results(t, a)
		// parameters not given take their defaults from actions.yaml; action-set with a dotted key makes a nested result
		for path, want := range map[string]string{"echo.text": "hi", "echo.times": "2", "echo.repeated": "hi hi", "upper": "false", "ratio": "0.5", "tags": ""} {
			// ops formats a Python bool as "True"/"False" before calling action-set, as it does under juju.
			if got := result(res, strings.Split(path, ".")...); !strings.EqualFold(got, want) {
				t.Errorf("result %s = %q, want %q (results %v)", path, got, want, res)
			}
		}
		// action-log lines, in order
		if got := logMessages(a); len(got) != 2 || got[0] != "echo: hi" || got[1] != "times: 2" {
			t.Errorf("log %q", got)
		}
		if a.Status.Started == nil || a.Status.Completed == nil {
			t.Errorf("started %v completed %v", a.Status.Started, a.Status.Completed)
		}
		// given parameters win over defaults and keep their types
		a = runAction(t, ns, u0, "echo", "  parameters:\n    text: yo\n    times: 3\n    upper: true\n    ratio: 1.5\n    tags: [a, b]\n")
		wantState(t, a, "completed")
		res = results(t, a)
		for path, want := range map[string]string{"echo.text": "YO", "echo.times": "3", "echo.repeated": "YO YO YO", "upper": "true", "ratio": "1.5", "tags": "a,b"} {
			// ops formats a Python bool as "True"/"False" before calling action-set, as it does under juju.
			if got := result(res, strings.Split(path, ".")...); !strings.EqualFold(got, want) {
				t.Errorf("result %s = %q, want %q (results %v)", path, got, want, res)
			}
		}
	})

	t.Run("fail", func(t *testing.T) {
		a := runAction(t, ns, u0, "fail", "  parameters:\n    message: boom\n")
		wantState(t, a, "failed")
		if a.Status.Message != "boom" {
			t.Errorf("message %q, want the action-fail message", a.Status.Message)
		}
		if got := result(results(t, a), "partial"); got != "yes" {
			t.Errorf("results set before action-fail are kept: partial = %q", got)
		}
		if got := logMessages(a); len(got) != 1 || got[0] != "about to fail" {
			t.Errorf("log %q", got)
		}
		a = runAction(t, ns, u0, "fail", "")
		wantState(t, a, "failed")
		if a.Status.Message != "failed on purpose" {
			t.Errorf("default message %q", a.Status.Message)
		}
	})

	t.Run("validation", func(t *testing.T) {
		// The operator rejects what the schema in actions.yaml does not allow, before any unit runs it.
		for _, c := range []struct{ what, params, mention string }{
			{"missing required parameter", "  parameters:\n    times: 2\n", "text"},
			{"wrong type", "  parameters:\n    text: validation-marker\n    times: many\n", "times"},
			{"wrong boolean type", "  parameters:\n    text: validation-marker\n    upper: 3\n", "upper"},
			{"unknown parameter", "  parameters:\n    text: validation-marker\n    bogus: 1\n", "bogus"},
			{"no parameters at all", "", "text"},
		} {
			a := runAction(t, ns, u0, "echo", c.params)
			if a.Status.State != "failed" || !strings.Contains(a.Status.Message, c.mention) {
				t.Errorf("%s: state %q message %q, want failed mentioning %q", c.what, a.Status.State, a.Status.Message, c.mention)
			}
			if len(a.Status.Log) != 0 || a.Status.Results != nil {
				t.Errorf("%s: the unit ran it: log %q results %v", c.what, logMessages(a), results(t, a))
			}
		}
		if strings.Contains(agentLogsOf(t, ns, app, 0), "validation-marker") {
			t.Errorf("the charm ran an action that failed validation")
		}
		a := runAction(t, ns, u0, "no-such-action", "")
		if a.Status.State != "failed" || !strings.Contains(a.Status.Message, "no-such-action") {
			t.Errorf("unknown action: state %q message %q", a.Status.State, a.Status.Message)
		}
		a = runAction(t, ns, jujuUnit(app, 9), "whoami", "")
		wantState(t, a, "failed")
	})

	t.Run("identity", func(t *testing.T) {
		// Three tasks created together run one at a time per unit and have distinct ids; tasks on the two units run side by side.
		var created []*v1alpha1.Action
		for _, unit := range []string{u0, u0, jujuUnit(app, 1)} {
			created = append(created, createAction(t, ns, unit, "whoami", ""))
		}
		ids := map[int64]bool{}
		ops := map[string]bool{}
		leaders := 0
		for i, c := range created {
			a := waitAction(t, ns, c.Name)
			wantState(t, a, "completed")
			if a.Status.ID <= 0 || ids[a.Status.ID] {
				t.Errorf("task id %d (ids so far %v)", a.Status.ID, ids)
			}
			ids[a.Status.ID] = true
			res := results(t, a)
			id := strconv.FormatInt(a.Status.ID, 10)
			if got := result(res, "action-id"); got != id {
				t.Errorf("JUJU_ACTION_UUID = %q, want the integer task id %s", got, id)
			}
			if got := result(res, "action-tag"); got != "action-"+id {
				t.Errorf("JUJU_ACTION_TAG = %q, want action-%s", got, id)
			}
			if got := result(res, "action-name"); got != "whoami" {
				t.Errorf("JUJU_ACTION_NAME = %q", got)
			}
			if got := result(res, "unit"); got != a.Spec.Unit {
				t.Errorf("task %d ran on %q, addressed to %q", i, got, a.Spec.Unit)
			}
			if got := result(res, "app"); got != app {
				t.Errorf("app = %q", got)
			}
			isLeader := result(res, "leader") == "true"
			if isLeader != (a.Spec.Unit == jujuUnit(app, lead)) {
				t.Errorf("task on %s says leader=%v, the leader is unit %d", a.Spec.Unit, isLeader, lead)
			}
			if isLeader {
				leaders++
			}
			op := a.Labels[v1alpha1.OperationLabel]
			if _, err := strconv.Atoi(op); err != nil {
				t.Errorf("operation label %q is not an integer", op)
			}
			ops[op] = true
		}
		if leaders == 0 {
			t.Errorf("no task ran on the leader")
		}
		if len(ops) != len(created) {
			t.Errorf("separately created tasks share operation ids: %v", ops)
		}
	})

	t.Run("exec", func(t *testing.T) {
		a := runAction(t, ns, u0, v1alpha1.ActionExec, "  parameters:\n    command: echo out; echo err >&2; exit 3\n")
		wantState(t, a, "completed")
		res := results(t, a)
		if got := result(res, "return-code"); got != "3" {
			t.Errorf("return-code %q (results %v)", got, res)
		}
		if got := result(res, "stdout"); strings.TrimSpace(got) != "out" {
			t.Errorf("stdout %q", got)
		}
		if got := result(res, "stderr"); strings.TrimSpace(got) != "err" {
			t.Errorf("stderr %q", got)
		}
		// through the hook path: hook environment and hook tools
		a = runAction(t, ns, u0, v1alpha1.ActionExec, "  parameters:\n    command: echo $JUJU_UNIT_NAME; config-get greeting; is-leader\n")
		wantState(t, a, "completed")
		lines := strings.Fields(result(results(t, a), "stdout"))
		if len(lines) != 3 || lines[0] != u0 || lines[1] != "hello" || (strings.ToLower(lines[2]) == "true") != (lead == 0) {
			t.Errorf("stdout lines %q, want %s, hello and is-leader=%v (results %v)", lines, u0, lead == 0, results(t, a))
		}
		if got := result(results(t, a), "return-code"); got != "0" {
			t.Errorf("return-code %q", got)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		started := time.Now()
		a := runAction(t, ns, u0, "sleep", "  timeoutSeconds: 2\n  parameters:\n    seconds: 600\n")
		if a.Status.State != "failed" && a.Status.State != "aborted" {
			t.Errorf("state %q message %q, want the task ended by its timeout", a.Status.State, a.Status.Message)
		}
		if !strings.Contains(strings.ToLower(a.Status.Message), "time") {
			t.Errorf("message %q does not say it timed out", a.Status.Message)
		}
		if d := time.Since(started); d > 2*time.Minute {
			t.Errorf("took %s", d)
		}
	})

	t.Run("abort", func(t *testing.T) {
		a := createAction(t, ns, u0, "sleep", "  parameters:\n    seconds: 600\n")
		eventually(t, time.Minute, "the sleep to run", func() (bool, error) {
			cur, err := getAction(ns, a.Name)
			if err != nil {
				return false, err
			}
			if cur.Status.State != "running" {
				return false, fmt.Errorf("state %q", cur.Status.State)
			}
			return true, nil
		})
		setActionState(t, ns, a.Name, "aborting")
		a = waitAction(t, ns, a.Name)
		wantState(t, a, "aborted")
		// the unit goes on: the next task runs
		wantState(t, runAction(t, ns, u0, "whoami", ""), "completed")
	})
}
