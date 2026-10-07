// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/operation/runaction.go and
// failaction.go, runner/runner.go (RunAction, runJujuExecAction, updateActionResults) and runner/context
// (finalizeAction).

package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
	"github.com/luci1900/jk/internal/hooktools"
)

// DefaultActionPollInterval is how often a running action looks for an abort request.
const DefaultActionPollInterval = time.Second

// maxActionOutput bounds the stdout and stderr kept in an Action's results (the object must stay under etcd's limit).
const maxActionOutput = 256 * 1024

// Action states in Action.status.state.
const (
	actionPending   = v1alpha1.ActionState("pending")
	actionRunning   = v1alpha1.ActionState("running")
	actionCompleted = v1alpha1.ActionState("completed")
	actionFailed    = v1alpha1.ActionState("failed")
	actionAborting  = v1alpha1.ActionState("aborting")
	actionAborted   = v1alpha1.ActionState("aborted")
)

// actionSnapshot fills the snapshot with the unit's pending Actions (by task id) and the ones recorded as running that
// this process is not running. Actions the process finished are skipped while the cache still shows an older state.
func (a *Agent) actionSnapshot(ctx context.Context, snap *resolver.Snapshot) {
	acts, err := a.kube.Actions(ctx)
	if err != nil {
		a.logf("listing actions: %v", err)
		return
	}
	if a.actionObjs == nil {
		a.actionObjs = map[string]v1alpha1.Action{}
	}
	for k := range a.actionObjs {
		delete(a.actionObjs, k)
	}
	seen := map[string]bool{}
	for _, act := range acts {
		seen[act.Name] = true
		if a.actionsDone[act.Name] {
			continue
		}
		a.actionObjs[act.Name] = act
		ref := resolver.ActionRef{Name: act.Name, ID: act.Status.ID}
		switch act.Status.State {
		case actionPending:
			if act.Status.ID > 0 {
				snap.PendingActions = append(snap.PendingActions, ref)
			}
		case actionRunning, actionAborting:
			snap.OrphanedActions = append(snap.OrphanedActions, ref)
		}
	}
	// Forget finished actions that are gone or that the cache has caught up on.
	for name := range a.actionsDone {
		if !seen[name] {
			delete(a.actionsDone, name)
		}
	}
	sort.Slice(snap.PendingActions, func(i, j int) bool {
		x, y := snap.PendingActions[i], snap.PendingActions[j]
		if x.ID != y.ID {
			return x.ID < y.ID
		}
		return x.Name < y.Name
	})
	sort.Slice(snap.OrphanedActions, func(i, j int) bool { return snap.OrphanedActions[i].Name < snap.OrphanedActions[j].Name })
}

// failAction finishes an Action as failed without running it. It only writes if the Action is still in the state
// the resolver saw (pending, or running for an orphan).
func (a *Agent) failAction(ctx context.Context, ref resolver.ActionRef, msg string) error {
	a.logf("action %s (%d) failed: %s", ref.Name, ref.ID, msg)
	now := metav1.NewTime(a.clk.Now())
	err := a.kube.UpdateActionStatus(ctx, ref.Name, func(act *v1alpha1.Action) bool {
		if s := act.Status.State; s != actionPending && s != actionRunning && s != actionAborting {
			return false
		}
		act.Status.State, act.Status.Message, act.Status.Completed = actionFailed, msg, &now
		return true
	})
	if err != nil {
		return err
	}
	a.markActionDone(ref.Name)
	return nil
}

func (a *Agent) markActionDone(name string) {
	if a.actionsDone == nil {
		a.actionsDone = map[string]bool{}
	}
	a.actionsDone[name] = true
}

// actionTimeout is the time an action may run: spec.timeoutSeconds, else for juju-exec the timeout parameter that
// juju sends (nanoseconds).
func actionTimeout(act *v1alpha1.Action, params map[string]any) time.Duration {
	if act.Spec.TimeoutSeconds > 0 {
		return time.Duration(act.Spec.TimeoutSeconds) * time.Second
	}
	if act.Spec.Name == v1alpha1.ActionExec {
		if ns, ok := params["timeout"].(float64); ok && ns > 0 {
			return time.Duration(ns)
		}
	}
	return 0
}

// parseActionParams decodes spec.parameters: an object, or nothing.
func parseActionParams(raw *v1alpha1.JSON) (map[string]any, error) {
	if raw == nil || len(bytes.TrimSpace(raw.Raw)) == 0 || string(bytes.TrimSpace(raw.Raw)) == "null" {
		return map[string]any{}, nil
	}
	var params map[string]any
	if err := json.Unmarshal(raw.Raw, &params); err != nil {
		return nil, fmt.Errorf("invalid action parameters: not an object")
	}
	return params, nil
}

// runAction is juju's runAction operation: mark the Action running, run its script through the hook path with the
// hook tools, commit what the script changed and record the outcome.
func (a *Agent) runAction(ctx context.Context, ref resolver.ActionRef, snap resolver.Snapshot, w *world) error {
	obj, ok := a.actionObjs[ref.Name]
	if !ok {
		a.markActionDone(ref.Name)
		return nil
	}
	params, perr := parseActionParams(obj.Spec.Parameters)
	if perr != nil {
		return a.failAction(ctx, ref, perr.Error())
	}

	// Take the Action: only if it is still pending (it may have been cancelled meanwhile).
	started := false
	now := metav1.NewTime(a.clk.Now())
	if err := a.kube.UpdateActionStatus(ctx, ref.Name, func(act *v1alpha1.Action) bool {
		if act.Status.State != actionPending {
			return false
		}
		act.Status.State, act.Status.Started, act.Status.Message = actionRunning, &now, ""
		started = true
		return true
	}); err != nil {
		return err
	}
	if !started {
		a.markActionDone(ref.Name)
		return nil
	}
	name := obj.Spec.Name
	a.logf("running action %s (%d) %s", ref.Name, ref.ID, name)
	if err := a.store.SetAgentStatus(ctx, AgentExecuting, "running action "+name); err != nil {
		return err
	}

	act := &actionRun{obj: ref.Name, name: name, id: ref.ID, params: params, results: map[string]any{}}
	state, msg, results, err := a.executeAction(ctx, &obj, act, w)
	if err != nil {
		return err // shutting down: the Action stays running, and the next agent fails it
	}
	if err := a.store.SetAgentStatus(ctx, AgentIdle, ""); err != nil {
		return err
	}
	return a.finishAction(ctx, ref, state, msg, results)
}

// finishAction writes the outcome. If the object would be too big for the API server it is written again without the
// results.
func (a *Agent) finishAction(ctx context.Context, ref resolver.ActionRef, state v1alpha1.ActionState, msg string, results map[string]any) error {
	done := metav1.NewTime(a.clk.Now())
	write := func(results map[string]any, msg string) error {
		raw, err := json.Marshal(results)
		if err != nil {
			return err
		}
		return a.kube.UpdateActionStatus(ctx, ref.Name, func(act *v1alpha1.Action) bool {
			act.Status.State, act.Status.Message, act.Status.Completed = state, msg, &done
			act.Status.Results = &v1alpha1.JSON{Raw: raw}
			return true
		})
	}
	err := write(results, msg)
	if err != nil {
		a.logf("action %s: writing results: %v; writing without them", ref.Name, err)
		code, _ := results["return-code"]
		err = write(map[string]any{"return-code": code}, strings.TrimSpace(msg+" (results could not be saved)"))
	}
	if err != nil {
		return err
	}
	a.markActionDone(ref.Name)
	return nil
}

// executeAction runs the action's process and flushes the hook context. It returns the final state, message and results,
// or an error when the agent is shutting down.
func (a *Agent) executeAction(ctx context.Context, obj *v1alpha1.Action, act *actionRun, w *world) (v1alpha1.ActionState, string, map[string]any, error) {
	cfg, _ := a.effective(w)
	a.hookSeq++
	execID := newExecID()
	hc := newHookContext(ctx, a, cfg, resolver.HookInfo{}, execID, w)
	hc.act = act
	ctxID := newContextID(a.cfg.Unit, "action-"+act.name, a.hookSeq)
	socket := filepath.Join(a.cfg.AgentDir(), "agent.socket")
	srv, err := hooktools.Listen("unix", socket, ctxID, &hooktools.Real{B: hc, Now: a.clk.Now})
	if err != nil {
		return actionFailed, "cannot serve hook tools: " + err.Error(), map[string]any{}, nil
	}
	defer srv.Close()

	var stdout, stderr limitedBuffer
	logw := &lineWriter{out: a.cfg.Log, prefix: fmt.Sprintf("jk-agent: action %s: ", act.name)}
	spec := HookSpec{
		Name: act.name, Dir: a.cfg.CharmDir(), Path: filepath.Join(a.cfg.CharmDir(), "dispatch"),
		Env:    a.hookEnv(resolver.HookInfo{}, act, ctxID, socket, w.model),
		Stdout: teeWriter{&stdout, logw}, Stderr: teeWriter{&stderr, logw},
	}
	isExec := act.name == v1alpha1.ActionExec
	if isExec {
		cmd, ok := act.params["command"].(string)
		if !ok {
			return actionFailed, "no command parameter to juju-exec action", map[string]any{}, nil
		}
		shell := "/bin/bash"
		if p, err := exec.LookPath("bash"); err == nil {
			shell = p
		} else if _, err := os.Stat(shell); err != nil {
			shell = "/bin/sh"
		}
		spec.Path, spec.Args = shell, []string{"-c", cmd}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var timedOut, aborted atomic.Bool
	timeout := actionTimeout(obj, act.params)
	if timeout > 0 {
		t := time.AfterFunc(timeout, func() { timedOut.Store(true); cancel() })
		defer t.Stop()
	}
	pollDone := make(chan struct{})
	go func() {
		interval := a.cfg.ActionPollInterval
		if interval <= 0 {
			interval = DefaultActionPollInterval
		}
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-pollDone:
				return
			case <-runCtx.Done():
				return
			case <-tick.C:
				if acts, err := a.kube.Actions(runCtx); err == nil {
					for _, x := range acts {
						if x.Name == act.obj && x.Status.State == actionAborting {
							aborted.Store(true)
							cancel()
							return
						}
					}
				}
			}
		}
	}()
	exit, missing, runErr := a.cfg.Runner.Run(runCtx, spec)
	close(pollDone)
	logw.Flush()
	a.logf("action %s finished exit=%d", act.name, exit)
	if ctx.Err() != nil {
		return "", "", nil, ctx.Err()
	}

	// Results: what action-set stored, then the process's output as juju records it.
	act.mu.Lock()
	results := act.results
	message, failed := act.message, act.failed
	act.mu.Unlock()
	if results == nil {
		results = map[string]any{}
	}
	if !missing {
		recordOutput(results, exit, stdout.Bytes(), stderr.Bytes())
	}

	switch {
	case aborted.Load():
		return actionAborted, firstNonEmpty(message, "action aborted"), results, nil
	case timedOut.Load():
		return actionFailed, fmt.Sprintf("action timed out after %s", timeout), results, nil
	case missing:
		return actionFailed, fmt.Sprintf("action not implemented on unit %q", a.cfg.Unit), results, nil
	case runErr != nil:
		return actionFailed, runErr.Error(), results, nil
	case exit != 0 && !isExec:
		return actionFailed, fmt.Sprintf("exit status %d", exit), results, nil
	}

	// The process succeeded: commit what it changed, as for a hook.
	mutate, err := hc.flush(ctx)
	if err == nil {
		err = a.store.Update(ctx, mutate)
	}
	if err != nil {
		a.logf("action %s failed to commit: %v", act.name, err)
		if s, _ := results["stderr"].(string); s != "" {
			results["stderr"] = s + "\n" + err.Error()
		} else {
			results["stderr"] = err.Error()
		}
		if code, ok := results["return-code"]; !ok || code == 0 {
			results["return-code"] = 1
		}
		return actionFailed, firstNonEmpty(message, "committing requested changes failed"), results, nil
	}
	a.local.TrackedSecrets = trackedFromSpec(a.store.Spec())
	if failed {
		return actionFailed, message, results, nil
	}
	return actionCompleted, message, results, nil
}

func firstNonEmpty(s ...string) string {
	for _, x := range s {
		if x != "" {
			return x
		}
	}
	return ""
}

// recordOutput stores the process outcome in results as juju's updateActionResults does: return-code, and stdout and
// stderr when not empty, base64 encoded (with a -encoding key) when they are not UTF-8.
func recordOutput(results map[string]any, code int, stdout, stderr []byte) {
	results["return-code"] = code
	for _, o := range []struct {
		key string
		b   []byte
	}{{"stdout", stdout}, {"stderr", stderr}} {
		val, enc := string(o.b), "utf8"
		if !utf8.Valid(o.b) {
			val, enc = base64.StdEncoding.EncodeToString(o.b), "base64"
		}
		if val != "" {
			results[o.key] = val
		}
		if enc != "utf8" {
			results[o.key+"-encoding"] = enc
		}
	}
}

// limitedBuffer keeps the first maxActionOutput bytes written to it.
type limitedBuffer struct {
	buf       bytes.Buffer
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := maxActionOutput - b.buf.Len(); room < len(p) {
		if room > 0 {
			b.buf.Write(p[:room])
		}
		b.truncated = true
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) Bytes() []byte {
	if b.truncated {
		return append(append([]byte(nil), b.buf.Bytes()...), "\n[output truncated]\n"...)
	}
	return b.buf.Bytes()
}

// teeWriter writes to both writers (the second is the agent log).
type teeWriter struct {
	a, b interface{ Write([]byte) (int, error) }
}

func (t teeWriter) Write(p []byte) (int, error) {
	_, _ = t.b.Write(p)
	return t.a.Write(p)
}
