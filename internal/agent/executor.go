// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/operation/runhook.go
// (runHook, beforeHook and afterHook).

package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
	"github.com/luci1900/jk/internal/hooktools"
)

// execute performs one operation.
func (a *Agent) execute(ctx context.Context, op resolver.Operation, snap resolver.Snapshot, w *world) error {
	switch op.Kind {
	case resolver.OpAcceptLeadership:
		a.logf("accepting leadership")
		return a.setState(ctx, a.local.State.LeaderElected())
	case resolver.OpResignLeadership:
		a.logf("resigning leadership")
		return a.setState(ctx, a.local.State.LeaderDeposed())
	case resolver.OpEnterScope:
		return a.enterScope(ctx, op.Relation, snap.Relations[op.Relation])
	case resolver.OpRecordCharm:
		a.logf("recording charm %s", op.CharmURL)
		return a.setLocal(ctx, a.local.CharmRecorded(op.CharmURL, op.CharmRevision))
	case resolver.OpRunAction:
		return a.runAction(ctx, op.Action, snap, w)
	case resolver.OpFailAction:
		return a.failAction(ctx, op.Action, op.Message)
	case resolver.OpSecretsRemoved:
		return a.setLocal(ctx, a.local.SecretsRemoved(op.DeletedSecrets, op.DeletedObsolete))
	case resolver.OpSkipHook:
		return a.commit(ctx, op.Hook, snap)
	case resolver.OpRunHook:
		return a.runHook(ctx, op.Hook, snap, w)
	}
	return fmt.Errorf("unknown operation %d", op.Kind)
}

func (a *Agent) setState(ctx context.Context, st resolver.State, more ...func(*v1alpha1.UnitDataSpec)) error {
	if err := a.store.SetState(ctx, st, more...); err != nil {
		return err
	}
	a.local.State = st
	return nil
}

// setLocal persists the resolver's local state (operation state, relation, storage and secret bookkeeping) in one write.
func (a *Agent) setLocal(ctx context.Context, l resolver.Local, more ...func(*v1alpha1.UnitDataSpec)) error {
	if err := a.store.SetState(ctx, l.State, append([]func(*v1alpha1.UnitDataSpec){func(sp *v1alpha1.UnitDataSpec) { applyLocal(sp, l) }}, more...)...); err != nil {
		return err
	}
	l.UpdateStatusVersion, l.RetryHookVersion = a.local.UpdateStatusVersion, a.local.RetryHookVersion
	a.local = l
	return nil
}

// enterScope makes the unit a member of a relation: other units see it, and its settings are seeded with its addresses
// (juju's EnterScope), before relation-created runs.
func (a *Agent) enterScope(ctx context.Context, id int, rs resolver.RelationSnapshot) error {
	ip := a.cfg.PodIP()
	l := a.local.EnteredScope(id, rs.Endpoint, rs.RemoteApp)
	return a.setLocal(ctx, l, func(sp *v1alpha1.UnitDataSpec) {
		st := sp.Relations[relKey(id)]
		st.Data, _ = seedAddresses(st.Data, ip)
		sp.Relations[relKey(id)] = st
	})
}

// commit records a finished (or skipped) hook: the state moves on, the agent goes idle and the error is cleared.
func (a *Agent) commit(ctx context.Context, info resolver.HookInfo, snap resolver.Snapshot) error {
	var next resolver.Local
	if _, known := a.local.Relations[info.RelationID]; info.IsRelation() && !known {
		// The relation is gone (the hook was queued before a restart): nothing to record but the end of the hook.
		next = a.local
		next.State = a.local.State.Committed(info, snap.ConfigHash)
	} else {
		next = a.local.Committed(info, snap.ConfigHash)
	}
	if err := a.setLocal(ctx, next, a.store.withAgentStatus(AgentIdle, ""), func(sp *v1alpha1.UnitDataSpec) { sp.ErrorState = nil }); err != nil {
		return err
	}
	if info.Kind == resolver.SecretExpired {
		a.expiredDone[resolver.SecretRevisionSpec(info.SecretURI, info.SecretRevision)] = true
	}
	// Any hook satisfies a pending update-status tick, so update-status only fires after the next timer.
	a.local.UpdateStatusVersion = snap.UpdateStatusVersion
	return nil
}

// runHook is juju's runHook operation: prepare (record the hook as pending), execute, flush the buffered writes with
// the "done" marker, commit.
func (a *Agent) runHook(ctx context.Context, info resolver.HookInfo, snap resolver.Snapshot, w *world) error {
	name := info.Name()

	// leader-elected is skipped if leadership was lost between queueing and running.
	if info.Kind == resolver.LeaderElected && !a.lead.IsLeader() {
		a.logf("unit is no longer the leader; skipping %s", name)
		return a.commit(ctx, info, snap)
	}

	if info.IsRelation() {
		st, known := a.local.Relations[info.RelationID]
		if !known {
			a.logf("relation %d is gone; skipping %s", info.RelationID, name)
			return a.commit(ctx, info, snap)
		}
		if err := st.Validate(info); err != nil {
			return fmt.Errorf("hook %s: %w", name, err)
		}
	}

	// Prepare.
	a.local.RetryHookVersion = snap.RetryHookVersion
	var pre []func(*v1alpha1.UnitDataSpec)
	if info.Kind != resolver.UpdateStatus {
		pre = append(pre, a.store.withAgentStatus(AgentExecuting, runningMessage(name)))
	}
	if msg, ok := a.preHookStatus(info); ok {
		pre = append(pre, func(sp *v1alpha1.UnitDataSpec) { sp.WorkloadStatus = a.store.status("maintenance", msg) })
	}
	// A rerun of the same hook (after a failure or a crash) keeps its execution id, so that its writes are idempotent.
	execID, ok := a.local.State.RetryExecID(info)
	if !ok {
		execID = newExecID()
	}
	if err := a.setState(ctx, a.local.State.Prepared(info, execID), pre...); err != nil {
		return err
	}

	// Execute.
	cfg, _ := a.effective(w)
	hc := newHookContext(ctx, a, cfg, info, execID, w)
	exit, runErr := a.execHook(ctx, info, hc, w)
	if ctx.Err() != nil {
		// Shutting down: the hook was killed, so run it again next time (queued, not an error).
		a.logf("hook %s was terminated", name)
		bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = a.setState(bg, a.local.State.Requeued(info))
		return ctx.Err()
	}
	if runErr != nil || exit != 0 {
		// The state stays pending: the resolver reports the error and retries with backoff.
		a.logf("hook %s failed (exit %d, %v)", name, exit, runErr)
		return a.reportHookError(info)
	}

	// Flush (secrets -> AppData -> UnitData), together with the done marker.
	mutate, err := hc.flush(ctx)
	if err != nil {
		a.logf("hook %s failed to commit: %v", name, err)
		return a.reportHookError(info)
	}
	post := a.afterHook(info, hc.statusSet || a.local.StatusSet)
	if err := a.setState(ctx, a.local.State.Executed(info, hc.statusSet), mutate, post); err != nil {
		return err
	}
	// The hook may have changed the consumer records.
	a.local.TrackedSecrets = trackedFromSpec(a.store.Spec())
	return a.commit(ctx, info, snap)
}

func newExecID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func runningMessage(name string) string { return "running " + name + " hook" }

// preHookStatus is juju's beforeHook: the workload status shown while install, stop and remove run.
func (a *Agent) preHookStatus(info resolver.HookInfo) (string, bool) {
	switch info.Kind {
	case resolver.Install:
		// Not if the charm already set a status in an earlier (failed) run.
		return installingMessage, !a.local.StatusSet
	case resolver.Stop:
		return "stopping charm software", true
	case resolver.Remove:
		return "cleaning up prior to charm deletion", true
	}
	return "", false
}

// afterHook is juju's afterHook: statuses the agent sets once a hook has run.
func (a *Agent) afterHook(info resolver.HookInfo, statusSet bool) func(*v1alpha1.UnitDataSpec) {
	switch info.Kind {
	case resolver.Stop:
		return func(sp *v1alpha1.UnitDataSpec) { sp.WorkloadStatus = a.store.status("maintenance", "") }
	case resolver.Remove:
		return func(sp *v1alpha1.UnitDataSpec) { sp.WorkloadStatus = a.store.status("terminated", "") }
	case resolver.Start:
		if !statusSet {
			return func(sp *v1alpha1.UnitDataSpec) { sp.WorkloadStatus = a.store.status("unknown", "") }
		}
	}
	return func(*v1alpha1.UnitDataSpec) {}
}

// execHook serves the hook tools and runs dispatch. It always logs the "finished" line.
func (a *Agent) execHook(ctx context.Context, info resolver.HookInfo, hc *hookContext, w *world) (exit int, err error) {
	name := info.Name()
	a.hookSeq++
	ctxID := newContextID(a.cfg.Unit, name, a.hookSeq)
	socket := filepath.Join(a.cfg.AgentDir(), "agent.socket")

	handler := &hooktools.Real{B: hc, Now: a.clk.Now}
	srv, err := hooktools.Listen("unix", socket, ctxID, handler)
	if err != nil {
		a.logf("hook %s finished exit=-1", name)
		return -1, err
	}
	defer srv.Close()

	out := &lineWriter{out: a.cfg.Log, prefix: fmt.Sprintf("jk-agent: hook %s: ", name)}
	exit, missing, err := a.cfg.Runner.Run(ctx, HookSpec{
		Name:   name,
		Dir:    a.cfg.CharmDir(),
		Path:   filepath.Join(a.cfg.CharmDir(), "dispatch"),
		Env:    a.hookEnv(a.withSecretLabel(info, w), nil, ctxID, socket, w.model),
		Output: out,
	})
	out.Flush()
	if missing {
		a.logf("hook %s skipped: no dispatch script", name)
	}
	a.logf("hook %s finished exit=%d", name, exit)
	return exit, err
}

// withSecretLabel fills in the owner's label for the secret hooks that carry none (secret-remove, secret-expired).
func (a *Agent) withSecretLabel(info resolver.HookInfo, w *world) resolver.HookInfo {
	if info.IsSecret() && info.SecretLabel == "" {
		if e, ok := a.findOwned(w, strings.TrimPrefix(info.SecretURI, "secret:")); ok {
			info.SecretLabel = e.Label
		}
	}
	return info
}
