// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/actions/resolver.go and the
// charm upgrade checks of internal/worker/uniter/resolver.go (charmModified).

package resolver

// ActionRef identifies an Action: the object name and the task id charms see.
type ActionRef struct {
	Name string
	ID   int64
}

const (
	// ActionTerminatedMessage finishes an action that was running when the agent stopped (juju never re-runs one).
	ActionTerminatedMessage = "action terminated"
	// ActionDyingMessage finishes an action that cannot run because the unit is going away.
	ActionDyingMessage = "unit is dying"
)

// actionsOp is juju's actions resolver: between operations (and while a hook is failing) the next pending action
// runs. Actions found running that nobody is running fail, and a dying unit fails them all.
func actionsOp(local Local, remote Snapshot) (Operation, error) {
	if len(remote.OrphanedActions) > 0 {
		return Operation{Kind: OpFailAction, Action: remote.OrphanedActions[0], Message: ActionTerminatedMessage}, nil
	}
	if len(remote.PendingActions) == 0 {
		return Operation{}, ErrNoOperation
	}
	next := remote.PendingActions[0]
	if remote.Life != Alive || remote.AppDying {
		return Operation{Kind: OpFailAction, Action: next, Message: ActionDyingMessage}, nil
	}
	step := local.Step
	if local.HookStep != nil {
		step = *local.HookStep
	}
	switch {
	case local.Kind == Continue,
		// Actions can run while a hook is in error.
		local.Kind == RunHook && step == Pending:
		return Operation{Kind: OpRunAction, Action: next}, nil
	}
	return Operation{}, ErrNoOperation
}

// charmOp upgrades the charm: a unit that has installed and runs a different charm than it last recorded runs
// upgrade-charm (then config-changed). A unit that recorded none (first install, or one from before charms were
// recorded) just records it.
func charmOp(local Local, remote Snapshot) (Operation, error) {
	if remote.CharmURL == "" || !local.Installed || local.Kind != Continue || remote.Life != Alive {
		return Operation{}, ErrNoOperation
	}
	switch {
	case local.CharmURL == "":
		return Operation{Kind: OpRecordCharm, CharmURL: remote.CharmURL, CharmRevision: remote.CharmRevision}, nil
	case local.CharmURL != remote.CharmURL:
		return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: UpgradeCharm, CharmURL: remote.CharmURL, CharmRevision: remote.CharmRevision}}, nil
	}
	return Operation{}, ErrNoOperation
}
