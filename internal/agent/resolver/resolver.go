// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/resolver.go,
// leadership/resolver.go and container/workload.go.

package resolver

import (
	"errors"
	"sort"
)

// Life is the lifecycle of the unit as the agent sees it.
type Life int

const (
	// Alive is a unit that is running (or restarting: it will come back).
	Alive Life = iota
	// Dying is a unit that is going away for good (scale-down or application removal): stop and remove run.
	Dying
	// Dead is a unit whose stop and remove hooks have run.
	Dead
)

// ResolvedMode is what a user asked of a unit in a hook error (the `resolved` command).
type ResolvedMode int

const (
	ResolvedNone ResolvedMode = iota
	ResolvedRetryHooks
	ResolvedNoHooks
)

// Pebble is the readiness of one workload container: BootID is empty while its Pebble does not answer.
type Pebble struct {
	Container string
	BootID    string
}

// Snapshot is the world as the agent sees it: juju's remotestate.Snapshot reduced to what jk has.
type Snapshot struct {
	// Unit is the unit's name (<app>/<n>).
	Unit string
	Life Life
	// AppDying is true while the unit's application is being removed.
	AppDying bool
	// Leader is true when the unit holds the Lease with at least 30s left.
	Leader bool
	// ConfigHash is the hash of the effective config.
	ConfigHash string
	// UpdateStatusVersion is bumped each time the update-status timer fires.
	UpdateStatusVersion int
	// RetryHookVersion is bumped each time the hook-retry timer fires.
	RetryHookVersion int
	Resolved         ResolvedMode
	Pebble           []Pebble

	// CharmURL and CharmRevision identify the charm the unit's pod runs ("" when unknown).
	CharmURL      string
	CharmRevision int
	// PendingActions are the Actions waiting for this unit, in the order to run them. OrphanedActions are Actions
	// recorded as running that this process is not running (it was restarted while they ran).
	PendingActions  []ActionRef
	OrphanedActions []ActionRef

	// Relations maps relation id to the relation's remote state.
	Relations map[int]RelationSnapshot
	// Storage maps storage id (name/index) to the storage's remote state.
	Storage map[string]StorageSnapshot
	// ConsumedSecretInfo maps the URI of each secret the unit tracks to its latest revision.
	ConsumedSecretInfo map[string]ConsumedSecret
	// ObsoleteSecretRevisions are owned secret revisions that nobody tracks any more, by URI.
	ObsoleteSecretRevisions map[string][]int
	// DeletedSecretRevisions are tracked secrets (nil list) or revisions that were removed.
	DeletedSecretRevisions map[string][]int
	// ExpiredSecretRevisions are owned revisions past their expiry, as "uri/revision".
	ExpiredSecretRevisions []string
}

// Local is the agent's own state: the persisted State plus the in-memory versions seen.
type Local struct {
	State
	UpdateStatusVersion int
	RetryHookVersion    int

	// Relations is the persisted per-relation state (UnitData.spec.relations).
	Relations map[int]RelationState
	// StorageAttached maps storage id to whether storage-attached has run (false after storage-detaching).
	StorageAttached map[string]bool
	// TrackedSecrets maps secret URI to the revision the unit tracks; ObsoleteSecretRevisions are the revisions
	// for which secret-remove has run.
	TrackedSecrets          map[string]int
	ObsoleteSecretRevisions map[string][]int

	// CharmURL and CharmRevision are the charm the unit last ran ("" for a unit that has not recorded one).
	CharmURL      string
	CharmRevision int
}

// OpKind is the kind of an operation.
type OpKind int

const (
	// OpRunHook runs Hook.
	OpRunHook OpKind = iota + 1
	// OpSkipHook commits Hook without running it (a finished hook, or one the user chose to skip).
	OpSkipHook
	// OpAcceptLeadership records leadership and queues leader-elected.
	OpAcceptLeadership
	// OpResignLeadership records that the unit is no longer the leader.
	OpResignLeadership
	// OpEnterScope makes the unit enter the scope of relation Relation: it becomes a member that other units
	// see, with its settings seeded with its addresses.
	OpEnterScope
	// OpSecretsRemoved prunes tracked secrets that were removed (DeletedSecrets, DeletedObsolete).
	OpSecretsRemoved
	// OpRecordCharm records the charm the unit runs (CharmURL, CharmRevision) without a hook.
	OpRecordCharm
	// OpRunAction runs Action.
	OpRunAction
	// OpFailAction finishes Action as failed with Message without running it.
	OpFailAction
)

// Operation is what the executor should do next.
type Operation struct {
	Kind OpKind
	Hook HookInfo
	// Relation is the relation of an OpEnterScope.
	Relation int
	// DeletedSecrets and DeletedObsolete are those of an OpSecretsRemoved.
	DeletedSecrets  map[string][]int
	DeletedObsolete map[string][]int
	// CharmURL and CharmRevision are those of an OpRecordCharm.
	CharmURL      string
	CharmRevision int
	// Action and Message are those of an OpRunAction and an OpFailAction.
	Action  ActionRef
	Message string
}

var (
	// ErrNoOperation means there is nothing to do until the world changes.
	ErrNoOperation = errors.New("no operations")
	// ErrUnitDead means the unit has stopped and been removed: the agent should exit.
	ErrUnitDead = errors.New("unit dead")
)

// Config holds the resolver's side effects, which the agent provides.
type Config struct {
	// ReportHookError sets the agent status to error ("hook failed: <name>").
	ReportHookError func(HookInfo) error
	// ClearResolved clears the user's resolved request.
	ClearResolved func() error
	// ShouldRetryHooks enables automatic retries (juju's automatically-retry-hooks, default true).
	ShouldRetryHooks    bool
	StartRetryHookTimer func()
	StopRetryHookTimer  func()
}

// Resolver is the port of juju's uniter resolver.
type Resolver struct {
	cfg                   Config
	retryHookTimerStarted bool
	// backoffActive is true from the first retry timer start until the backoff is reset. Unlike juju, which
	// forgets to reset the backoff when a retried hook then succeeds, it is kept across retries.
	backoffActive bool
}

// New returns a Resolver.
func New(cfg Config) *Resolver {
	noop := func() {}
	if cfg.StartRetryHookTimer == nil {
		cfg.StartRetryHookTimer = noop
	}
	if cfg.StopRetryHookTimer == nil {
		cfg.StopRetryHookTimer = noop
	}
	if cfg.ReportHookError == nil {
		cfg.ReportHookError = func(HookInfo) error { return nil }
	}
	if cfg.ClearResolved == nil {
		cfg.ClearResolved = func() error { return nil }
	}
	return &Resolver{cfg: cfg}
}

// SetShouldRetryHooks changes whether failed hooks are retried automatically (the model's automatically-retry-hooks).
func (r *Resolver) SetShouldRetryHooks(v bool) { r.cfg.ShouldRetryHooks = v }

// NextOp returns the next operation, ErrNoOperation or ErrUnitDead.
func (r *Resolver) NextOp(local Local, remote Snapshot) (Operation, error) {
	if remote.Life == Dead || local.Removed {
		return Operation{}, ErrUnitDead
	}

	if r.backoffActive && (local.Kind != RunHook || local.Step != Pending) {
		// The hook-retry backoff is running but no hook is pending: we are not in an error state,
		// so stop the timer to reset the backoff.
		r.cfg.StopRetryHookTimer()
		r.retryHookTimerStarted, r.backoffActive = false, false
	}

	// A pod that starts with a new charm upgrades before anything else runs.
	if op, err := charmOp(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}

	// The order is juju's: created relations, leadership, workloads, secrets, actions, storage.
	if op, err := createdRelationsOp(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}
	if op, err := r.leadership(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}
	if remote.Life == Alive {
		if op, err := pebbleReady(local, remote); !errors.Is(err, ErrNoOperation) {
			return op, err
		}
	}
	if op, err := secretsOp(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}
	if op, err := actionsOp(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}
	if op, err := storageOp(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}

	switch local.Kind {
	case RunHook:
		step := local.Step
		if local.HookStep != nil {
			step = *local.HookStep
		}
		switch step {
		case Pending:
			return r.nextOpHookError(local, remote)
		case Queued:
			if local.Hook.Kind == Install {
				// Install is handled in nextOp so that nothing happens while the unit is dying.
				return r.nextOp(local, remote)
			}
			return Operation{Kind: OpRunHook, Hook: *local.Hook}, nil
		case Done:
			return Operation{Kind: OpSkipHook, Hook: *local.Hook}, nil
		default:
			return Operation{}, errors.New("unknown hook operation step " + string(step))
		}
	case Continue:
		return r.nextOp(local, remote)
	default:
		return Operation{}, errors.New("unknown operation kind " + string(local.Kind))
	}
}

// leadership is juju's leadership resolver.
func (r *Resolver) leadership(local Local, remote Snapshot) (Operation, error) {
	if !local.Installed {
		return Operation{}, ErrNoOperation
	}
	canAccept := !local.Leader
	if remote.Life != Alive || local.Kind != Continue {
		canAccept = false
	}
	switch {
	case remote.Leader && canAccept:
		return Operation{Kind: OpAcceptLeadership}, nil
	case local.Leader && (!remote.Leader || remote.Life != Alive):
		return Operation{Kind: OpResignLeadership}, nil
	}
	return Operation{}, ErrNoOperation
}

// pebbleReady is juju's workload hook resolver for ready events: a container whose Pebble answers with a
// boot ID we have not acknowledged gets a pebble-ready hook. It only acts between operations.
func pebbleReady(local Local, remote Snapshot) (Operation, error) {
	if local.Kind != Continue {
		return Operation{}, ErrNoOperation
	}
	ready := append([]Pebble(nil), remote.Pebble...)
	sort.SliceStable(ready, func(i, j int) bool { return ready[i].Container < ready[j].Container })
	for _, p := range ready {
		if p.BootID != "" && local.PebbleBoot[p.Container] != p.BootID {
			return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: PebbleReady, WorkloadName: p.Container, BootID: p.BootID}}, nil
		}
	}
	return Operation{}, ErrNoOperation
}

func (r *Resolver) nextOpHookError(local Local, remote Snapshot) (Operation, error) {
	if err := r.cfg.ReportHookError(*local.Hook); err != nil {
		return Operation{}, err
	}
	switch remote.Resolved {
	case ResolvedNone:
		if remote.RetryHookVersion > local.RetryHookVersion {
			// Asked to retry: clear the started flag so the timer restarts if this fails again.
			r.retryHookTimerStarted = false
			return Operation{Kind: OpRunHook, Hook: *local.Hook}, nil
		}
		if !r.retryHookTimerStarted && r.cfg.ShouldRetryHooks {
			r.cfg.StartRetryHookTimer()
			r.retryHookTimerStarted, r.backoffActive = true, true
		}
		return Operation{}, ErrNoOperation
	case ResolvedRetryHooks:
		r.cfg.StopRetryHookTimer()
		r.retryHookTimerStarted, r.backoffActive = false, false
		if err := r.cfg.ClearResolved(); err != nil {
			return Operation{}, err
		}
		return Operation{Kind: OpRunHook, Hook: *local.Hook}, nil
	case ResolvedNoHooks:
		r.cfg.StopRetryHookTimer()
		r.retryHookTimerStarted, r.backoffActive = false, false
		if err := r.cfg.ClearResolved(); err != nil {
			return Operation{}, err
		}
		return Operation{Kind: OpSkipHook, Hook: *local.Hook}, nil
	}
	return Operation{}, errors.New("unknown resolved mode")
}

func (r *Resolver) nextOp(local Local, remote Snapshot) (Operation, error) {
	switch remote.Life {
	case Dying:
		// Normally relations are handled last, but a dying unit must depart its relations before it stops.
		if op, err := relationsOp(local, remote); !errors.Is(err, ErrNoOperation) {
			return op, err
		}
		if local.Started && !local.Stopped {
			return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: Stop}}, nil
		} else if local.Installed && !local.Removed {
			return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: Remove}}, nil
		}
		return Operation{}, ErrUnitDead
	case Dead:
		return Operation{}, ErrUnitDead
	}

	if !local.Installed && !local.Started {
		return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: Install}}, nil
	}
	if local.ConfigHash != remote.ConfigHash {
		return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: ConfigChanged}}, nil
	}
	if op, err := relationsOp(local, remote); !errors.Is(err, ErrNoOperation) {
		return op, err
	}
	if local.UpdateStatusVersion != remote.UpdateStatusVersion {
		return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: UpdateStatus}}, nil
	}
	return Operation{}, ErrNoOperation
}
