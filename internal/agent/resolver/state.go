// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/operation/state.go,
// operation/runhook.go and operation/leader.go. Reduced to what jk needs (no upgrades, actions, relations,
// storage or secrets yet); the state machine and its transitions are juju's.

// Package resolver decides which operation a unit agent runs next. It is a port of juju's uniter resolver
// (resolver.go, leadership and container resolvers, relation, storage and secrets resolvers) with the state machine
// of juju's operation package. It is pure: it reads a local state and a snapshot of the world and returns an
// operation, so every hook sequence can be table tested.
package resolver

import (
	"fmt"
	"strings"
)

// Hook kinds the agent runs. Workload hooks are named "<container>-pebble-ready", relation hooks
// "<endpoint>-relation-<change>" and storage hooks "<storage>-storage-<change>".
const (
	Install       = "install"
	LeaderElected = "leader-elected"
	ConfigChanged = "config-changed"
	Start         = "start"
	Stop          = "stop"
	Remove        = "remove"
	UpdateStatus  = "update-status"
	PebbleReady   = "pebble-ready"
	UpgradeCharm  = "upgrade-charm"

	RelationCreated  = "relation-created"
	RelationJoined   = "relation-joined"
	RelationChanged  = "relation-changed"
	RelationDeparted = "relation-departed"
	RelationBroken   = "relation-broken"

	StorageAttached  = "storage-attached"
	StorageDetaching = "storage-detaching"

	SecretChanged = "secret-changed"
	SecretRemove  = "secret-remove"
	SecretExpired = "secret-expired"
)

// Kind is the kind of the operation recorded in the state.
type Kind string

const (
	// RunHook means a hook is queued, running (Pending) or finished but uncommitted (Done).
	RunHook Kind = "run-hook"
	// Continue means no operation is in progress.
	Continue Kind = "continue"
)

// Step is the progress of an operation.
type Step string

const (
	Queued  Step = "queued"
	Pending Step = "pending"
	Done    Step = "done"
)

// HookInfo identifies a hook to run (juju's hook.Info).
type HookInfo struct {
	Kind string `json:"kind"`
	// WorkloadName is the container for pebble-ready.
	WorkloadName string `json:"workload,omitempty"`
	// BootID is the Pebble boot ID that a pebble-ready hook acknowledges.
	BootID string `json:"bootID,omitempty"`

	// RelationID, Endpoint (the local endpoint's name), RemoteUnit (empty for application hooks), RemoteApp,
	// DepartingUnit and ChangeVersion describe a relation hook.
	RelationID    int    `json:"relationID,omitempty"`
	Endpoint      string `json:"endpoint,omitempty"`
	RemoteUnit    string `json:"remoteUnit,omitempty"`
	RemoteApp     string `json:"remoteApp,omitempty"`
	DepartingUnit string `json:"departingUnit,omitempty"`
	ChangeVersion int64  `json:"changeVersion,omitempty"`

	// CharmURL and CharmRevision are the charm an upgrade-charm hook upgrades to (recorded when it commits).
	CharmURL      string `json:"charmURL,omitempty"`
	CharmRevision int    `json:"charmRevision,omitempty"`

	// StorageID is name/index for a storage hook.
	StorageID string `json:"storage,omitempty"`

	// SecretURI (secret:<xid>), SecretRevision and SecretLabel describe a secret hook.
	SecretURI      string `json:"secretURI,omitempty"`
	SecretRevision int    `json:"secretRevision,omitempty"`
	SecretLabel    string `json:"secretLabel,omitempty"`
}

// IsRelation reports whether the hook is a relation hook.
func (h HookInfo) IsRelation() bool {
	switch h.Kind {
	case RelationCreated, RelationJoined, RelationChanged, RelationDeparted, RelationBroken:
		return true
	}
	return false
}

// IsStorage reports whether the hook is a storage hook.
func (h HookInfo) IsStorage() bool { return h.Kind == StorageAttached || h.Kind == StorageDetaching }

// IsSecret reports whether the hook is a secret hook.
func (h HookInfo) IsSecret() bool {
	return h.Kind == SecretChanged || h.Kind == SecretRemove || h.Kind == SecretExpired
}

// StorageName is the storage's name (the part of its id before the slash).
func StorageName(id string) string {
	n, _, _ := strings.Cut(id, "/")
	return n
}

// Name is the hook's name as the charm sees it (JUJU_HOOK_NAME and the dispatch path).
func (h HookInfo) Name() string {
	switch {
	case h.Kind == PebbleReady:
		return h.WorkloadName + "-" + PebbleReady
	case h.IsRelation():
		return h.Endpoint + "-" + h.Kind
	case h.IsStorage():
		return StorageName(h.StorageID) + "-" + h.Kind
	}
	return h.Kind
}

// State is the persisted state of the unit agent: juju's operation.State minus what jk does not have.
// It is stored as JSON in UnitData.spec.operation.
type State struct {
	Leader    bool `json:"leader,omitempty"`
	Started   bool `json:"started,omitempty"`
	Stopped   bool `json:"stopped,omitempty"`
	Installed bool `json:"installed,omitempty"`
	Removed   bool `json:"removed,omitempty"`
	// StatusSet records that a hook has set the workload status (so install does not overwrite it).
	StatusSet bool `json:"statusSet,omitempty"`

	Kind     Kind      `json:"op"`
	Step     Step      `json:"opStep"`
	Hook     *HookInfo `json:"hook,omitempty"`
	HookStep *Step     `json:"hookStep,omitempty"`

	// ConfigHash is the hash of the effective config at the last config-changed hook.
	ConfigHash string `json:"configHash,omitempty"`
	// PebbleBoot maps container to the Pebble boot ID whose pebble-ready hook last ran.
	PebbleBoot map[string]string `json:"pebbleBoot,omitempty"`

	// ExecID identifies the execution of the pending hook. It survives retries of the same hook (and a crash
	// between the hook's secret writes and its commit), so writes derived from it are idempotent.
	ExecID string `json:"execID,omitempty"`
}

// Initial is the state of a unit that has never run: the install hook is queued.
func Initial() State {
	return State{Kind: RunHook, Step: Queued, Hook: &HookInfo{Kind: Install}}
}

// Validate checks the state is consistent.
func (s State) Validate() error {
	switch s.Kind {
	case RunHook:
		if s.Hook == nil {
			return fmt.Errorf("invalid operation state: missing hook with kind %q", s.Kind)
		}
	case Continue:
		if s.Hook != nil {
			return fmt.Errorf("invalid operation state: unexpected hook with kind %q", s.Kind)
		}
	default:
		return fmt.Errorf("invalid operation state: unknown operation %q", s.Kind)
	}
	switch s.Step {
	case Queued, Pending, Done:
	default:
		return fmt.Errorf("invalid operation state: unknown step %q", s.Step)
	}
	return nil
}

func (s State) clone() State {
	if s.Hook != nil {
		h := *s.Hook
		s.Hook = &h
	}
	if s.HookStep != nil {
		st := *s.HookStep
		s.HookStep = &st
	}
	if s.PebbleBoot != nil {
		m := make(map[string]string, len(s.PebbleBoot))
		for k, v := range s.PebbleBoot {
			m[k] = v
		}
		s.PebbleBoot = m
	}
	return s
}

func (s State) with(kind Kind, step Step, hook *HookInfo, hookStep *Step) State {
	s = s.clone()
	s.Kind, s.Step, s.Hook, s.HookStep = kind, step, hook, hookStep
	return s
}

// RetryExecID returns the execution id to reuse when h is run again after a failure or a crash: the id of the
// pending or finished execution of the same hook.
func (s State) RetryExecID(h HookInfo) (string, bool) {
	if s.Kind == RunHook && s.Hook != nil && *s.Hook == h && s.ExecID != "" {
		return s.ExecID, true
	}
	return "", false
}

// Prepared is the state recorded just before a hook runs; execID identifies this execution.
func (s State) Prepared(h HookInfo, execID string) State {
	n := s.with(RunHook, Pending, &h, nil)
	n.ExecID = execID
	return n
}

// Executed is the state recorded when a hook ran to completion, before it is committed.
// statusSet reports whether the hook (or an earlier one) set the workload status.
func (s State) Executed(h HookInfo, statusSet bool) State {
	done := Done
	s = s.with(RunHook, Done, &h, &done)
	s.StatusSet = s.StatusSet || statusSet
	return s
}

// Requeued is the state recorded when a hook was killed by the agent shutting down: it runs again.
func (s State) Requeued(h HookInfo) State {
	q := Queued
	return s.with(RunHook, Queued, &h, &q)
}

// Committed is the state after a hook ran (or was skipped): juju's runHook.Commit. Config-changed after
// a fresh install queues start; the hook's effect on the installed/started flags is recorded.
// configHash is the hash that a config-changed hook acknowledges.
func (s State) Committed(h HookInfo, configHash string) State {
	n := s.with(Continue, Pending, nil, nil)
	n.ExecID = ""
	if h.Kind == UpgradeCharm {
		// As in juju, config-changed always follows upgrade-charm (and start follows it on a unit that has not started).
		n = s.with(RunHook, Queued, &HookInfo{Kind: ConfigChanged}, nil)
		n.ExecID = ""
	}
	if h.Kind == ConfigChanged {
		n.ConfigHash = configHash
		if !s.Started {
			n = s.with(RunHook, Queued, &HookInfo{Kind: Start}, nil)
			n.ConfigHash = configHash
			n.ExecID = ""
		}
	}
	switch h.Kind {
	case Install:
		n.Installed, n.Removed = true, false
	case Start:
		n.Started, n.Stopped = true, false
	case Stop:
		n.Stopped = true
	case Remove:
		n.Removed = true
	case PebbleReady:
		m := make(map[string]string, len(n.PebbleBoot)+1)
		for k, v := range n.PebbleBoot {
			m[k] = v
		}
		m[h.WorkloadName] = h.BootID
		n.PebbleBoot = m
	}
	return n
}

// LeaderElected is the state after accepting leadership: leader-elected is queued (juju's acceptLeadership.Commit).
func (s State) LeaderElected() State {
	n := s.with(RunHook, Queued, &HookInfo{Kind: LeaderElected}, nil)
	n.Leader = true
	return n
}

// LeaderDeposed is the state after resigning leadership.
func (s State) LeaderDeposed() State {
	n := s.clone()
	n.Leader = false
	return n
}
