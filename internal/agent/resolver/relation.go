// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/relation/resolver.go,
// relation/state.go and relation/statetracker.go (SynchronizeScopes). The hook order and the bookkeeping
// ("units joined, settings versions seen") are juju's; scope entry is an operation here because the resolver is pure.

package resolver

import (
	"fmt"
	"sort"
	"strings"
)

// RelationSnapshot is the remote state of one relation as the unit sees it (juju's remotestate.RelationSnapshot
// plus what juju's state tracker knows about the endpoint).
type RelationSnapshot struct {
	// Life is Dying while the relation is being removed.
	Life      Life
	Suspended bool
	// Peer is true for a peer relation (one endpoint): relation-broken never runs for it.
	Peer bool
	// Endpoint is the local endpoint's name and RemoteApp the application on the other side (the unit's own
	// application for a peer relation).
	Endpoint  string
	RemoteApp string
	// Members maps each remote unit in scope to the version of its settings.
	Members map[string]int64
	// AppVersion is the version of the remote application's settings (0 when it has set none).
	AppVersion int64
}

// RelationState is what the unit has done in one relation: juju's relation.State.
type RelationState struct {
	// InScope is true once the unit has entered the relation's scope (the relation is "known").
	InScope bool
	// Created is true once relation-created has run.
	Created bool
	// Members maps each remote unit joined so far to the settings version last delivered in a hook.
	Members map[string]int64
	// AppVersion is the application settings version last delivered.
	AppVersion int64
	// ChangedPending is the unit whose relation-changed must run before any other relation hook.
	ChangedPending string
	// Endpoint and RemoteApp are the local endpoint and the application on the other side, remembered so that the
	// unit can still depart from a relation whose object is gone (after an agent restart).
	Endpoint  string
	RemoteApp string
}

func (s RelationState) clone() RelationState {
	m := make(map[string]int64, len(s.Members))
	for k, v := range s.Members {
		m[k] = v
	}
	s.Members = m
	return s
}

// Validate checks that h is a valid change to the relation state (juju's State.Validate).
func (s RelationState) Validate(h HookInfo) error {
	if h.Kind == RelationBroken {
		if len(s.Members) == 0 {
			return nil
		}
		return fmt.Errorf("inappropriate %q for %q: cannot run \"relation-broken\" while units still present", h.Kind, h.RemoteUnit)
	}
	if s.ChangedPending != "" {
		if h.RemoteUnit != s.ChangedPending || h.Kind != RelationChanged {
			return fmt.Errorf("inappropriate %q for %q: expected \"relation-changed\" for %q", h.Kind, h.RemoteUnit, s.ChangedPending)
		}
		return nil
	}
	if h.RemoteUnit != "" {
		_, joined := s.Members[h.RemoteUnit]
		if joined && h.Kind == RelationJoined {
			return fmt.Errorf("inappropriate %q for %q: unit already joined", h.Kind, h.RemoteUnit)
		} else if !joined && h.Kind != RelationJoined && h.Kind != RelationCreated {
			return fmt.Errorf("inappropriate %q for %q: unit has not joined", h.Kind, h.RemoteUnit)
		}
	}
	return nil
}

// ApplyHook returns the state after a successful hook (juju's UpdateStateForHook); successive applications of the
// same hook are idempotent.
func (s RelationState) ApplyHook(h HookInfo) RelationState {
	s = s.clone()
	switch h.Kind {
	case RelationCreated:
		s.Created = true
		return s
	case RelationBroken:
		return s
	case RelationDeparted:
		if h.RemoteUnit == "" {
			s.AppVersion = 0
		} else {
			delete(s.Members, h.RemoteUnit)
		}
		return s
	}
	if h.RemoteUnit == "" {
		s.AppVersion = h.ChangeVersion
	} else {
		s.Members[h.RemoteUnit] = h.ChangeVersion
	}
	if h.Kind == RelationJoined {
		s.ChangedPending = h.RemoteUnit
	} else {
		s.ChangedPending = ""
	}
	return s
}

// UnitApp is the application of a unit name ("app/3" -> "app").
func UnitApp(unit string) string {
	app, _, _ := strings.Cut(unit, "/")
	return app
}

func sortedIDs[V any](m map[int]V) []int {
	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// synchronizeScopes is the part of juju's SynchronizeScopes that matters here: a unit enters the scope of every live,
// unsuspended relation it has not entered yet.
func synchronizeScopes(local Local, remote Snapshot) (Operation, bool) {
	if remote.Life != Alive {
		return Operation{}, false
	}
	for _, id := range sortedIDs(remote.Relations) {
		rs := remote.Relations[id]
		if rs.Life != Alive || rs.Suspended || local.Relations[id].InScope {
			continue
		}
		return Operation{Kind: OpEnterScope, Relation: id}, true
	}
	return Operation{}, false
}

// createdRelationsOp is juju's createdRelationsResolver: relation-created, once per relation, before anything else
// but after install.
func createdRelationsOp(local Local, remote Snapshot) (Operation, error) {
	if !local.Installed || remote.Life == Dying || local.Kind != Continue {
		return Operation{}, ErrNoOperation
	}
	if op, ok := synchronizeScopes(local, remote); ok {
		return op, nil
	}
	for _, id := range sortedIDs(remote.Relations) {
		rs := remote.Relations[id]
		if rs.Life != Alive {
			continue
		}
		st, known := local.Relations[id]
		if !known || !st.InScope || st.Created {
			continue
		}
		return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: RelationCreated, RelationID: id, Endpoint: rs.Endpoint, RemoteApp: rs.RemoteApp}}, nil
	}
	return Operation{}, ErrNoOperation
}

// relationsOp is juju's relationsResolver: joined, changed, departed and broken hooks. Peer relations come last, so
// that on application removal the hooks of other relations can rely on the peers still being there.
func relationsOp(local Local, remote Snapshot) (Operation, error) {
	if local.Kind != Continue {
		return Operation{}, ErrNoOperation
	}
	if op, ok := synchronizeScopes(local, remote); ok {
		return op, nil
	}
	var peers []int
	for _, id := range sortedIDs(remote.Relations) {
		if remote.Relations[id].Peer {
			peers = append(peers, id)
			continue
		}
		if op, err := processRelation(id, local, remote); err == nil || err != ErrNoOperation {
			return op, err
		}
	}
	for _, id := range peers {
		if op, err := processRelation(id, local, remote); err == nil || err != ErrNoOperation {
			return op, err
		}
	}
	return Operation{}, ErrNoOperation
}

func processRelation(id int, local Local, remote Snapshot) (Operation, error) {
	rs := remote.Relations[id]
	st, known := local.Relations[id]
	if !known || !st.InScope {
		return Operation{}, ErrNoOperation
	}
	// If either the unit or the relation are dying, or the relation is suspended, the relation is broken: every
	// remote member departs.
	remoteBroken := false
	if remote.Life == Dying || rs.Life == Dying || rs.Suspended {
		rs.Members, rs.AppVersion = nil, 0
		remoteBroken = true
	}
	h, err := nextHookForRelation(id, st, rs, remoteBroken, localDeparting(remote), remote.Unit)
	if err != nil {
		return Operation{}, err
	}
	return Operation{Kind: OpRunHook, Hook: h}, nil
}

func localDeparting(remote Snapshot) bool { return remote.Life != Alive || remote.AppDying }

// nextHookForRelation is juju's relationsResolver.nextHookForRelation.
func nextHookForRelation(id int, st RelationState, rs RelationSnapshot, remoteBroken, selfDeparting bool, self string) (HookInfo, error) {
	base := HookInfo{RelationID: id, Endpoint: rs.Endpoint}
	// A relation-changed queued by relation-joined comes first.
	if st.ChangedPending != "" {
		u := st.ChangedPending
		h := base
		h.Kind, h.RemoteUnit, h.RemoteApp, h.ChangeVersion = RelationChanged, u, UnitApp(u), rs.Members[u]
		return h, nil
	}

	units := map[string]struct{}{}
	for u := range st.Members {
		units[u] = struct{}{}
	}
	for u := range rs.Members {
		units[u] = struct{}{}
	}
	sorted := make([]string, 0, len(units))
	for u := range units {
		sorted = append(sorted, u)
	}
	sort.Strings(sorted)

	// Locally known units that are gone from the remote state depart.
	for _, u := range sorted {
		version, joined := st.Members[u]
		if !joined {
			continue
		}
		if _, ok := rs.Members[u]; ok {
			continue
		}
		departee := u
		if selfDeparting {
			departee = self
		}
		h := base
		h.Kind, h.RemoteUnit, h.RemoteApp, h.ChangeVersion, h.DepartingUnit = RelationDeparted, u, UnitApp(u), version, departee
		return h, nil
	}

	// A relation that is meant to be broken is broken. Peer relations are never broken.
	if remoteBroken && !rs.Peer {
		h := base
		h.Kind, h.RemoteApp = RelationBroken, rs.RemoteApp
		return h, nil
	}

	// Application settings that changed. As in juju, a never-set (version 0) application does not trigger a hook.
	if !remoteBroken && rs.RemoteApp != "" && st.AppVersion != rs.AppVersion {
		h := base
		h.Kind, h.RemoteApp, h.ChangeVersion = RelationChanged, rs.RemoteApp, rs.AppVersion
		return h, nil
	}

	// Remote units not locally known join.
	for _, u := range sorted {
		version, ok := rs.Members[u]
		if !ok {
			continue
		}
		if _, joined := st.Members[u]; !joined {
			h := base
			h.Kind, h.RemoteUnit, h.RemoteApp, h.ChangeVersion = RelationJoined, u, UnitApp(u), version
			return h, nil
		}
	}

	// Remote units whose settings version is not the one delivered: != and not >, as in juju.
	for _, u := range sorted {
		rv, ok := rs.Members[u]
		if !ok {
			continue
		}
		lv, joined := st.Members[u]
		if !joined || rv == lv {
			continue
		}
		h := base
		h.Kind, h.RemoteUnit, h.RemoteApp, h.ChangeVersion = RelationChanged, u, UnitApp(u), rv
		return h, nil
	}
	return HookInfo{}, ErrNoOperation
}
