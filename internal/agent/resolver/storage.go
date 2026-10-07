// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/storage/resolver.go and
// storage/state.go, for CAAS models.

package resolver

import "sort"

// StorageSnapshot is the remote state of one storage instance of the unit.
type StorageSnapshot struct {
	Life Life
	// Attached is true once the volume is available to the unit (provisioned and mounted).
	Attached bool
}

// storageOp is juju's storage resolver for CAAS models: storage hooks run between operations, once the unit has
// started a hook sequence (so after install and leader-elected, before config-changed and start), and never delay install.
func storageOp(local Local, remote Snapshot) (Operation, error) {
	snaps := make(map[string]StorageSnapshot, len(remote.Storage))
	for id, s := range remote.Storage {
		if remote.Life == Dying {
			// The unit is dying, so all of its storage is.
			s.Life = Dying
		}
		snaps[id] = s
	}

	var run bool
	switch {
	case local.Kind == Continue:
		run = true
	case local.Kind == RunHook && local.Step == Queued:
		// For CAAS, storage is provisioned after the pod has been created, so a queued hook only gives way once the unit started.
		run = local.Started
	}
	if !run {
		return Operation{}, ErrNoOperation
	}

	ids := make([]string, 0, len(snaps))
	for id := range snaps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		snap := snaps[id]
		attached, known := local.StorageAttached[id]
		attached = attached && known
		// Storage that is not alive and never had storage-attached run is simply dropped.
		if !attached && snap.Life != Alive {
			continue
		}
		switch snap.Life {
		case Alive:
			if attached || !snap.Attached {
				continue
			}
			return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: StorageAttached, StorageID: id}}, nil
		case Dying:
			if attached {
				return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: StorageDetaching, StorageID: id}}, nil
			}
		}
	}
	return Operation{}, ErrNoOperation
}

// ApplyStorageHook returns the attached map after a storage hook (juju's Attachments.CommitHook).
func ApplyStorageHook(attached map[string]bool, h HookInfo) map[string]bool {
	out := make(map[string]bool, len(attached)+1)
	for k, v := range attached {
		out[k] = v
	}
	switch h.Kind {
	case StorageAttached:
		out[h.StorageID] = true
	case StorageDetaching:
		out[h.StorageID] = false
	}
	return out
}
