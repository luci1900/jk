// SPDX-License-Identifier: AGPL-3.0-only

package resolver

import (
	"reflect"
	"testing"
)

func attachedStorage(ids ...string) map[string]StorageSnapshot {
	m := map[string]StorageSnapshot{}
	for _, id := range ids {
		m[id] = StorageSnapshot{Life: Alive, Attached: true}
	}
	return m
}

func TestStorageOrder(t *testing.T) {
	tests := []struct {
		name   string
		leader bool
		pebble []Pebble
		want   []string
	}{
		// Juju (CAAS): storage-attached runs between operations once install has run: after install and leader-elected,
		// after pebble-ready, before config-changed and start.
		{"non-leader", false, nil, []string{"install", "data-storage-attached", "config-changed", "start"}},
		{"leader", true, nil, []string{"install", "leader-elected", "data-storage-attached", "config-changed", "start"}},
		{"leader with pebble", true, []Pebble{ready("app", "b")}, []string{"install", "leader-elected", "app-pebble-ready", "data-storage-attached", "config-changed", "start"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSim(t)
			s.remote.Leader, s.remote.Pebble = tt.leader, tt.pebble
			s.remote.Storage = attachedStorage("data/0")
			s.run().want(tt.want...)
			if !s.local.StorageAttached["data/0"] {
				t.Fatalf("%v", s.local.StorageAttached)
			}
			s.run().want()
		})
	}
}

func TestStorageNotYetProvisionedWaitsAndNeverBlocksInstall(t *testing.T) {
	s := newSim(t)
	s.remote.Storage = map[string]StorageSnapshot{"data/0": {Life: Alive}}
	s.run().want("install", "config-changed", "start")
	s.remote.Storage = attachedStorage("data/0")
	s.run().want("data-storage-attached")
}

func TestStorageRestartDoesNotReattach(t *testing.T) {
	s := newSim(t)
	s.remote.Storage = attachedStorage("data/0", "logs/0")
	s.run().want("install", "data-storage-attached", "logs-storage-attached", "config-changed", "start")
	s.restart()
	s.run().want()
}

func TestStorageRestartBetweenInstallAndAttached(t *testing.T) {
	var reference []string
	{
		s := newSim(t)
		s.remote.Storage = attachedStorage("data/0")
		s.run()
		reference = s.ran
	}
	for n := 0; n <= len(reference)+2; n++ {
		s := newSim(t)
		s.remote.Storage = attachedStorage("data/0")
		for i := 0; i < n; i++ {
			s.step()
		}
		s.restart()
		s.run()
		if !reflect.DeepEqual(s.ran, reference) {
			t.Fatalf("restart after %d: %v, want %v", n, s.ran, reference)
		}
	}
}

func TestStorageDetachingWhenUnitIsDying(t *testing.T) {
	s := newSim(t)
	s.remote.Storage = attachedStorage("data/0")
	s.run()
	s.ran = nil
	s.remote.Life = Dying
	s.run().want("data-storage-detaching", "stop", "remove", "<dead>")
	if s.local.StorageAttached["data/0"] {
		t.Fatalf("%v", s.local.StorageAttached)
	}
}

func TestStorageDetachingWhenStorageIsDying(t *testing.T) {
	s := newSim(t)
	s.remote.Storage = attachedStorage("data/0", "logs/0")
	s.run()
	s.ran = nil
	s.remote.Storage["data/0"] = StorageSnapshot{Life: Dying, Attached: true}
	s.run().want("data-storage-detaching")
	s.run().want()
	// Never attached storage that dies is simply dropped.
	s.remote.Storage["x/0"] = StorageSnapshot{Life: Dying, Attached: true}
	s.run().want()
}

func TestStorageHookWaitsForQueuedHookUntilStarted(t *testing.T) {
	// A queued hook before the unit started (leader-elected after install) is not interrupted by storage hooks.
	s := newSim(t)
	s.remote.Leader = true
	s.remote.Storage = attachedStorage("data/0")
	s.step() // install
	s.step() // accept leadership: leader-elected queued
	if op, err := s.r.NextOp(s.local, s.remote); err != nil || op.Hook.Kind != LeaderElected {
		t.Fatalf("%+v %v", op, err)
	}
	// Once started, a queued hook gives way.
	s2 := newSim(t)
	s2.local.State = State{Kind: RunHook, Step: Queued, Hook: &HookInfo{Kind: UpdateStatus}, Installed: true, Started: true}
	s2.remote.Storage = attachedStorage("data/0")
	if op, err := s2.r.NextOp(s2.local, s2.remote); err != nil || op.Hook.Kind != StorageAttached {
		t.Fatalf("%+v %v", op, err)
	}
}

func TestApplyStorageHookCopies(t *testing.T) {
	a := map[string]bool{"x/0": true}
	b := ApplyStorageHook(a, HookInfo{Kind: StorageAttached, StorageID: "y/0"})
	c := ApplyStorageHook(b, HookInfo{Kind: StorageDetaching, StorageID: "x/0"})
	if len(a) != 1 || !b["y/0"] || !b["x/0"] || c["x/0"] || !c["y/0"] {
		t.Fatalf("%v %v %v", a, b, c)
	}
}
