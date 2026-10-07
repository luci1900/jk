// SPDX-License-Identifier: AGPL-3.0-only

package resolver

import (
	"reflect"
	"testing"
)

const sec = "secret:cnj3q47mp25c7a0e7klg"

func TestSecretChanged(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	hooks := func() HookInfo { return hookInfos(s)[0] }

	// An untracked secret does nothing. A tracked secret at the latest revision does nothing.
	s.local.TrackedSecrets = map[string]int{sec: 1}
	s.remote.ConsumedSecretInfo = map[string]ConsumedSecret{sec: {LatestRevision: 1, Label: "peer"}}
	s.run().want()

	// A new revision runs secret-changed with the new revision and the consumer label.
	s.remote.ConsumedSecretInfo = map[string]ConsumedSecret{sec: {LatestRevision: 2, Label: "peer"}}
	if h := hooks(); h.Kind != SecretChanged || h.SecretURI != sec || h.SecretRevision != 2 || h.SecretLabel != "peer" {
		t.Fatalf("%+v", h)
	}
	s.run().want("secret-changed")
	if s.local.TrackedSecrets[sec] != 2 {
		t.Fatalf("%v", s.local.TrackedSecrets)
	}
	s.run().want()
	s.restart()
	s.run().want()
}

func TestSecretChangedWaitsForInstallAndSkipsDyingUnits(t *testing.T) {
	s := newSim(t)
	s.local.TrackedSecrets = map[string]int{sec: 1}
	s.remote.ConsumedSecretInfo = map[string]ConsumedSecret{sec: {LatestRevision: 2}}
	s.run().want("install", "secret-changed", "config-changed", "start")
	s.remote.ConsumedSecretInfo = map[string]ConsumedSecret{sec: {LatestRevision: 3}}
	s.remote.Life = Dying
	s.run().want("stop", "remove", "<dead>")
}

func TestSecretChangedSeveralInURIOrder(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.remote.ConsumedSecretInfo = map[string]ConsumedSecret{"secret:b": {LatestRevision: 1}, "secret:a": {LatestRevision: 1}}
	s.run().want("secret-changed", "secret-changed")
	if len(s.local.TrackedSecrets) != 2 {
		t.Fatalf("%v", s.local.TrackedSecrets)
	}
}

func TestSecretRemoveForObsoleteRevisions(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.remote.ObsoleteSecretRevisions = map[string][]int{sec: {3, 1}}
	s.run().want("secret-remove", "secret-remove")
	if !reflect.DeepEqual(s.local.ObsoleteSecretRevisions[sec], []int{1, 3}) {
		t.Fatalf("%v", s.local.ObsoleteSecretRevisions)
	}
	// Processed revisions do not run again, also after a restart; a new one does.
	s.restart()
	s.run().want()
	s.remote.ObsoleteSecretRevisions = map[string][]int{sec: {1, 2, 3}}
	s.run().want("secret-remove")
	hooks := s.local.ObsoleteSecretRevisions[sec]
	if !reflect.DeepEqual(hooks, []int{1, 2, 3}) {
		t.Fatalf("%v", hooks)
	}
}

func TestSecretRemoveCarriesRevision(t *testing.T) {
	s := newSim(t)
	s.run()
	s.remote.ObsoleteSecretRevisions = map[string][]int{sec: {4}}
	h := hookInfos(s)[0]
	if h.Kind != SecretRemove || h.SecretRevision != 4 || h.SecretURI != sec {
		t.Fatalf("%+v", h)
	}
}

func TestSecretExpired(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.remote.ExpiredSecretRevisions = []string{SecretRevisionSpec(sec, 2)}
	h := hookInfos(s)[0]
	if h.Kind != SecretExpired || h.SecretRevision != 2 || h.SecretURI != sec {
		t.Fatalf("%+v", h)
	}
	// The agent drops the expired revision from the snapshot once the hook ran.
	s.step()
	s.want("secret-expired")
	s.remote.ExpiredSecretRevisions = nil
	s.run().want()
	// Garbage is ignored.
	s.remote.ExpiredSecretRevisions = []string{"nonsense"}
	s.run().want()
}

func TestDeletedSecretsArePruned(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.local.TrackedSecrets = map[string]int{sec: 2, "secret:other": 1}
	s.local.ObsoleteSecretRevisions = map[string][]int{sec: {1}, "secret:other": {1, 2}}
	s.remote.ObsoleteSecretRevisions = map[string][]int{"secret:other": {1, 2}}
	// The whole secret was removed.
	s.remote.DeletedSecretRevisions = map[string][]int{sec: nil}
	s.step()
	s.remote.DeletedSecretRevisions = nil
	s.want("<secrets-removed>")
	if _, ok := s.local.TrackedSecrets[sec]; ok || len(s.local.ObsoleteSecretRevisions[sec]) != 0 || s.local.TrackedSecrets["secret:other"] != 1 {
		t.Fatalf("%v %v", s.local.TrackedSecrets, s.local.ObsoleteSecretRevisions)
	}
	// A revision was removed: only the processed-obsolete record goes.
	s.remote.DeletedSecretRevisions = map[string][]int{"secret:other": {1}}
	s.step()
	s.remote.DeletedSecretRevisions = nil
	s.want("<secrets-removed>")
	if got := s.local.ObsoleteSecretRevisions["secret:other"]; !reflect.DeepEqual(got, []int{2}) || s.local.TrackedSecrets["secret:other"] != 1 {
		t.Fatalf("%v %v", s.local.TrackedSecrets, got)
	}
}

func TestProcessedObsoleteRevisionsForForgottenSecretsAreCollected(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.local.ObsoleteSecretRevisions = map[string][]int{"secret:gone": {1}, sec: {1, 2}}
	s.remote.ObsoleteSecretRevisions = map[string][]int{sec: {2}}
	s.run().want("<secrets-removed>")
	if _, ok := s.local.ObsoleteSecretRevisions["secret:gone"]; ok || !reflect.DeepEqual(s.local.ObsoleteSecretRevisions[sec], []int{2}) {
		t.Fatalf("%v", s.local.ObsoleteSecretRevisions)
	}
}

func TestSecretOrderBeforeStorageAndAfterPebble(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.remote.Pebble = []Pebble{ready("app", "b")}
	s.remote.Storage = attachedStorage("data/0")
	s.remote.ConsumedSecretInfo = map[string]ConsumedSecret{sec: {LatestRevision: 5}}
	s.run().want("app-pebble-ready", "secret-changed", "data-storage-attached")
}

func TestApplySecretHookCopies(t *testing.T) {
	tr := map[string]int{"a": 1}
	ob := map[string][]int{"a": {1}}
	tr2, ob2 := ApplySecretHook(tr, ob, HookInfo{Kind: SecretChanged, SecretURI: "b", SecretRevision: 3})
	tr3, ob3 := ApplySecretHook(tr2, ob2, HookInfo{Kind: SecretRemove, SecretURI: "a", SecretRevision: 0 + 2})
	_, ob4 := ApplySecretHook(tr3, ob3, HookInfo{Kind: SecretRemove, SecretURI: "a", SecretRevision: 2})
	if len(tr) != 1 || tr2["b"] != 3 || !reflect.DeepEqual(ob3["a"], []int{1, 2}) || !reflect.DeepEqual(ob4["a"], []int{1, 2}) || len(ob["a"]) != 1 {
		t.Fatalf("%v %v %v %v", tr2, ob3, ob4, ob)
	}
}
