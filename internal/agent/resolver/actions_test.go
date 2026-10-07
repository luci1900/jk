// SPDX-License-Identifier: AGPL-3.0-only

package resolver

import "testing"

func TestActionsRunInOrderBetweenHooks(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.PendingActions = []ActionRef{{"a1", 1}, {"a2", 2}}
	s.run().want("<action a1>", "<action a2>")
	s.run().want()
}

func TestActionsWaitForTheInstallHook(t *testing.T) {
	s := newSim(t)
	s.remote.PendingActions = []ActionRef{{"a1", 1}}
	s.run().want("install", "<action a1>", "config-changed", "start")
}

func TestActionRunsWhileAHookIsFailing(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.remote.ConfigHash = "h2"
	s.failed["config-changed"] = 1
	s.run().want("config-changed!")
	s.remote.PendingActions = []ActionRef{{"a1", 1}}
	if op, err := s.r.NextOp(s.local, s.remote); err != nil || op.Kind != OpRunAction || op.Action.Name != "a1" {
		t.Fatalf("%+v %v", op, err)
	}
}

func TestOrphanedAndDyingActionsFail(t *testing.T) {
	s := newSim(t)
	s.run()
	s.ran = nil
	s.remote.OrphanedActions = []ActionRef{{"o", 3}}
	s.remote.PendingActions = []ActionRef{{"p", 4}}
	s.run().want("<fail o: "+ActionTerminatedMessage+">", "<action p>")

	s.remote.PendingActions = []ActionRef{{"q", 5}}
	s.remote.Life = Dying
	s.run().want("<fail q: "+ActionDyingMessage+">", "stop", "remove", "<dead>")

	s = newSim(t)
	s.run()
	s.ran = nil
	s.remote.AppDying = true
	s.remote.PendingActions = []ActionRef{{"r", 6}}
	s.run().want("<fail r: " + ActionDyingMessage + ">")
}

func TestCharmIsRecordedThenUpgraded(t *testing.T) {
	s := newSim(t)
	s.remote.CharmURL, s.remote.CharmRevision = "img-a", 1
	// Fresh install: recorded without an upgrade hook, before config-changed.
	s.run().want("install", "<charm img-a>", "config-changed", "start")
	if s.local.CharmURL != "img-a" || s.local.CharmRevision != 1 {
		t.Fatalf("%+v", s.local)
	}
	s.run().want()
	// A new charm: upgrade-charm, then config-changed (no start), recorded at the commit.
	s.remote.CharmURL, s.remote.CharmRevision = "img-b", 2
	s.run().want("upgrade-charm", "config-changed")
	if s.local.CharmURL != "img-b" || s.local.CharmRevision != 2 {
		t.Fatalf("%+v", s.local)
	}
	s.restart()
	s.run().want()
}

func TestUpgradeCharmComesBeforeEverythingElse(t *testing.T) {
	s := newPeerSim(t, nil)
	s.remote.CharmURL = "a"
	s.run()
	s.ran = nil
	s.restart()
	s.remote.CharmURL = "b"
	s.remote.Pebble = []Pebble{{Container: "c", BootID: "new"}}
	s.remote.Relations = map[int]RelationSnapshot{1: peerRel(map[string]int64{"app/1": 4}, 0), 2: {Endpoint: "x", RemoteApp: "o"}}
	s.run()
	if s.ran[0] != "upgrade-charm" || s.ran[1] != "config-changed" {
		t.Fatalf("%v", s.ran)
	}
}

func TestFailedUpgradeIsRetriedAndNotRecorded(t *testing.T) {
	s := newSim(t)
	s.remote.CharmURL = "a"
	s.run()
	s.remote.CharmURL = "b"
	s.failed["upgrade-charm"] = 1
	s.run()
	if s.local.CharmURL != "a" {
		t.Fatalf("%q", s.local.CharmURL)
	}
	s.remote.RetryHookVersion++
	s.run()
	if s.local.CharmURL != "b" {
		t.Fatalf("%q", s.local.CharmURL)
	}
}

func TestNoCharmIdentityNoUpgrade(t *testing.T) {
	s := newSim(t)
	s.run()
	s.local.CharmURL = "a"
	s.run().want("install", "config-changed", "start")
	// A dying unit does not upgrade.
	s.remote.CharmURL = "z"
	s.remote.Life = Dying
	s.run().want("stop", "remove", "<dead>")
}
