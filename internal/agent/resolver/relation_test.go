// SPDX-License-Identifier: AGPL-3.0-only

package resolver

import (
	"reflect"
	"testing"
)

const peerEP = "database-peers"

func peerRel(members map[string]int64, appVersion int64) RelationSnapshot {
	return RelationSnapshot{Peer: true, Endpoint: peerEP, RemoteApp: "app", Members: members, AppVersion: appVersion}
}

func newPeerSim(t *testing.T, members map[string]int64) *sim {
	s := newSim(t)
	s.remote.Unit = "app/0"
	s.remote.Relations = map[int]RelationSnapshot{1: peerRel(members, 0)}
	return s
}

func TestPeerRelationFreshUnit(t *testing.T) {
	tests := []struct {
		name   string
		leader bool
		want   []string
	}{
		// Juju runs relation-created right after install, before leader-elected and before storage and config hooks.
		{"non-leader", false, []string{"install", "database-peers-relation-created", "config-changed", "start"}},
		{"leader", true, []string{"install", "database-peers-relation-created", "leader-elected", "config-changed", "start"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newPeerSim(t, nil)
			s.remote.Leader = tt.leader
			s.run().want(tt.want...)
			if !reflect.DeepEqual(s.scopes, []int{1}) {
				t.Fatalf("scope entries %v", s.scopes)
			}
			if st := s.local.Relations[1]; !st.InScope || !st.Created {
				t.Fatalf("relation state %+v", st)
			}
			s.run().want()
		})
	}
}

func TestScopeIsEnteredAfterInstall(t *testing.T) {
	s := newPeerSim(t, nil)
	// Nothing about relations happens before install has run.
	if op, err := s.r.NextOp(s.local, s.remote); err != nil || op.Kind != OpRunHook || op.Hook.Kind != Install {
		t.Fatalf("%+v %v", op, err)
	}
	s.run()
	if len(s.scopes) != 1 {
		t.Fatalf("scopes %v", s.scopes)
	}
}

func TestPeersJoinChangeAndDepart(t *testing.T) {
	s := newPeerSim(t, nil)
	s.run().want("install", "database-peers-relation-created", "config-changed", "start")

	set := func(members map[string]int64, app int64) {
		s.remote.Relations = map[int]RelationSnapshot{1: peerRel(members, app)}
	}
	// Two peers appear: joined then (queued first) changed for each, units in name order.
	set(map[string]int64{"app/2": 20, "app/1": 10}, 0)
	s.run().want("database-peers-relation-joined", "database-peers-relation-changed",
		"database-peers-relation-joined", "database-peers-relation-changed")
	if !reflect.DeepEqual(s.local.Relations[1].Members, map[string]int64{"app/1": 10, "app/2": 20}) {
		t.Fatalf("%+v", s.local.Relations[1])
	}
	s.run().want()

	// A settings version change: one changed hook for that unit only.
	set(map[string]int64{"app/2": 21, "app/1": 10}, 0)
	s.run().want("database-peers-relation-changed")
	s.run().want()

	// Application settings are set by the leader: an application hook with no remote unit.
	set(map[string]int64{"app/2": 21, "app/1": 10}, 7)
	hooks := hookInfos(s)
	if len(hooks) != 1 || hooks[0].RemoteUnit != "" || hooks[0].RemoteApp != "app" || hooks[0].ChangeVersion != 7 || hooks[0].Kind != RelationChanged {
		t.Fatalf("%+v", hooks)
	}
	s.run().want("database-peers-relation-changed")
	if s.local.Relations[1].AppVersion != 7 {
		t.Fatalf("%+v", s.local.Relations[1])
	}

	// A peer leaves: departed with the unit as the departing unit.
	set(map[string]int64{"app/2": 21}, 7)
	hooks = hookInfos(s)
	if len(hooks) != 1 || hooks[0].Kind != RelationDeparted || hooks[0].RemoteUnit != "app/1" || hooks[0].DepartingUnit != "app/1" || hooks[0].ChangeVersion != 10 {
		t.Fatalf("%+v", hooks)
	}
	s.run().want("database-peers-relation-departed")
	if _, ok := s.local.Relations[1].Members["app/1"]; ok {
		t.Fatalf("%+v", s.local.Relations[1])
	}
	// It comes back (a new pod with the same name): joins again.
	set(map[string]int64{"app/2": 21, "app/1": 30}, 7)
	s.run().want("database-peers-relation-joined", "database-peers-relation-changed")
}

func TestApplicationHookComesBeforeJoined(t *testing.T) {
	s := newPeerSim(t, nil)
	s.run()
	s.ran = nil
	s.remote.Relations = map[int]RelationSnapshot{1: peerRel(map[string]int64{"app/1": 5}, 3)}
	hooks := hookInfos(s)
	if hooks[0].RemoteUnit != "" {
		t.Fatalf("first hook %+v", hooks[0])
	}
	s.run().want("database-peers-relation-changed", "database-peers-relation-joined", "database-peers-relation-changed")
}

func TestUnsetApplicationDataDoesNotTriggerAHook(t *testing.T) {
	s := newPeerSim(t, map[string]int64{"app/1": 5})
	s.run().want("install", "database-peers-relation-created", "config-changed", "start",
		"database-peers-relation-joined", "database-peers-relation-changed")
	s.run().want()
}

func TestRelationRestartPersistence(t *testing.T) {
	members := map[string]int64{"app/1": 10, "app/2": 20}
	var reference []string
	{
		s := newPeerSim(t, members)
		s.remote.Leader = true
		s.run()
		reference = s.ran
	}
	if len(reference) < 8 {
		t.Fatalf("reference %v", reference)
	}
	for n := 0; n <= len(reference)+4; n++ {
		s := newPeerSim(t, members)
		s.remote.Leader = true
		for i := 0; i < n; i++ {
			s.step()
		}
		s.restart()
		s.run()
		if !reflect.DeepEqual(s.ran, reference) {
			t.Fatalf("restart after %d ops: ran %v, want %v", n, s.ran, reference)
		}
		if len(s.scopes) != 1 {
			// Scope entry is idempotent; a restart before it simply enters again, never twice.
			t.Fatalf("restart after %d ops entered scope %v", n, s.scopes)
		}
	}
}

func TestChangedPendingSurvivesRestartAndTakesPriority(t *testing.T) {
	s := newPeerSim(t, nil)
	s.run()
	s.ran = nil
	s.remote.Relations = map[int]RelationSnapshot{1: peerRel(map[string]int64{"app/1": 5}, 0)}
	s.step() // joined
	s.ran = nil
	if st := s.local.Relations[1]; st.ChangedPending != "app/1" {
		t.Fatalf("%+v", st)
	}
	// While the changed hook is pending, another peer appears and the first one's version moves on.
	s.remote.Relations = map[int]RelationSnapshot{1: peerRel(map[string]int64{"app/0b": 1, "app/1": 6}, 0)}
	s.restart()
	hooks := hookInfos(s)
	if hooks[0].Kind != RelationChanged || hooks[0].RemoteUnit != "app/1" || hooks[0].ChangeVersion != 6 {
		t.Fatalf("%+v", hooks[0])
	}
	s.run().want("database-peers-relation-changed", "database-peers-relation-joined", "database-peers-relation-changed")
}

func TestScaleDownOfThisUnit(t *testing.T) {
	s := newPeerSim(t, map[string]int64{"app/1": 10, "app/2": 20})
	s.remote.Leader = true
	s.run()
	s.ran = nil
	// This unit is going away: it departs from every peer (departing unit: itself), then stops and is removed. Peer
	// relations are never broken.
	s.remote.Life = Dying
	s.run().want("database-peers-relation-departed", "database-peers-relation-departed", "stop", "remove", "<dead>")
	if st := s.local.Relations[1]; st.InScope || len(st.Members) != 0 {
		t.Fatalf("scope not left: %+v", st)
	}
}

func TestDepartingUnitIsThisUnitWhenTheApplicationIsDying(t *testing.T) {
	s := newPeerSim(t, map[string]int64{"app/1": 10})
	s.run()
	s.remote.Relations = map[int]RelationSnapshot{1: peerRel(nil, 0)}
	s.remote.AppDying = true
	hooks := hookInfos(s)
	if hooks[0].Kind != RelationDeparted || hooks[0].DepartingUnit != "app/0" || hooks[0].RemoteUnit != "app/1" {
		t.Fatalf("%+v", hooks[0])
	}
}

func TestDyingUnitDepartsBeforeStopButAfterStorageDetaching(t *testing.T) {
	s := newPeerSim(t, map[string]int64{"app/1": 10})
	s.remote.Storage = map[string]StorageSnapshot{"pgdata/0": {Life: Alive, Attached: true}}
	s.run().want("install", "database-peers-relation-created", "pgdata-storage-attached", "config-changed", "start",
		"database-peers-relation-joined", "database-peers-relation-changed")
	s.remote.Life = Dying
	s.run().want("pgdata-storage-detaching", "database-peers-relation-departed", "stop", "remove", "<dead>")
}

func TestRelationBrokenForNonPeers(t *testing.T) {
	s := newSim(t)
	s.remote.Unit = "app/0"
	rs := RelationSnapshot{Endpoint: "db", RemoteApp: "other", Members: map[string]int64{"other/0": 1}}
	s.remote.Relations = map[int]RelationSnapshot{4: rs}
	s.run().want("install", "db-relation-created", "config-changed", "start", "db-relation-joined", "db-relation-changed")

	// The relation is removed: departed for each member, then broken, then the unit is out of scope.
	rs.Life = Dying
	s.remote.Relations = map[int]RelationSnapshot{4: rs}
	s.run().want("db-relation-departed", "db-relation-broken")
	if st := s.local.Relations[4]; st.InScope || st.Created || len(st.Members) != 0 || st.Endpoint != "db" || st.RemoteApp != "other" {
		t.Fatalf("state after broken: %+v", st)
	}
	s.run().want()
}

func TestSuspendedRelationIsBroken(t *testing.T) {
	s := newSim(t)
	rs := RelationSnapshot{Endpoint: "db", RemoteApp: "other", Members: map[string]int64{"other/0": 1}}
	s.remote.Relations = map[int]RelationSnapshot{4: rs}
	s.run()
	s.ran = nil
	rs.Suspended = true
	s.remote.Relations = map[int]RelationSnapshot{4: rs}
	s.run().want("db-relation-departed", "db-relation-broken")
}

func TestNonPeersBeforePeersAndIDOrder(t *testing.T) {
	s := newSim(t)
	s.remote.Relations = map[int]RelationSnapshot{
		3: peerRelEP("restart"),
		1: peerRelEP("upgrade"),
		2: {Endpoint: "db", RemoteApp: "other"},
	}
	// Created hooks follow the relation ids.
	s.run().want("install", "upgrade-relation-created", "db-relation-created", "restart-relation-created", "config-changed", "start")
	// Joins: non-peer relations first, peers last (by id).
	s.remote.Relations = map[int]RelationSnapshot{
		3: withMembers(peerRelEP("restart"), "app/1"),
		1: withMembers(peerRelEP("upgrade"), "app/1"),
		2: withMembers(RelationSnapshot{Endpoint: "db", RemoteApp: "other"}, "other/0"),
	}
	s.run().want("db-relation-joined", "db-relation-changed", "upgrade-relation-joined", "upgrade-relation-changed",
		"restart-relation-joined", "restart-relation-changed")
}

func peerRelEP(ep string) RelationSnapshot {
	return RelationSnapshot{Peer: true, Endpoint: ep, RemoteApp: "app"}
}

func withMembers(r RelationSnapshot, units ...string) RelationSnapshot {
	r.Members = map[string]int64{}
	for i, u := range units {
		r.Members[u] = int64(i + 1)
	}
	return r
}

func TestDeadOrSuspendedRelationsAreNotEntered(t *testing.T) {
	s := newSim(t)
	s.remote.Relations = map[int]RelationSnapshot{
		1: {Endpoint: "a", RemoteApp: "o", Life: Dying},
		2: {Endpoint: "b", RemoteApp: "o", Suspended: true},
	}
	s.run().want("install", "config-changed", "start")
	if len(s.scopes) != 0 {
		t.Fatalf("scopes %v", s.scopes)
	}
}

func TestRelationHooksWaitForPendingHook(t *testing.T) {
	s := newPeerSim(t, map[string]int64{"app/1": 1})
	s.run()
	s.ran = nil
	s.failed["update-status"] = 1
	s.remote.UpdateStatusVersion++
	s.run().want("update-status!")
	s.remote.Relations = map[int]RelationSnapshot{1: withMembers(peerRelEP(peerEP), "app/1", "app/2")}
	s.run().want() // the failed hook blocks relation hooks, as in juju
	s.remote.RetryHookVersion++
	s.run().want("update-status", "database-peers-relation-joined", "database-peers-relation-changed")
}

func TestRelationStateValidate(t *testing.T) {
	tests := []struct {
		name string
		st   RelationState
		h    HookInfo
		ok   bool
	}{
		{"join", RelationState{}, HookInfo{Kind: RelationJoined, RemoteUnit: "a/1"}, true},
		{"join twice", RelationState{Members: map[string]int64{"a/1": 1}}, HookInfo{Kind: RelationJoined, RemoteUnit: "a/1"}, false},
		{"change unjoined", RelationState{}, HookInfo{Kind: RelationChanged, RemoteUnit: "a/1"}, false},
		{"app change", RelationState{}, HookInfo{Kind: RelationChanged}, true},
		{"depart joined", RelationState{Members: map[string]int64{"a/1": 1}}, HookInfo{Kind: RelationDeparted, RemoteUnit: "a/1"}, true},
		{"pending change first", RelationState{ChangedPending: "a/1", Members: map[string]int64{"a/1": 1}}, HookInfo{Kind: RelationChanged, RemoteUnit: "a/2"}, false},
		{"pending change ok", RelationState{ChangedPending: "a/1", Members: map[string]int64{"a/1": 1}}, HookInfo{Kind: RelationChanged, RemoteUnit: "a/1"}, true},
		{"broken with members", RelationState{Members: map[string]int64{"a/1": 1}}, HookInfo{Kind: RelationBroken}, false},
		{"broken empty", RelationState{}, HookInfo{Kind: RelationBroken}, true},
		{"created", RelationState{}, HookInfo{Kind: RelationCreated}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.st.Validate(tt.h); (err == nil) != tt.ok {
				t.Fatalf("Validate = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestRelationApplyHookIsIdempotentAndCopies(t *testing.T) {
	st := RelationState{InScope: true, Members: map[string]int64{"a/1": 1}}
	h := HookInfo{Kind: RelationJoined, RemoteUnit: "a/2", ChangeVersion: 4}
	a := st.ApplyHook(h)
	b := a.ApplyHook(h)
	if !reflect.DeepEqual(a, b) || len(st.Members) != 1 || a.ChangedPending != "a/2" {
		t.Fatalf("st=%+v a=%+v b=%+v", st, a, b)
	}
	d := a.ApplyHook(HookInfo{Kind: RelationDeparted, RemoteUnit: "a/1"})
	if _, ok := d.Members["a/1"]; ok || len(a.Members) != 2 {
		t.Fatalf("d=%+v a=%+v", d, a)
	}
	app := a.ApplyHook(HookInfo{Kind: RelationChanged, ChangeVersion: 9})
	if app.AppVersion != 9 || app.ChangedPending != "" {
		t.Fatalf("%+v", app)
	}
	gone := app.ApplyHook(HookInfo{Kind: RelationDeparted})
	if gone.AppVersion != 0 {
		t.Fatalf("%+v", gone)
	}
}

func TestHookNames(t *testing.T) {
	for h, want := range map[HookInfo]string{
		{Kind: RelationJoined, Endpoint: "db"}:         "db-relation-joined",
		{Kind: StorageAttached, StorageID: "pgdata/3"}: "pgdata-storage-attached",
		{Kind: StorageDetaching, StorageID: "data/0"}:  "data-storage-detaching",
		{Kind: SecretChanged, SecretURI: "secret:abc"}: "secret-changed",
		{Kind: PebbleReady, WorkloadName: "web"}:       "web-pebble-ready",
		{Kind: Install}:                                "install",
	} {
		if got := h.Name(); got != want {
			t.Errorf("%+v: %q, want %q", h, got, want)
		}
	}
	if !(HookInfo{Kind: SecretRemove}).IsSecret() || (HookInfo{Kind: Install}).IsSecret() || (HookInfo{Kind: Install}).IsRelation() || (HookInfo{Kind: Install}).IsStorage() {
		t.Fatal("kind predicates")
	}
	if UnitApp("db-app/12") != "db-app" || StorageName("data/1") != "data" {
		t.Fatal("name helpers")
	}
}

// hookInfos returns the hook the resolver wants next without running it.
func hookInfos(s *sim) []HookInfo {
	op, err := s.r.NextOp(s.local, s.remote)
	if err != nil {
		s.t.Fatalf("NextOp: %v", err)
	}
	if op.Kind != OpRunHook {
		s.t.Fatalf("op %+v", op)
	}
	return []HookInfo{op.Hook}
}

func TestRelationRemovalHandshakeAtEveryStep(t *testing.T) {
	// Restarting before each step of a removal gives the same hooks and the same end state.
	for restartAt := 0; restartAt < 4; restartAt++ {
		s := newSim(t)
		s.remote.Unit = "app/0"
		rs := RelationSnapshot{Endpoint: "db", RemoteApp: "other", Members: map[string]int64{"other/0": 1, "other/1": 2}}
		s.remote.Relations = map[int]RelationSnapshot{4: rs}
		s.run().want("install", "db-relation-created", "config-changed", "start", "db-relation-joined", "db-relation-changed",
			"db-relation-joined", "db-relation-changed")
		rs.Life = Dying
		s.remote.Relations = map[int]RelationSnapshot{4: rs}
		var ran []string
		for i := 0; s.step() && i < 20; i++ {
			if i == restartAt {
				s.restart()
			}
		}
		ran = s.ran
		want := []string{"db-relation-departed", "db-relation-departed", "db-relation-broken"}
		if len(ran) != 3 || ran[0] != want[0] || ran[1] != want[1] || ran[2] != want[2] {
			t.Fatalf("restart at %d: %v", restartAt, ran)
		}
		if st := s.local.Relations[4]; st.InScope || st.Endpoint != "db" {
			t.Fatalf("restart at %d: %+v", restartAt, st)
		}
	}
}

func TestRemoteUnitChurnAndCoalescing(t *testing.T) {
	s := newSim(t)
	s.remote.Unit = "app/0"
	rel := func(m map[string]int64, app int64) map[int]RelationSnapshot {
		return map[int]RelationSnapshot{4: {Endpoint: "db", RemoteApp: "other", Members: m, AppVersion: app}}
	}
	s.remote.Relations = rel(nil, 0)
	s.run().want("install", "db-relation-created", "config-changed", "start")
	// Several versions of one unit's settings between two passes give one changed hook; the app hook comes first.
	s.remote.Relations = rel(map[string]int64{"other/0": 3, "other/1": 5}, 7)
	s.run().want("db-relation-changed", "db-relation-joined", "db-relation-changed", "db-relation-joined", "db-relation-changed")
	s.remote.Relations = rel(map[string]int64{"other/0": 4, "other/1": 6}, 7)
	s.remote.Relations = rel(map[string]int64{"other/0": 9, "other/1": 6}, 7)
	hooks := hookInfos(s)
	if len(hooks) != 1 || hooks[0].RemoteUnit != "other/0" || hooks[0].ChangeVersion != 9 {
		t.Fatalf("%+v", hooks)
	}
	s.run().want("db-relation-changed", "db-relation-changed")
	// A unit leaves and a new one joins in the same pass: departed first.
	s.remote.Relations = rel(map[string]int64{"other/1": 6, "other/2": 1}, 7)
	s.run().want("db-relation-departed", "db-relation-joined", "db-relation-changed")
	// App data removed: one more hook for the application (its version differs).
	s.remote.Relations = rel(map[string]int64{"other/1": 6, "other/2": 1}, 0)
	s.run().want("db-relation-changed")
}
