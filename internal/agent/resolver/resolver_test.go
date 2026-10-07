// SPDX-License-Identifier: AGPL-3.0-only

package resolver

import (
	"errors"
	"reflect"
	"testing"
)

// sim drives the resolver the way the agent's executor does and records the hooks that ran.
type sim struct {
	t      *testing.T
	r      *Resolver
	local  Local
	remote Snapshot
	ran    []string
	scopes []int
	failed map[string]int // hook name -> remaining failures
	// side effects seen
	reported         []string
	retryStarts      int
	retryStops       int
	cleared          int
	retryInitialized bool
}

func newSim(t *testing.T) *sim {
	s := &sim{t: t, failed: map[string]int{}}
	s.local = Local{State: Initial()}
	s.remote = Snapshot{ConfigHash: "h1"}
	s.r = New(Config{
		ShouldRetryHooks:    true,
		ReportHookError:     func(h HookInfo) error { s.reported = append(s.reported, h.Name()); return nil },
		ClearResolved:       func() error { s.cleared++; s.remote.Resolved = ResolvedNone; return nil },
		StartRetryHookTimer: func() { s.retryStarts++ },
		StopRetryHookTimer:  func() { s.retryStops++ },
	})
	return s
}

// restart simulates an agent restart: persisted state survives, the resolver and in-memory versions do not.
func (s *sim) restart() {
	old := s.local
	s.local = Local{State: old.State, Relations: old.Relations, StorageAttached: old.StorageAttached,
		TrackedSecrets: old.TrackedSecrets, ObsoleteSecretRevisions: old.ObsoleteSecretRevisions,
		CharmURL: old.CharmURL, CharmRevision: old.CharmRevision}
	s.local.UpdateStatusVersion = s.remote.UpdateStatusVersion
	s.local.RetryHookVersion = s.remote.RetryHookVersion
	cfg := s.r.cfg
	s.r = New(cfg)
}

// step runs one operation; it reports false when there is nothing to do.
func (s *sim) step() bool {
	s.t.Helper()
	op, err := s.r.NextOp(s.local, s.remote)
	if errors.Is(err, ErrNoOperation) || errors.Is(err, ErrUnitDead) {
		if errors.Is(err, ErrUnitDead) {
			s.ran = append(s.ran, "<dead>")
		}
		return false
	}
	if err != nil {
		s.t.Fatalf("NextOp: %v", err)
	}
	switch op.Kind {
	case OpAcceptLeadership:
		s.local.State = s.local.State.LeaderElected()
	case OpResignLeadership:
		s.local.State = s.local.State.LeaderDeposed()
	case OpEnterScope:
		rs := s.remote.Relations[op.Relation]
		s.local = s.local.EnteredScope(op.Relation, rs.Endpoint, rs.RemoteApp)
		s.scopes = append(s.scopes, op.Relation)
	case OpSecretsRemoved:
		s.local = s.local.SecretsRemoved(op.DeletedSecrets, op.DeletedObsolete)
		s.ran = append(s.ran, "<secrets-removed>")
	case OpRecordCharm:
		s.local = s.local.CharmRecorded(op.CharmURL, op.CharmRevision)
		s.ran = append(s.ran, "<charm "+op.CharmURL+">")
	case OpRunAction, OpFailAction:
		tag := "<action " + op.Action.Name + ">"
		if op.Kind == OpFailAction {
			tag = "<fail " + op.Action.Name + ": " + op.Message + ">"
		}
		s.ran = append(s.ran, tag)
		s.finishAction(op.Action.Name)
	case OpSkipHook:
		s.local = s.local.Committed(op.Hook, s.remote.ConfigHash)
		s.afterHook(op.Hook)
	case OpRunHook:
		h := op.Hook
		s.local.RetryHookVersion = s.remote.RetryHookVersion
		s.local.State = s.local.State.Prepared(h, "x")
		s.ran = append(s.ran, h.Name())
		if s.failed[h.Name()] > 0 {
			s.failed[h.Name()]--
			s.ran[len(s.ran)-1] += "!"
			return true // stays Pending
		}
		s.local.State = s.local.State.Executed(h, false)
		s.local = s.local.Committed(h, s.remote.ConfigHash)
		s.afterHook(h)
	}
	if err := s.local.Validate(); err != nil {
		s.t.Fatal(err)
	}
	return true
}

// finishAction removes an action from the pending and orphaned lists, as the agent's status write does.
func (s *sim) finishAction(name string) {
	keep := func(in []ActionRef) []ActionRef {
		var out []ActionRef
		for _, a := range in {
			if a.Name != name {
				out = append(out, a)
			}
		}
		return out
	}
	s.remote.PendingActions = keep(s.remote.PendingActions)
	s.remote.OrphanedActions = keep(s.remote.OrphanedActions)
}

func (s *sim) afterHook(h HookInfo) {
	s.local.UpdateStatusVersion = s.remote.UpdateStatusVersion
}

func (s *sim) run() *sim {
	s.t.Helper()
	for i := 0; i < 50; i++ {
		if !s.step() {
			return s
		}
	}
	s.t.Fatalf("resolver did not settle; ran %v", s.ran)
	return s
}

func (s *sim) want(hooks ...string) {
	s.t.Helper()
	if !reflect.DeepEqual(append([]string(nil), s.ran...), hooks) && !(len(s.ran) == 0 && len(hooks) == 0) {
		s.t.Fatalf("hooks ran %v, want %v", s.ran, hooks)
	}
	s.ran = nil
}

func ready(c, id string) Pebble { return Pebble{Container: c, BootID: id} }

func TestFreshUnit(t *testing.T) {
	tests := []struct {
		name   string
		remote Snapshot
		want   []string
	}{
		{"non-leader, no pebble", Snapshot{ConfigHash: "h"}, []string{"install", "config-changed", "start"}},
		{"leader", Snapshot{ConfigHash: "h", Leader: true}, []string{"install", "leader-elected", "config-changed", "start"}},
		{"pebble ready at install, non-leader", Snapshot{ConfigHash: "h", Pebble: []Pebble{ready("app", "b1")}},
			[]string{"install", "app-pebble-ready", "config-changed", "start"}},
		{"leader and pebble ready: leader-elected first", Snapshot{ConfigHash: "h", Leader: true, Pebble: []Pebble{ready("app", "b1")}},
			[]string{"install", "leader-elected", "app-pebble-ready", "config-changed", "start"}},
		{"two containers in name order", Snapshot{ConfigHash: "h", Pebble: []Pebble{ready("web", "w"), ready("db", "d")}},
			[]string{"install", "db-pebble-ready", "web-pebble-ready", "config-changed", "start"}},
		{"unready container is ignored", Snapshot{ConfigHash: "h", Pebble: []Pebble{{Container: "app"}}},
			[]string{"install", "config-changed", "start"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSim(t)
			s.remote = tt.remote
			s.run().want(tt.want...)
			if !s.local.Installed || !s.local.Started {
				t.Fatalf("state not installed/started: %+v", s.local.State)
			}
			// Settled: nothing more happens.
			s.run().want()
		})
	}
}

func TestPebbleReadyLater(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	// Pebble answers after start.
	s.remote.Pebble = []Pebble{ready("app", "b1")}
	s.run().want("app-pebble-ready")
	// Same boot ID: nothing. New boot ID (workload container restarted): again.
	s.run().want()
	s.remote.Pebble = []Pebble{ready("app", "b2")}
	s.run().want("app-pebble-ready")
	// Pebble going away and coming back with the same boot ID is not a restart.
	s.remote.Pebble = []Pebble{{Container: "app"}}
	s.run().want()
	s.remote.Pebble = []Pebble{ready("app", "b2")}
	s.run().want()
}

func TestRestartAfterEveryStep(t *testing.T) {
	// Restarting the agent between any two operations must not change the hook sequence.
	var reference []string
	{
		s := newSim(t)
		s.remote.Leader = true
		s.remote.Pebble = []Pebble{ready("app", "b1")}
		s.run()
		reference = s.ran
	}
	for n := 0; n <= len(reference)+3; n++ {
		s := newSim(t)
		s.remote.Leader = true
		s.remote.Pebble = []Pebble{ready("app", "b1")}
		for i := 0; i < n; i++ {
			s.step()
		}
		s.restart()
		s.run()
		if !reflect.DeepEqual(s.ran, reference) {
			t.Fatalf("restart after %d ops: ran %v, want %v", n, s.ran, reference)
		}
	}
}

func TestRestartNeverReinstalls(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.restart()
	s.run().want()
	s.remote.Pebble = []Pebble{ready("app", "b1")}
	s.run().want("app-pebble-ready")
	s.restart()
	s.run().want() // boot ID was persisted: no second pebble-ready
	if s.local.State.PebbleBoot["app"] != "b1" {
		t.Fatalf("state %+v", s.local.State)
	}
}

func TestRestartWhileHookPendingReruns(t *testing.T) {
	// An agent that died mid-hook finds the hook Pending and treats it as failed: it reports and retries.
	s := newSim(t)
	s.local.State = Initial().Prepared(HookInfo{Kind: Install}, "")
	s.run().want() // waits for the retry timer
	if !reflect.DeepEqual(s.reported, []string{"install"}) || s.retryStarts != 1 {
		t.Fatalf("reported %v, retry timers started %d", s.reported, s.retryStarts)
	}
	s.remote.RetryHookVersion++
	s.run().want("install", "config-changed", "start")
}

func TestConfigChange(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.ConfigHash = "h2"
	s.run().want("config-changed")
	s.run().want()
	// Changing and reverting between runs still runs once per observed hash.
	s.remote.ConfigHash = "h1"
	s.run().want("config-changed")
}

func TestUpdateStatus(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.UpdateStatusVersion++
	s.run().want("update-status")
	s.run().want()
	// Config changes take priority over update-status.
	s.remote.UpdateStatusVersion++
	s.remote.ConfigHash = "h2"
	s.run().want("config-changed") // any hook satisfies a pending timer tick, as in juju
	// A timer that fires during a fresh unit's startup is satisfied by the hooks that run (no duplicate).
	s2 := newSim(t)
	s2.remote.UpdateStatusVersion = 1
	s2.local.UpdateStatusVersion = 1
	s2.run().want("install", "config-changed", "start")
}

func TestLeadership(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	// Gained.
	s.remote.Leader = true
	s.run().want("leader-elected")
	if !s.local.Leader {
		t.Fatal("not leader")
	}
	// Still leader: no repeat, also after a restart.
	s.run().want()
	s.restart()
	s.run().want()
	// Lost: state only, no hook.
	s.remote.Leader = false
	s.run().want()
	if s.local.Leader {
		t.Fatal("still leader")
	}
	// Regained: hook again.
	s.remote.Leader = true
	s.run().want("leader-elected")
}

func TestLeadershipIsResignedWhenDying(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.Leader = true
	s.run().want("leader-elected")
	s.remote.Life = Dying
	s.run().want("stop", "remove", "<dead>")
	if s.local.Leader {
		t.Fatal("a dying unit still holds leadership")
	}
}

func TestLeadershipNotAcceptedBeforeInstallOrWhileHookPending(t *testing.T) {
	s := newSim(t)
	s.remote.Leader = true
	s.local.State = Initial().Prepared(HookInfo{Kind: Install}, "")
	s.run().want() // install is pending (failed): nothing, in particular no leader-elected
	if s.local.Leader {
		t.Fatal("accepted leadership before install")
	}
	// Installed but a hook is pending: leader-elected waits.
	s2 := newSim(t)
	s2.run().want("install", "config-changed", "start")
	s2.failed["update-status"] = 1
	s2.remote.UpdateStatusVersion++
	s2.run().want("update-status!")
	s2.remote.Leader = true
	s2.run().want()
	if s2.local.Leader {
		t.Fatal("accepted leadership while a hook is pending")
	}
	s2.remote.RetryHookVersion++
	s2.run().want("update-status", "leader-elected")
}

func TestLeaderLostWhileLeaderElectedQueued(t *testing.T) {
	// leader-elected is queued; the agent loses leadership before it runs. The executor skips the hook
	// in Prepare when the unit is no longer leader; the resolver itself keeps the hook queued and resigns on
	// the next Continue. Here: the queued hook still resolves to a RunHook.
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.Leader = true
	s.step() // accept leadership
	s.remote.Leader = false
	if op, err := s.r.NextOp(s.local, s.remote); err != nil || op.Kind != OpResignLeadership {
		t.Fatalf("op %+v err %v", op, err)
	}
	s.local.State = s.local.State.LeaderDeposed()
	op, err := s.r.NextOp(s.local, s.remote)
	if err != nil || op.Kind != OpRunHook || op.Hook.Kind != LeaderElected {
		t.Fatalf("op %+v err %v", op, err)
	}
}

func TestHookFailureRetryBackoffAndSuccess(t *testing.T) {
	s := newSim(t)
	s.failed["config-changed"] = 2
	s.run().want("install", "config-changed!")
	if !reflect.DeepEqual(s.reported, []string{"config-changed"}) || s.retryStarts != 1 || s.retryStops != 0 {
		t.Fatalf("reported=%v starts=%d stops=%d", s.reported, s.retryStarts, s.retryStops)
	}
	// Re-resolving without a timer tick does nothing and does not start a second timer.
	s.run().want()
	if s.retryStarts != 1 {
		t.Fatalf("retry timer started %d times", s.retryStarts)
	}
	// Timer fires: retry; it fails again; the timer is started again.
	s.remote.RetryHookVersion++
	s.run().want("config-changed!")
	if s.retryStarts != 2 {
		t.Fatalf("retry timer starts = %d, want 2", s.retryStarts)
	}
	// Third attempt succeeds, the flow continues and the timer is stopped.
	s.remote.RetryHookVersion++
	s.run().want("config-changed", "start")
	if s.retryStops == 0 {
		t.Fatal("retry timer not stopped after success")
	}
}

func TestHookFailureNoAutoRetry(t *testing.T) {
	s := newSim(t)
	s.r = New(Config{ShouldRetryHooks: false, StartRetryHookTimer: func() { s.retryStarts++ }})
	s.failed["install"] = 1
	s.run().want("install!")
	if s.retryStarts != 0 {
		t.Fatal("timer started with retries disabled")
	}
}

func TestResolved(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		s := newSim(t)
		s.failed["install"] = 1
		s.run().want("install!")
		s.remote.Resolved = ResolvedRetryHooks
		s.run().want("install", "config-changed", "start")
		if s.cleared != 1 {
			t.Fatalf("cleared %d", s.cleared)
		}
	})
	t.Run("no hooks skips", func(t *testing.T) {
		s := newSim(t)
		s.failed["install"] = 100
		s.run().want("install!")
		s.remote.Resolved = ResolvedNoHooks
		s.run().want("config-changed", "start")
		if !s.local.Installed || s.cleared != 1 {
			t.Fatalf("installed=%v cleared=%d", s.local.Installed, s.cleared)
		}
	})
}

func TestTerminationRunsStopAndRemove(t *testing.T) {
	s := newSim(t)
	s.remote.Leader = true
	s.run().want("install", "leader-elected", "config-changed", "start")
	s.remote.Life = Dying
	s.run().want("stop", "remove", "<dead>")
	if s.local.Leader {
		t.Fatal("leadership kept while dying")
	}
	if !s.local.Stopped || !s.local.Removed {
		t.Fatalf("state %+v", s.local.State)
	}
}

func TestTerminationBeforeStartSkipsStop(t *testing.T) {
	s := newSim(t)
	s.local.State = Initial().Committed(HookInfo{Kind: Install}, "") // installed, not started
	s.remote.Life = Dying
	s.run().want("remove", "<dead>")
	// Never installed: straight to dead.
	s2 := newSim(t)
	s2.local.State = State{Kind: Continue, Step: Pending}
	s2.remote.Life = Dying
	s2.run().want("<dead>")
}

func TestTerminationRestartMidStop(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.Life = Dying
	s.local.State = s.local.State.Prepared(HookInfo{Kind: Stop}, "") // killed during stop: pending
	s.restart()
	s.retryNow()
	s.run().want("stop", "remove", "<dead>")
}

func (s *sim) retryNow() { s.remote.RetryHookVersion++ }

func TestDyingIgnoresPebbleAndUpdateStatus(t *testing.T) {
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.remote.Life = Dying
	s.remote.Pebble = []Pebble{ready("app", "b9")}
	s.remote.UpdateStatusVersion++
	s.remote.ConfigHash = "other"
	s.run().want("stop", "remove", "<dead>")
}

func TestDeadWhenRemoved(t *testing.T) {
	s := newSim(t)
	s.local.State = State{Kind: Continue, Step: Pending, Installed: true, Removed: true}
	s.run().want("<dead>")
}

func TestQueuedHookIsRunAfterTermination(t *testing.T) {
	// A hook killed by shutdown is requeued, not pending: no error is reported and it runs on restart.
	s := newSim(t)
	s.run().want("install", "config-changed", "start")
	s.local.State = s.local.State.Requeued(HookInfo{Kind: UpdateStatus})
	s.restart()
	s.run().want("update-status")
	if len(s.reported) != 0 {
		t.Fatalf("error reported for a requeued hook: %v", s.reported)
	}
}

func TestStateTransitions(t *testing.T) {
	st := Initial()
	if err := st.Validate(); err != nil {
		t.Fatal(err)
	}
	p := st.Prepared(HookInfo{Kind: Install}, "")
	if p.Step != Pending || p.HookStep != nil {
		t.Fatalf("%+v", p)
	}
	d := p.Executed(HookInfo{Kind: Install}, true)
	if d.Step != Done || *d.HookStep != Done || !d.StatusSet {
		t.Fatalf("%+v", d)
	}
	c := d.Committed(HookInfo{Kind: Install}, "h")
	if c.Kind != Continue || !c.Installed || c.Hook != nil || !c.StatusSet {
		t.Fatalf("%+v", c)
	}
	// config-changed on an unstarted unit queues start; on a started unit it does not.
	q := c.Committed(HookInfo{Kind: ConfigChanged}, "h")
	if q.Kind != RunHook || q.Hook.Kind != Start || q.ConfigHash != "h" || q.Step != Queued {
		t.Fatalf("%+v", q)
	}
	started := q.Committed(HookInfo{Kind: Start}, "")
	if !started.Started || started.Stopped {
		t.Fatalf("%+v", started)
	}
	again := started.Committed(HookInfo{Kind: ConfigChanged}, "h2")
	if again.Kind != Continue || again.ConfigHash != "h2" {
		t.Fatalf("%+v", again)
	}
	// Commit does not mutate its input (shared maps).
	a := State{Kind: Continue, Step: Pending, PebbleBoot: map[string]string{"a": "1"}}
	b := a.Committed(HookInfo{Kind: PebbleReady, WorkloadName: "b", BootID: "2"}, "")
	if len(a.PebbleBoot) != 1 || len(b.PebbleBoot) != 2 {
		t.Fatalf("a=%v b=%v", a.PebbleBoot, b.PebbleBoot)
	}
	for _, bad := range []State{{Kind: "x", Step: Queued}, {Kind: RunHook, Step: Queued}, {Kind: Continue, Step: Queued, Hook: &HookInfo{}}, {Kind: Continue, Step: "y"}} {
		if bad.Validate() == nil {
			t.Fatalf("%+v validated", bad)
		}
	}
	if (HookInfo{Kind: PebbleReady, WorkloadName: "web"}).Name() != "web-pebble-ready" {
		t.Fatal("name")
	}
}

func TestReportHookErrorFailureIsReturned(t *testing.T) {
	boom := errors.New("boom")
	r := New(Config{ReportHookError: func(HookInfo) error { return boom }, ClearResolved: func() error { return boom }})
	local := Local{State: Initial().Prepared(HookInfo{Kind: Install}, "")}
	if _, err := r.NextOp(local, Snapshot{}); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	r = New(Config{ClearResolved: func() error { return boom }})
	for _, m := range []ResolvedMode{ResolvedRetryHooks, ResolvedNoHooks} {
		if _, err := r.NextOp(local, Snapshot{Resolved: m}); !errors.Is(err, boom) {
			t.Fatalf("mode %v err %v", m, err)
		}
	}
	if _, err := r.NextOp(local, Snapshot{Resolved: 99}); err == nil {
		t.Fatal("unknown mode accepted")
	}
	if _, err := r.NextOp(Local{State: State{Kind: "weird", Step: Queued}}, Snapshot{}); err == nil {
		t.Fatal("unknown kind accepted")
	}
	bogus := Local{State: State{Kind: RunHook, Step: "weird", Hook: &HookInfo{Kind: Start}}}
	if _, err := r.NextOp(bogus, Snapshot{}); err == nil {
		t.Fatal("unknown step accepted")
	}
}
