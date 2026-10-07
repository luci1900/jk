package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
	"github.com/luci1900/jk/internal/hooktools"
)

// Defaults.
const (
	DefaultUpdateStatusInterval = 5 * time.Minute
	DefaultRetryMin             = 5 * time.Second
	DefaultRetryMax             = 5 * time.Minute
	DefaultRetryFactor          = 2
	// ResolvedPollInterval is how often a unit in a hook error looks for the resolved annotation.
	ResolvedPollInterval = 5 * time.Second
	installingMessage    = "installing charm software"
)

// Config configures an Agent. Unit identity comes from the environment the operator sets (JK_POD_NAME and friends).
type Config struct {
	App       string
	Unit      string // <app>/<n>
	Ordinal   int
	Namespace string
	ModelName string
	ModelUUID string
	// DataDir is /var/lib/juju: the charm is at <DataDir>/agents/unit-<app>-<n>/charm and the hook tools at <DataDir>/tools/unit-<app>-<n>.
	DataDir string
	// ContainersDir is /charm/containers (each workload's Pebble socket is <ContainersDir>/<name>/pebble.socket).
	ContainersDir string
	// Containers are the workload containers (JUJU_CONTAINER_NAMES); metadata.yaml's containers if empty.
	Containers []string
	// Self is the path of the jk-agent binary the hook tools link to.
	Self string

	Kube   Kube
	Pebble PebbleProber
	Clock  clock.Clock
	Runner HookRunner
	Log    io.Writer
	// Getenv reads the pod environment for the allow-listed variables (os.LookupEnv by default).
	Getenv func(string) (string, bool)
	PodIP  func() string
	// Network describes the pod's network for network-get (derived from PodIP when nil).
	Network func() hooktools.NetworkInfo
	// ActionPollInterval is how often a running action looks for an abort request (DefaultActionPollInterval if zero).
	ActionPollInterval time.Duration
	// SecretRetryDelay is the wait between attempts to read a secret the operator has not granted access to yet.
	SecretRetryDelay time.Duration

	RetryMin, RetryMax time.Duration
	RetryFactor        int
	PebbleInterval     time.Duration
}

func (c Config) unitTag() string {
	return fmt.Sprintf("unit-%s-%d", c.App, c.Ordinal)
}

// AgentDir is <DataDir>/agents/unit-<app>-<n>.
func (c Config) AgentDir() string { return filepath.Join(c.DataDir, "agents", c.unitTag()) }

// CharmDir is the charm directory.
func (c Config) CharmDir() string { return filepath.Join(c.AgentDir(), "charm") }

// ToolsDir is the directory of hook tool symlinks.
func (c Config) ToolsDir() string { return filepath.Join(c.DataDir, "tools", c.unitTag()) }

// Agent is the unit agent. Reconcile is its single entry point: it is called by the controller (and by tests)
// and runs operations until the resolver has nothing more to do.
type Agent struct {
	cfg   Config
	kube  Kube
	clk   clock.Clock
	store *UnitStore
	lead  *Leadership
	peb   *PebblePoller
	res   *resolver.Resolver

	// Trigger asks for another reconcile (set by Run; nil in tests).
	Trigger func()

	mu          sync.Mutex // serialises Reconcile
	inited      bool
	curCtx      context.Context
	local       resolver.Local
	charm       *Charm
	jujuVersion string
	hookSeq     int
	lastWorld   *world

	// relMeta remembers what the agent has seen of each relation, so that departures can still run after the
	// Relation object is gone; settingsSeen keeps the last settings of remote units for relation-get after they left.
	relMeta      map[int]relationMeta
	settingsSeen map[string]map[string]string
	// actionObjs are the Actions of the last snapshot by object name; actionsDone those this process has finished (the
	// cache may still show them in an older state).
	actionObjs  map[string]v1alpha1.Action
	actionsDone map[string]bool
	// expiredDone are the secret revisions whose secret-expired hook ran in this process.
	expiredDone map[string]bool

	life atomic.Int32 // resolver.Life
	done chan struct{}
	dead bool

	// timers
	statusInterval time.Duration
	statusFired    time.Time
	startedAt      time.Time
	statusVersion  int
	retryVersion   int
	retryDue       time.Time
	retryDelay     time.Duration
}

// New returns an Agent.
func New(cfg Config) *Agent {
	if cfg.Clock == nil {
		cfg.Clock = clock.RealClock{}
	}
	if cfg.Runner == nil {
		cfg.Runner = ExecRunner{}
	}
	if cfg.Log == nil {
		cfg.Log = os.Stdout
	}
	if cfg.Getenv == nil {
		cfg.Getenv = osGetenv
	}
	if cfg.PodIP == nil {
		cfg.PodIP = firstIPv4
	}
	if cfg.Pebble == nil {
		cfg.Pebble = SocketPebble{Dir: cfg.ContainersDir}
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = DefaultRetryMin
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = DefaultRetryMax
	}
	if cfg.RetryFactor <= 1 {
		cfg.RetryFactor = DefaultRetryFactor
	}
	if cfg.SecretRetryDelay == 0 {
		cfg.SecretRetryDelay = time.Second
	}
	a := &Agent{cfg: cfg, kube: cfg.Kube, clk: cfg.Clock, done: make(chan struct{}),
		relMeta: map[int]relationMeta{}, settingsSeen: map[string]map[string]string{}, expiredDone: map[string]bool{},
		actionsDone: map[string]bool{}}
	a.store = NewUnitStore(cfg.Kube, cfg.Clock)
	a.lead = NewLeadership(cfg.Kube, cfg.Unit, cfg.Clock)
	a.retryDelay = cfg.RetryMin
	a.statusInterval = DefaultUpdateStatusInterval
	a.res = resolver.New(resolver.Config{
		ShouldRetryHooks:    true,
		ReportHookError:     a.reportHookError,
		ClearResolved:       a.clearResolved,
		StartRetryHookTimer: a.startRetryTimer,
		StopRetryHookTimer:  a.stopRetryTimer,
	})
	return a
}

// Leadership exposes the leadership tracker (the lease renewal loop is run by Run).
func (a *Agent) Leadership() *Leadership { return a.lead }

// Done is closed once the unit's stop and remove hooks have run.
func (a *Agent) Done() <-chan struct{} { return a.done }

// Terminate makes the unit go away for good: stop and remove run next. Call it when the pod will not come back.
func (a *Agent) Terminate() {
	a.life.Store(int32(resolver.Dying))
	a.trigger()
}

func (a *Agent) trigger() {
	if a.Trigger != nil {
		a.Trigger()
	}
}

func (a *Agent) logf(format string, args ...any) {
	fmt.Fprintf(a.cfg.Log, "jk-agent: "+format+"\n", args...)
}

// State returns the current operation state (for tests and diagnostics).
func (a *Agent) State() resolver.State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.local.State
}

// setup loads persisted state and the charm, links the hook tools and records the initial statuses.
func (a *Agent) setup(ctx context.Context) error {
	app, err := a.kube.Application(ctx)
	if apierrors.IsNotFound(err) {
		app, err = &v1alpha1.Application{}, nil // being removed: the charm directory is enough
	}
	if err != nil {
		return fmt.Errorf("getting application: %w", err)
	}
	charm, err := LoadCharm(a.cfg.CharmDir(), app.Status.Charm)
	if err != nil {
		return err
	}
	a.charm = charm
	if len(a.cfg.Containers) == 0 {
		a.cfg.Containers = charm.Containers
	}
	a.jujuVersion = JujuVersion(charm.Assumes)
	a.peb = &PebblePoller{Prober: a.cfg.Pebble, Containers: a.cfg.Containers, Clock: a.clk, Interval: a.cfg.PebbleInterval}

	if err := a.linkTools(); err != nil {
		return err
	}
	if err := os.MkdirAll(a.cfg.AgentDir(), 0o755); err != nil {
		return err
	}

	st, err := a.store.Load(ctx)
	if err != nil {
		return err
	}
	a.local = localFromSpec(a.store.Spec(), st)
	a.local.UpdateStatusVersion = a.statusVersion
	a.local.RetryHookVersion = a.retryVersion
	if !a.store.exists {
		// First start of this unit: create UnitData with the initial statuses.
		if err := a.store.SetState(ctx, st,
			a.store.withAgentStatus(AgentAllocating, ""),
			func(sp *v1alpha1.UnitDataSpec) { sp.WorkloadStatus = a.store.status("waiting", "installing agent") },
		); err != nil {
			return err
		}
	} else if st.Kind == resolver.Continue {
		if err := a.store.SetAgentStatus(ctx, AgentIdle, ""); err != nil {
			return err
		}
	}
	// The pod may have a new address since the unit last ran: keep the seeded relation settings current.
	if err := a.refreshAddresses(ctx); err != nil {
		return err
	}
	a.statusFired = a.clk.Now()
	a.startedAt = a.statusFired
	return nil
}

func (a *Agent) linkTools() error {
	_ = os.RemoveAll(a.cfg.ToolsDir())
	if err := os.MkdirAll(a.cfg.ToolsDir(), 0o755); err != nil {
		return err
	}
	for _, n := range hooktools.Names {
		if err := os.Symlink(a.cfg.Self, filepath.Join(a.cfg.ToolsDir(), n)); err != nil {
			return err
		}
	}
	return nil
}

// Init loads persisted state and prepares the agent. It is idempotent; Reconcile calls it too.
func (a *Agent) Init(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.initLocked(ctx)
}

func (a *Agent) initLocked(ctx context.Context) error {
	if a.inited {
		return nil
	}
	if err := a.setup(ctx); err != nil {
		return err
	}
	a.inited = true
	return nil
}

// world is what a reconcile reads before resolving.
type world struct {
	app       *v1alpha1.Application
	model     map[string]string
	relations []v1alpha1.Relation
	units     []v1alpha1.UnitData
	appData   map[string]*v1alpha1.AppData
}

// Reconcile resolves and runs operations until there is nothing to do. It returns when the agent should next
// be woken for a timer (0 for none). Errors are API errors; a failing hook is not an error but a state.
func (a *Agent) Reconcile(ctx context.Context) (time.Duration, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.curCtx = ctx
	if err := a.initLocked(ctx); err != nil {
		return 0, err
	}
	if a.dead {
		return 0, nil
	}
	const maxOps = 100
	for i := 0; i < maxOps; i++ {
		w, wait, err := a.observe(ctx)
		if err != nil {
			return 0, err
		}
		if w == nil {
			return wait, nil
		}
		if wait := a.awaitLeaseHolder(); wait > 0 {
			return wait, nil
		}
		a.lastWorld = w
		snap := a.snapshot(ctx, w)
		op, err := a.res.NextOp(a.local, snap)
		switch {
		case errors.Is(err, resolver.ErrNoOperation):
			return a.nextWake(snap), nil
		case errors.Is(err, resolver.ErrUnitDead):
			a.markDead()
			return 0, nil
		case err != nil:
			return 0, err
		}
		if err := a.execute(ctx, op, snap, w); err != nil {
			return 0, err
		}
	}
	return time.Second, nil // yield; more work is likely queued
}

// observe reads the world. A missing Application is not an error before termination: the unit waits.
func (a *Agent) observe(ctx context.Context) (*world, time.Duration, error) {
	app, err := a.kube.Application(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if resolver.Life(a.life.Load()) == resolver.Alive {
				a.logf("application %s not found; waiting", a.cfg.App)
				return nil, 5 * time.Second, nil
			}
			app = &v1alpha1.Application{}
		} else {
			return nil, 0, err
		}
	}
	model, err := a.kube.ModelConfig(ctx)
	if err != nil {
		return nil, 0, err
	}
	if lease, err := a.kube.Lease(ctx); err == nil {
		a.lead.Observe(lease)
		if a.lead.NeedsRenewal() {
			// Newly assigned: renew now rather than at the next tick (the error is retried by the renewal loop).
			if _, err := a.lead.Renew(ctx); err != nil {
				a.logf("renewing lease: %v", err)
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, 0, err
	}
	w := &world{app: app, model: model, appData: map[string]*v1alpha1.AppData{}}
	if w.relations, err = a.kube.Relations(ctx); err != nil {
		return nil, 0, err
	}
	if w.units, err = a.kube.UnitDatas(ctx); err != nil {
		return nil, 0, err
	}
	ads, err := a.kube.AppDatas(ctx)
	if err != nil {
		return nil, 0, err
	}
	for i := range ads {
		w.appData[ads[i].Name] = &ads[i]
	}
	remote, err := a.kube.RemoteDatas(ctx)
	if err != nil {
		return nil, 0, err
	}
	a.addRemote(w, remote)
	return w, 0, nil
}

// effective computes the effective config and the hash that decides when config-changed runs: as in juju, a change of
// the trust level runs it too, since a charm may start working once it is trusted.
func (a *Agent) effective(w *world) (map[string]any, string) {
	cfg, bad := EffectiveConfig(a.charm.Options, w.app.Spec.Config)
	for _, b := range bad {
		a.logf("ignoring invalid config value %s", b)
	}
	return cfg, ConfigHash(map[string]any{"config": cfg, "trust": string(w.app.Spec.Trust)})
}

func parseInterval(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return def
}

// snapshot builds the resolver's view of the world and advances the timers.
func (a *Agent) snapshot(ctx context.Context, w *world) resolver.Snapshot {
	now := a.clk.Now()
	_, hash := a.effective(w)

	interval := parseInterval(modelValue(w.model, "update-status-hook-interval"), DefaultUpdateStatusInterval)
	a.statusInterval = interval
	if !now.Before(a.statusFired.Add(interval)) {
		a.statusVersion++
		a.statusFired = now
	}
	a.res.SetShouldRetryHooks(modelValue(w.model, "automatically-retry-hooks") != "false")

	if !a.retryDue.IsZero() && !now.Before(a.retryDue) {
		a.retryVersion++
		a.retryDue = time.Time{}
	}

	snap := resolver.Snapshot{
		Unit:                a.cfg.Unit,
		AppDying:            w.app.DeletionTimestamp != nil,
		Life:                resolver.Life(a.life.Load()),
		Leader:              a.lead.IsLeader(),
		ConfigHash:          hash,
		UpdateStatusVersion: a.statusVersion,
		RetryHookVersion:    a.retryVersion,
	}
	if a.peb != nil {
		snap.Pebble = a.peb.Snapshot()
	}
	snap.CharmURL, snap.CharmRevision = a.charmIdentity(w.app)
	a.actionSnapshot(ctx, &snap)
	snap.Relations = a.relationSnapshots(w)
	snap.Storage = a.storageSnapshots()
	a.secretSnapshots(ctx, w, &snap)
	if a.local.Kind == resolver.RunHook && a.local.Step == resolver.Pending {
		switch r, _ := a.kube.Resolved(ctx); r {
		case "retry":
			snap.Resolved = resolver.ResolvedRetryHooks
		case "no-retry":
			snap.Resolved = resolver.ResolvedNoHooks
		}
	}
	return snap
}

// nextWake is when the next timer needs a reconcile.
func (a *Agent) nextWake(snap resolver.Snapshot) time.Duration {
	now := a.clk.Now()
	var wake time.Duration
	consider := func(t time.Time) {
		d := t.Sub(now)
		if d <= 0 {
			d = time.Millisecond
		}
		if wake == 0 || d < wake {
			wake = d
		}
	}
	if resolver.Life(a.life.Load()) == resolver.Alive && a.local.Installed {
		consider(a.statusFired.Add(a.statusInterval))
		if t, ok := a.nextSecretExpiry(); ok {
			consider(t)
		}
	}
	if !a.retryDue.IsZero() {
		consider(a.retryDue)
	}
	if a.local.Kind == resolver.RunHook && a.local.Step == resolver.Pending {
		consider(now.Add(ResolvedPollInterval))
	}
	return wake
}

func (a *Agent) markDead() {
	if a.dead {
		return
	}
	a.dead = true
	a.logf("unit %s stopped and removed", a.cfg.Unit)
	close(a.done)
}

// Retry timer: juju's backoff timer, 5s doubling to 5min.
func (a *Agent) startRetryTimer() {
	a.retryDue = a.clk.Now().Add(a.retryDelay)
	a.retryDelay *= time.Duration(a.cfg.RetryFactor)
	if a.retryDelay > a.cfg.RetryMax {
		a.retryDelay = a.cfg.RetryMax
	}
}

func (a *Agent) stopRetryTimer() {
	a.retryDue = time.Time{}
	a.retryDelay = a.cfg.RetryMin
}

func (a *Agent) clearResolved() error { return a.kube.ClearResolved(a.curCtx) }

// reportHookError sets the agent status to error and records the failed hook. It is called by the resolver on every
// pass over a failed hook, so it writes only on a change.
func (a *Agent) reportHookError(h resolver.HookInfo) error {
	msg := "hook failed: " + h.Name()
	cur := a.store.Spec()
	if cur.AgentStatus != nil && cur.AgentStatus.State == AgentError && cur.AgentStatus.Message == msg {
		return nil
	}
	raw, _ := json.Marshal(map[string]string{"hook": h.Name()})
	return a.store.Update(a.curCtx, func(sp *v1alpha1.UnitDataSpec) {
		sp.AgentStatus = a.store.status(AgentError, msg)
		sp.ErrorState = &v1alpha1.JSON{Raw: raw}
	})
}

func newContextID(unit, hook string, seq int) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s-%d-%s", unit, hook, seq, hex.EncodeToString(b[:]))
}

// LeaseWaitTimeout is how long a fresh unit waits for the Lease holder to be set before running install as a non-leader.
const LeaseWaitTimeout = 30 * time.Second

// awaitLeaseHolder holds back the first operation of a fresh unit until the Lease has a holder (and, if it is this
// unit, until its first renewal), so that leader-elected runs right after install as in juju. It returns how long
// to wait, or 0 to proceed.
func (a *Agent) awaitLeaseHolder() time.Duration {
	st := a.local.State
	fresh := st.Kind == resolver.RunHook && st.Step == resolver.Queued && st.Hook != nil && st.Hook.Kind == resolver.Install && !st.Installed
	if !fresh || resolver.Life(a.life.Load()) != resolver.Alive || a.lead.Decided() {
		return 0
	}
	left := LeaseWaitTimeout - a.clk.Since(a.startedAt)
	if left <= 0 {
		a.logf("no leader assigned after %s; starting as a non-leader", LeaseWaitTimeout)
		return 0
	}
	if left > time.Second {
		left = time.Second
	}
	return left
}
