package agent

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

var agentBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "jk-agent-test")
	if err != nil {
		panic(err)
	}
	agentBin = filepath.Join(dir, "jk-agent")
	cmd := exec.Command("go", "build", "-o", agentBin, "github.com/luci1900/jk/cmd/jk-agent")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building jk-agent: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

const testMetadata = `
name: test
containers:
  app: {resource: app-image}
  sidecar: {resource: sidecar-image}
`

const testConfigYAML = `
options:
  greeting: {type: string, default: hello}
  retries: {type: int, default: 3}
  ratio: {type: float, default: 0.5}
  verbose: {type: boolean, default: false}
  nodefault: {type: string}
`

const dispatchScript = `#!/bin/sh
A="$CHARM_DIR/.."
echo "$JUJU_HOOK_NAME" >> "$A/hooks.log"
env | sort > "$A/env-$JUJU_HOOK_NAME"
[ -f "$A/script-$JUJU_HOOK_NAME" ] && . "$A/script-$JUJU_HOOK_NAME"
if [ -f "$A/fail-$JUJU_HOOK_NAME" ]; then echo "boom" >&2; exit 3; fi
exit 0
`

type harness struct {
	t     *testing.T
	kube  *fakeKube
	peb   *fakePebble
	clk   *testingclock.FakeClock
	dir   string
	log   *syncBuffer
	a     *Agent
	cfg   Config
	agent string // agent dir
}

type harnessOpt func(*harness, *Config)

func withMetadata(md string) harnessOpt {
	return func(h *harness, _ *Config) { h.write("charm/metadata.yaml", md) }
}

func newHarness(t *testing.T, opts ...harnessOpt) *harness {
	t.Helper()
	// A short path: unix socket paths are limited to ~104 bytes on macOS.
	dir, err := os.MkdirTemp("/tmp", "jk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	h := &harness{t: t, kube: newFakeKube(), peb: &fakePebble{}, clk: testingclock.NewFakeClock(time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)),
		dir: dir, log: &syncBuffer{}}
	h.agent = filepath.Join(dir, "agents", "unit-app-0")
	h.write("charm/metadata.yaml", testMetadata)
	h.write("charm/config.yaml", testConfigYAML)
	h.write("charm/dispatch", dispatchScript)
	if err := os.Chmod(filepath.Join(h.agent, "charm", "dispatch"), 0o755); err != nil {
		t.Fatal(err)
	}
	h.cfg = Config{
		App: "app", Unit: "app/0", Ordinal: 0, Namespace: "ns", ModelName: "ns", ModelUUID: "uuid-1234",
		DataDir: dir, ContainersDir: filepath.Join(dir, "containers"), Self: agentBin,
		Kube: h.kube, Pebble: h.peb, Clock: h.clk, Log: h.log,
		Getenv: func(k string) (string, bool) {
			switch k {
			case "KUBERNETES_SERVICE_HOST":
				return "10.0.0.1", true
			case "PATH":
				return os.Getenv("PATH"), true
			case "SECRET_POD_ENV":
				return "leak", true
			}
			return "", false
		},
		PodIP: func() string { return "10.1.2.3" },
	}
	for _, o := range opts {
		o(h, &h.cfg)
	}
	h.kube.setHolder("app/1") // another unit leads unless a test says otherwise
	h.a = New(h.cfg)
	return h
}

func (h *harness) write(rel, content string) {
	h.t.Helper()
	p := filepath.Join(h.agent, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) remove(rel string) { _ = os.Remove(filepath.Join(h.agent, rel)) }

func (h *harness) read(rel string) string {
	b, _ := os.ReadFile(filepath.Join(h.agent, rel))
	return string(b)
}

// restart replaces the agent with a fresh one on the same kube and data dir (a process restart).
func (h *harness) restart() {
	h.a = New(h.cfg)
}

func (h *harness) reconcile() time.Duration {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d, err := h.a.Reconcile(ctx)
	if err != nil {
		h.t.Fatalf("reconcile: %v\nlog:\n%s", err, h.log.String())
	}
	return d
}

// hooks returns the hooks run since the last call.
func (h *harness) hooks() []string {
	lines := strings.Fields(h.read("hooks.log"))
	_ = os.Remove(filepath.Join(h.agent, "hooks.log"))
	return lines
}

func (h *harness) wantHooks(want ...string) {
	h.t.Helper()
	got := h.hooks()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		h.t.Fatalf("hooks ran %v, want %v\nlog:\n%s", got, want, h.log.String())
	}
}

func (h *harness) makeLeader() {
	h.t.Helper()
	h.kube.setHolder("app/0")
	if _, err := h.a.Leadership().Renew(context.Background()); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) pollPebble() {
	h.t.Helper()
	if err := h.a.Init(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	h.a.peb.Poll(context.Background())
}

func (h *harness) env(hook string) map[string]string {
	h.t.Helper()
	m := map[string]string{}
	for _, l := range strings.Split(h.read("env-"+hook), "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			m[k] = v
		}
	}
	return m
}

func wantStatus(t *testing.T, got *v1alpha1.WorkloadStatus, state, msg string) {
	t.Helper()
	if got == nil || got.State != state || got.Message != msg {
		t.Fatalf("status = %+v, want %s %q", got, state, msg)
	}
}

func TestFreshNonLeader(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	wake := h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	if wake != DefaultUpdateStatusInterval {
		t.Fatalf("wake = %v, want the update-status interval", wake)
	}
	sp := h.kube.spec()
	st := h.a.State()
	if !st.Installed || !st.Started || st.Leader || st.Kind != resolver.Continue {
		t.Fatalf("state %+v", st)
	}
	wantStatus(t, sp.AgentStatus, AgentIdle, "")
	// start did not set a status: the agent sets unknown.
	wantStatus(t, sp.WorkloadStatus, "unknown", "")
	if sp.Operation == nil || sp.ErrorState != nil {
		t.Fatalf("spec %+v", sp)
	}
	for _, line := range []string{
		"jk-agent: hook install finished exit=0",
		"jk-agent: hook config-changed finished exit=0",
		"jk-agent: hook start finished exit=0",
	} {
		if !strings.Contains(h.log.String(), line+"\n") {
			t.Fatalf("log lacks %q:\n%s", line, h.log.String())
		}
	}
	// Idempotent: nothing more to do.
	h.reconcile()
	h.wantHooks()
}

func TestFreshLeader(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.makeLeader()
	h.reconcile()
	h.wantHooks("install", "leader-elected", "config-changed", "start")
	if !h.a.State().Leader {
		t.Fatal("not recorded as leader")
	}
}

func TestPebbleReadyOrdering(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.makeLeader()
	h.peb.set("app", "boot-a")
	h.pollPebble()
	h.reconcile()
	h.wantHooks("install", "leader-elected", "app-pebble-ready", "config-changed", "start")
	if h.env("app-pebble-ready")["JUJU_WORKLOAD_NAME"] != "app" || h.env("app-pebble-ready")["JUJU_DISPATCH_PATH"] != "hooks/app-pebble-ready" {
		t.Fatalf("pebble-ready env: %v", h.env("app-pebble-ready"))
	}
	if _, ok := h.env("config-changed")["JUJU_WORKLOAD_NAME"]; ok {
		t.Fatal("JUJU_WORKLOAD_NAME set for a non-workload hook")
	}
	// Second container comes up later, and the first restarts.
	h.peb.set("sidecar", "boot-s")
	h.peb.set("app", "boot-b")
	h.pollPebble()
	h.reconcile()
	h.wantHooks("app-pebble-ready", "sidecar-pebble-ready")
	if got := h.kube.spec(); got.Operation == nil || !strings.Contains(string(got.Operation.Raw), "boot-s") {
		t.Fatalf("boot ids not persisted: %s", got.Operation.Raw)
	}
	// Agent restart: boot IDs persisted, so no pebble-ready again.
	h.restart()
	h.pollPebble()
	h.reconcile()
	h.wantHooks()
}

func TestCommitOnSuccessOnly(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "state-set a=1 b=2; open-port 8080/tcp; open-port 9000-9010/udp; application-version-set 1.2.3; status-set maintenance 'working hard'\n")
	h.write("fail-config-changed", "")
	h.write("script-config-changed", "state-set from-config-changed=yes; open-port 1234; status-set blocked 'oops'; application-version-set 9.9.9\n")
	wake := h.reconcile()
	h.wantHooks("install", "config-changed")

	sp := h.kube.spec()
	// install committed...
	if sp.State["a"] != "1" || sp.State["b"] != "2" || sp.WorkloadVersion != "1.2.3" || len(sp.OpenedPorts) != 2 {
		t.Fatalf("install writes not committed: %+v", sp)
	}
	// ...config-changed failed: its buffered writes are dropped, but its status-set stands (as in juju).
	if _, ok := sp.State["from-config-changed"]; ok {
		t.Fatalf("state from a failed hook committed: %v", sp.State)
	}
	if sp.WorkloadVersion != "1.2.3" || len(sp.OpenedPorts) != 2 {
		t.Fatalf("writes from a failed hook committed: %+v", sp)
	}
	wantStatus(t, sp.WorkloadStatus, "blocked", "oops")
	wantStatus(t, sp.AgentStatus, AgentError, "hook failed: config-changed")
	if sp.ErrorState == nil || !strings.Contains(string(sp.ErrorState.Raw), "config-changed") {
		t.Fatalf("error state %+v", sp.ErrorState)
	}
	if wake != DefaultRetryMin {
		t.Fatalf("retry wake = %v, want %v", wake, DefaultRetryMin)
	}
	st := h.a.State()
	if st.Kind != resolver.RunHook || st.Step != resolver.Pending || st.Hook.Kind != resolver.ConfigChanged || !st.Installed {
		t.Fatalf("state %+v", st)
	}
	if !strings.Contains(h.log.String(), "jk-agent: hook config-changed finished exit=3\n") {
		t.Fatalf("log:\n%s", h.log.String())
	}
	if !strings.Contains(h.log.String(), "jk-agent: hook config-changed: boom\n") {
		t.Fatalf("hook output not logged:\n%s", h.log.String())
	}
}

func TestRetryBackoffThenSuccess(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("fail-config-changed", "")
	h.write("script-config-changed", "state-set attempt=$(date +%s%N)\n")
	h.reconcile()
	h.wantHooks("install", "config-changed")

	// Reconciling before the timer does nothing.
	if wake := h.reconcile(); wake <= 0 || wake > DefaultRetryMin {
		t.Fatalf("wake %v", wake)
	}
	h.wantHooks()

	// Backoff: 5s, then 10s, then 20s.
	for i, delay := range []time.Duration{5 * time.Second, 10 * time.Second} {
		h.clk.Step(delay - time.Second)
		h.reconcile()
		h.wantHooks()
		h.clk.Step(time.Second)
		h.reconcile()
		h.wantHooks("config-changed")
		want := delay * 2
		if due := h.a.retryDue.Sub(h.clk.Now()); due != want {
			t.Fatalf("round %d: next retry in %v, want %v", i, due, want)
		}
	}
	// The hook now succeeds: the unit continues and the error clears.
	h.remove("fail-config-changed")
	h.clk.Step(20 * time.Second)
	h.reconcile()
	h.wantHooks("config-changed", "start")
	sp := h.kube.spec()
	wantStatus(t, sp.AgentStatus, AgentIdle, "")
	if sp.ErrorState != nil || sp.State["attempt"] == "" {
		t.Fatalf("spec %+v", sp)
	}
	// The backoff was reset by the success.
	if h.a.retryDelay != DefaultRetryMin {
		t.Fatalf("retry delay %v", h.a.retryDelay)
	}
}

func TestRetryBackoffCapped(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("fail-install", "")
	h.reconcile()
	h.wantHooks("install")
	want := []time.Duration{10, 20, 40, 80, 160, 300, 300}
	for _, w := range want {
		h.clk.Step(h.a.retryDue.Sub(h.clk.Now()))
		h.reconcile()
		if got := h.a.retryDue.Sub(h.clk.Now()); got != w*time.Second {
			t.Fatalf("next retry in %v, want %v", got, w*time.Second)
		}
		h.wantHooks("install")
	}
}

func TestNoAutoRetryWhenDisabled(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.kube.model[ModelConfigPrefix+"automatically-retry-hooks"] = "false"
	h.write("fail-install", "")
	h.reconcile()
	h.wantHooks("install")
	h.clk.Step(time.Hour)
	h.reconcile()
	h.wantHooks()
	if !h.a.retryDue.IsZero() {
		t.Fatal("retry timer started although disabled")
	}
	// Turning it on again retries.
	h.kube.model[ModelConfigPrefix+"automatically-retry-hooks"] = "true"
	h.reconcile()
	h.clk.Step(DefaultRetryMin)
	h.reconcile()
	h.wantHooks("install")
}

func TestRestartNoReinstall(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	h.restart()
	h.reconcile()
	h.wantHooks()
	if !h.a.State().Started {
		t.Fatal("state lost")
	}
	// A restart in a hook error resumes the retry (the pending hook is reported and retried).
	h2 := newHarness(t)
	h2.write("fail-install", "")
	h2.reconcile()
	h2.wantHooks("install")
	h2.remove("fail-install")
	h2.restart()
	h2.reconcile() // pending hook: reports, starts the timer
	h2.wantHooks()
	h2.clk.Step(DefaultRetryMin)
	h2.reconcile()
	h2.wantHooks("install", "config-changed", "start")
}

func TestRestartOfCrashedAgentMidHook(t *testing.T) {
	t.Parallel()
	// UnitData says install is done (hook ran, commit not recorded): the agent commits without rerunning.
	h := newHarness(t)
	h.reconcile()
	h.hooks()
	ctx := context.Background()
	st := resolver.Initial().Prepared(resolver.HookInfo{Kind: resolver.Install}, "x").Executed(resolver.HookInfo{Kind: resolver.Install}, false)
	h2 := newHarness(t)
	h2.kube = newFakeKube()
	h2.cfg.Kube = h2.kube
	store := NewUnitStore(h2.kube, h2.clk)
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.SetState(ctx, st); err != nil {
		t.Fatal(err)
	}
	h2.restart()
	h2.reconcile()
	h2.wantHooks("config-changed", "start")
	if !h2.a.State().Installed {
		t.Fatal("not installed")
	}
}

func TestConfigChange(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-config-changed", "config-get --format=json > \"$A/config.json\"\n")
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	want := `{"greeting":"hello","ratio":0.5,"retries":3,"verbose":false}`
	if got := strings.TrimSpace(h.read("config.json")); got != want {
		t.Fatalf("config-get = %s, want %s", got, want)
	}
	h.kube.setConfig(map[string]string{"greeting": "hi", "retries": "7", "verbose": "true", "nodefault": "x", "unknown": "y"})
	h.reconcile()
	h.wantHooks("config-changed")
	want = `{"greeting":"hi","nodefault":"x","ratio":0.5,"retries":7,"verbose":true}`
	if got := strings.TrimSpace(h.read("config.json")); got != want {
		t.Fatalf("config-get = %s, want %s", got, want)
	}
	// Same config again: no hook. Invalid values are ignored (default kept) without hooks looping.
	h.reconcile()
	h.wantHooks()
	h.kube.setConfig(map[string]string{"retries": "many"})
	h.reconcile()
	h.wantHooks("config-changed")
	if !strings.Contains(h.log.String(), "ignoring invalid config value retries") {
		t.Fatalf("log:\n%s", h.log.String())
	}
	// A hash restart persists: no config-changed after a restart.
	h.restart()
	h.reconcile()
	h.wantHooks()
}

func TestTrustChangeRunsConfigChanged(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	h.kube.setTrust(v1alpha1.TrustCluster)
	h.reconcile()
	h.wantHooks("config-changed")
	h.reconcile()
	h.wantHooks()
	h.kube.setTrust(v1alpha1.TrustNone)
	h.reconcile()
	h.wantHooks("config-changed")
}

func TestUpdateStatusTiming(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.kube.model[ModelConfigPrefix+"update-status-hook-interval"] = "1m"
	wake := h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	if wake != time.Minute {
		t.Fatalf("wake %v", wake)
	}
	h.clk.Step(59 * time.Second)
	if wake := h.reconcile(); wake != time.Second {
		t.Fatalf("wake %v, want 1s", wake)
	}
	h.wantHooks()
	h.clk.Step(time.Second)
	h.reconcile()
	h.wantHooks("update-status")
	// update-status does not show as "executing" and leaves the agent idle.
	wantStatus(t, h.kube.spec().AgentStatus, AgentIdle, "")
	h.reconcile()
	h.wantHooks()
	// Ticks are not queued up: a long gap gives one hook.
	h.clk.Step(10 * time.Minute)
	h.reconcile()
	h.wantHooks("update-status")
	// Interval change takes effect.
	h.kube.model[ModelConfigPrefix+"update-status-hook-interval"] = "10s"
	h.clk.Step(10 * time.Second)
	h.reconcile()
	h.wantHooks("update-status")
	// Bare model key and seconds are accepted too.
	delete(h.kube.model, ModelConfigPrefix+"update-status-hook-interval")
	h.kube.model["update-status-hook-interval"] = "30"
	h.clk.Step(30 * time.Second)
	h.reconcile()
	h.wantHooks("update-status")
}

func TestUpdateStatusFailureReports(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.kube.model[ModelConfigPrefix+"update-status-hook-interval"] = "1m"
	h.reconcile()
	h.hooks()
	h.write("fail-update-status", "")
	h.clk.Step(time.Minute)
	h.reconcile()
	h.wantHooks("update-status")
	wantStatus(t, h.kube.spec().AgentStatus, AgentError, "hook failed: update-status")
}

func TestLeadershipGainedAndLost(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	// Another unit holds the lease.
	h.kube.setHolder("app/1")
	h.reconcile()
	h.wantHooks()
	// The operator assigns us; the first renewal confers leadership.
	h.makeLeader()
	h.reconcile()
	h.wantHooks("leader-elected")
	h.reconcile()
	h.wantHooks()
	// is-leader is true only with >= 30s left: advance 31s without renewing.
	h.clk.Step(31 * time.Second)
	if h.a.Leadership().IsLeader() {
		t.Fatal("still leader with less than 30s left")
	}
	h.reconcile()
	if h.a.State().Leader {
		t.Fatal("leadership not given up")
	}
	// Renewed: leader again, hook again.
	if _, err := h.a.Leadership().Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.reconcile()
	h.wantHooks("leader-elected")
	// Lease moves away.
	h.kube.setHolder("app/2")
	h.reconcile()
	if h.a.State().Leader || h.a.Leadership().IsLeader() {
		t.Fatal("leadership kept after the lease moved")
	}
}

func TestIsLeaderToolAndApplicationStatus(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "is-leader --format=json > \"$A/leader-install\"; status-set --application active 'app up' 2> \"$A/appstatus-install\"; true\n")
	h.write("script-config-changed", "is-leader > \"$A/leader-cc\"; status-set --application=true blocked x 2> \"$A/err-cc\"; status-get --application --include-data --format=json > \"$A/appget\"; status-get --format=json --include-data > \"$A/uget\"\n")
	h.makeLeader()
	h.reconcile()
	h.wantHooks("install", "leader-elected", "config-changed", "start")
	if strings.TrimSpace(h.read("leader-install")) != "true" || strings.TrimSpace(h.read("leader-cc")) != "True" {
		t.Fatalf("is-leader: %q %q", h.read("leader-install"), h.read("leader-cc"))
	}
	h.kube.mu.Lock()
	ad := h.kube.appData
	h.kube.mu.Unlock()
	if ad == nil || ad.Status == nil || ad.Status.State != "blocked" || ad.Status.Message != "x" {
		t.Fatalf("app status %+v", ad)
	}
	if !strings.Contains(h.read("appget"), `"application-status"`) || !strings.Contains(h.read("appget"), `"status":"blocked"`) {
		t.Fatalf("status-get --application: %s", h.read("appget"))
	}
	if !strings.Contains(h.read("uget"), `"message"`) {
		t.Fatalf("status-get: %s", h.read("uget"))
	}
}

func TestNonLeaderCannotSetApplicationStatus(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "status-set --application active x 2> \"$A/err\"; echo $? > \"$A/code\"\n")
	h.reconcile()
	if !strings.Contains(h.read("err"), "this unit is not the leader") || strings.TrimSpace(h.read("code")) != "1" {
		t.Fatalf("err %q code %q", h.read("err"), h.read("code"))
	}
	h.kube.mu.Lock()
	defer h.kube.mu.Unlock()
	if h.kube.appData != nil || h.kube.appWrites != 0 {
		t.Fatal("AppData written by a non-leader")
	}
}

func TestHookEnvironment(t *testing.T) {
	h := newHarness(t)
	h.kube.model[ModelConfigPrefix+"juju-http-proxy"] = "http://proxy:3128"
	h.kube.model["juju-no-proxy"] = "localhost"
	t.Setenv("INHERITED_FROM_AGENT", "yes")
	h.reconcile()
	env := h.env("install")
	charmDir := filepath.Join(h.agent, "charm")
	for k, want := range map[string]string{
		"JUJU_DISPATCH_PATH": "hooks/install", "JUJU_HOOK_NAME": "install", "JUJU_MODEL_NAME": "ns", "JUJU_MODEL_UUID": "uuid-1234",
		"JUJU_UNIT_NAME": "app/0", "JUJU_VERSION": "3.6.29", "JUJU_CHARM_DIR": charmDir, "CHARM_DIR": charmDir,
		"JUJU_AGENT_SOCKET_NETWORK": "unix", "JUJU_AGENT_SOCKET_ADDRESS": filepath.Join(h.agent, "agent.socket"),
		"KUBERNETES_SERVICE_HOST": "10.0.0.1", "JUJU_CHARM_HTTP_PROXY": "http://proxy:3128", "JUJU_CHARM_NO_PROXY": "localhost",
		"JUJU_CHARM_HTTPS_PROXY": "", "LANG": "C.UTF-8", "DEBIAN_FRONTEND": "noninteractive",
	} {
		if got, ok := env[k]; !ok || got != want {
			t.Errorf("%s = %q (set %v), want %q", k, got, ok, want)
		}
	}
	if env["JUJU_CONTEXT_ID"] == "" {
		t.Error("JUJU_CONTEXT_ID missing")
	}
	for _, k := range []string{"JUJU_API_ADDRESSES", "JUJU_MACHINE_ID", "JUJU_PRINCIPAL_UNIT", "JUJU_AVAILABILITY_ZONE", "CLOUD_API_VERSION",
		"JUJU_CHARM_FTP_PROXY", "JUJU_CHARM_TRACE_CONFIG_HTTP", "JUJU_CHARM_TRACE_CONFIG_GRPC", "JUJU_CHARM_TRACE_CONFIG_CA_CERT"} {
		if _, ok := env[k]; !ok {
			t.Errorf("%s not set", k)
		}
	}
	// From scratch: not inherited, container names and the pebble socket are not in hook environments.
	for _, k := range []string{"INHERITED_FROM_AGENT", "SECRET_POD_ENV", "JUJU_CONTAINER_NAMES", "PEBBLE_SOCKET"} {
		if _, ok := env[k]; ok {
			t.Errorf("%s leaked into the hook environment", k)
		}
	}
	if !strings.HasPrefix(env["PATH"], filepath.Join(h.dir, "tools", "unit-app-0")+":") {
		t.Errorf("PATH = %q", env["PATH"])
	}
	// The tools exist as links to the agent binary.
	if target, err := os.Readlink(filepath.Join(h.dir, "tools", "unit-app-0", "config-get")); err != nil || target != agentBin {
		t.Errorf("tool link %q %v", target, err)
	}
}

func TestJujuVersionFromAssumes(t *testing.T) {
	t.Parallel()
	h := newHarness(t, withMetadata("name: x\nassumes:\n  - juju >= 3.5.1, < 4\n"))
	h.reconcile()
	if h.env("install")["JUJU_VERSION"] != "3.6.29" {
		t.Fatalf("%v", h.env("install")["JUJU_VERSION"])
	}
	h = newHarness(t, withMetadata("name: x\nassumes:\n  - juju >= 4.0\n"))
	h.reconcile()
	if h.env("install")["JUJU_VERSION"] != "4.0.0" {
		t.Fatalf("%v", h.env("install")["JUJU_VERSION"])
	}
}

func TestTerminationRunsStopAndRemove(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.makeLeader()
	h.reconcile()
	h.hooks()
	h.a.Terminate()
	h.reconcile()
	h.wantHooks("stop", "remove")
	select {
	case <-h.a.Done():
	default:
		t.Fatal("agent not done")
	}
	sp := h.kube.spec()
	wantStatus(t, sp.WorkloadStatus, "terminated", "")
	st := h.a.State()
	if !st.Stopped || !st.Removed || st.Leader {
		t.Fatalf("state %+v", st)
	}
	// Reconciling a dead agent is a no-op.
	h.reconcile()
	h.wantHooks()
	// A restarted agent that finds the unit removed stays idle.
	h.restart()
	h.reconcile()
	h.wantHooks()
}

func TestTerminationStatuses(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.hooks()
	h.write("script-stop", "cp \"$A/../../../placeholder\" /dev/null 2>/dev/null; true\n")
	h.a.Terminate()
	h.reconcile()
	h.wantHooks("stop", "remove")
	var states []string
	for _, sp := range h.kube.unitHistory {
		if sp.WorkloadStatus != nil {
			states = append(states, sp.WorkloadStatus.State+":"+sp.WorkloadStatus.Message)
		}
	}
	joined := strings.Join(states, "|")
	for _, want := range []string{"maintenance:stopping charm software", "maintenance:|", "maintenance:cleaning up prior to charm deletion", "terminated:"} {
		if !strings.Contains(joined+"|", want) {
			t.Errorf("workload status history lacks %q: %s", want, joined)
		}
	}
}

func TestTerminationBeforeInstall(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.a.Terminate()
	h.reconcile()
	h.wantHooks()
	select {
	case <-h.a.Done():
	default:
		t.Fatal("agent not done")
	}
}

func TestWillComeBack(t *testing.T) {
	t.Parallel()
	scale := func(n int32) *int32 { return &n }
	now := metav1Now()
	tests := []struct {
		name    string
		app     *v1alpha1.Application
		ordinal int
		want    bool
	}{
		{"within scale", &v1alpha1.Application{Spec: v1alpha1.ApplicationSpec{Scale: scale(2)}}, 1, true},
		{"scaled down past ordinal", &v1alpha1.Application{Spec: v1alpha1.ApplicationSpec{Scale: scale(1)}}, 1, false},
		{"scaled to zero", &v1alpha1.Application{Spec: v1alpha1.ApplicationSpec{Scale: scale(0)}}, 0, false},
		{"being deleted", &v1alpha1.Application{ObjectMeta: metaWithDeletion(now), Spec: v1alpha1.ApplicationSpec{Scale: scale(3)}}, 0, false},
		{"default scale", &v1alpha1.Application{}, 0, true},
		{"default scale, ordinal 1", &v1alpha1.Application{}, 1, false},
		{"gone", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WillComeBack(tt.app, tt.ordinal); got != tt.want {
				t.Fatalf("WillComeBack = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShutdownKillsHookAndRequeues(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "echo started > \"$A/started\"; sleep 30\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := h.a.Reconcile(ctx); done <- err }()
	deadline := time.Now().Add(10 * time.Second)
	for h.read("started") == "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if h.read("started") == "" {
		t.Fatal("hook did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("reconcile did not report the cancellation")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("hook not killed")
	}
	st := h.kube.spec()
	var op resolver.State
	if err := jsonUnmarshal(st.Operation.Raw, &op); err != nil {
		t.Fatal(err)
	}
	if op.Kind != resolver.RunHook || op.Step != resolver.Queued || op.Hook.Kind != resolver.Install {
		t.Fatalf("state after kill %+v", op)
	}
	h.hooks()
	// Next start: runs install again without an error status in between.
	h.remove("script-install")
	h.restart()
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
}

func TestSaveFailureIsAnError(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.hooks()
	h.kube.setConfig(map[string]string{"greeting": "x"})
	h.kube.mu.Lock()
	h.kube.failSaves = 1
	h.kube.mu.Unlock()
	ctx := context.Background()
	if _, err := h.a.Reconcile(ctx); err == nil {
		t.Fatal("expected the save error")
	}
	// The in-memory state did not advance past what was persisted, so the next reconcile redoes the hook.
	h.reconcile()
	h.wantHooks("config-changed")
}

func TestStateSizeLimit(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "head -c 2000000 /dev/zero | tr '\\0' x > \"$A/big\"; state-set --file=\"$A/bigfile\" 2>/dev/null; echo \"big: $(cat \"$A/big\")\" > \"$A/bigfile\"; state-set --file \"$A/bigfile\"\n")
	h.reconcile()
	h.wantHooks("install")
	wantStatus(t, h.kube.spec().AgentStatus, AgentError, "hook failed: install")
	if len(h.kube.spec().State) != 0 {
		t.Fatal("oversized state committed")
	}
	if !strings.Contains(h.log.String(), "over the limit") {
		t.Fatalf("log:\n%s", h.log.String())
	}
}

func TestResolvedAnnotation(t *testing.T) {
	t.Parallel()
	t.Run("retry", func(t *testing.T) {
		h := newHarness(t)
		h.write("fail-install", "")
		h.reconcile()
		h.wantHooks("install")
		h.remove("fail-install")
		h.kube.resolved = "retry"
		h.reconcile()
		h.wantHooks("install", "config-changed", "start")
		if h.kube.cleared != 1 || h.kube.resolved != "" {
			t.Fatalf("annotation not cleared: %d %q", h.kube.cleared, h.kube.resolved)
		}
	})
	t.Run("no-retry skips the hook", func(t *testing.T) {
		h := newHarness(t)
		h.write("fail-install", "")
		h.reconcile()
		h.wantHooks("install")
		h.kube.resolved = "no-retry"
		h.reconcile()
		h.wantHooks("config-changed", "start") // install skipped, the rest goes on
		if !h.a.State().Installed {
			t.Fatal("skipped install not recorded")
		}
	})
}

func TestMissingDispatchIsASkippedHook(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	os.Remove(filepath.Join(h.agent, "charm", "dispatch"))
	h.reconcile()
	if st := h.a.State(); !st.Started {
		t.Fatalf("state %+v", st)
	}
	if !strings.Contains(h.log.String(), "jk-agent: hook install finished exit=0") || !strings.Contains(h.log.String(), "no dispatch script") {
		t.Fatalf("log:\n%s", h.log.String())
	}
}

func TestStatusSetInInstallIsNotOverwritten(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "status-set waiting 'for db'\n")
	h.write("fail-config-changed", "")
	h.reconcile()
	h.hooks()
	wantStatus(t, h.kube.spec().WorkloadStatus, "waiting", "for db")
	if !h.a.State().StatusSet {
		t.Fatal("StatusSet not recorded")
	}
	// After start, a charm that set a status keeps it (no "unknown").
	h.remove("fail-config-changed")
	h.clk.Step(DefaultRetryMin)
	h.reconcile()
	h.wantHooks("config-changed", "start")
	wantStatus(t, h.kube.spec().WorkloadStatus, "waiting", "for db")
}

func TestInstallSetsInstallingMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "status-get --format=json --include-data > \"$A/during\"\n")
	h.reconcile()
	if !strings.Contains(h.read("during"), "installing charm software") {
		t.Fatalf("status during install: %s", h.read("during"))
	}
	first := h.kube.unitHistory[0]
	wantStatus(t, first.AgentStatus, AgentAllocating, "")
	wantStatus(t, first.WorkloadStatus, "waiting", "installing agent")
}

func TestAgentStatusWhileExecuting(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	var msgs []string
	for _, sp := range h.kube.unitHistory {
		if sp.AgentStatus != nil {
			msgs = append(msgs, sp.AgentStatus.State+":"+sp.AgentStatus.Message)
		}
	}
	joined := strings.Join(msgs, "|")
	for _, want := range []string{"allocating:", "executing:running install hook", "idle:", "executing:running config-changed hook", "executing:running start hook"} {
		if !strings.Contains(joined, want) {
			t.Errorf("agent status history lacks %q: %s", want, joined)
		}
	}
}

func TestApplicationMissingWaits(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.hooks()
	h.kube.mu.Lock()
	h.kube.app = nil
	h.kube.mu.Unlock()
	if wake := h.reconcile(); wake != 5*time.Second {
		t.Fatalf("wake %v", wake)
	}
	h.wantHooks()
	// Application gone and the unit terminating: stop and remove still run.
	h.a.Terminate()
	h.reconcile()
	h.wantHooks("stop", "remove")
}

func TestHookToolsFallbackAndErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", `relation-ids database-peers > "$A/relids"
secret-get --label nothing 2> "$A/secret" ; echo $? > "$A/secretcode"
action-get 2> "$A/action"; echo $? > "$A/actioncode"
unit-get private-address > "$A/ip"
network-get db --ingress-address > "$A/net"
juju-log -l WARNING 'hello from the charm'
`)
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	if strings.TrimSpace(h.read("ip")) != "10.1.2.3" || strings.TrimSpace(h.read("net")) != "10.1.2.3" {
		t.Fatalf("ip %q net %q", h.read("ip"), h.read("net"))
	}
	if !strings.Contains(h.read("secret"), "not found") || !strings.Contains(h.read("action"), "not running an action") || strings.TrimSpace(h.read("actioncode")) != "1" {
		t.Fatalf("secret %q action %q", h.read("secret"), h.read("action"))
	}
	if !strings.Contains(h.log.String(), "jk-agent: juju-log WARNING: hello from the charm\n") {
		t.Fatalf("log:\n%s", h.log.String())
	}
	if strings.TrimSpace(h.read("relids")) != "" {
		t.Fatalf("relids %q", h.read("relids"))
	}
}

func TestAppDataOnlyWrittenByLeaderLogic(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", "status-set --application active x 2>/dev/null; true\n")
	h.reconcile()
	h.kube.mu.Lock()
	n := h.kube.appWrites
	h.kube.mu.Unlock()
	if n != 0 {
		t.Fatalf("non-leader wrote AppData %d times", n)
	}
}

func TestFreshUnitWaitsForLeaseHolder(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.kube.mu.Lock()
	h.kube.lease = nil // no Lease yet
	h.kube.mu.Unlock()
	if wake := h.reconcile(); wake <= 0 || wake > time.Second {
		t.Fatalf("wake %v", wake)
	}
	h.wantHooks()
	// Lease exists but nobody holds it: still waiting.
	h.kube.setHolder("")
	h.clk.Step(10 * time.Second)
	h.reconcile()
	h.wantHooks()
	// Named as holder: the reconcile renews at once (not at the next tick) and leader-elected follows install.
	h.kube.setHolder("app/0")
	h.reconcile()
	h.wantHooks("install", "leader-elected", "config-changed", "start")
}

func TestFreshUnitWaitTimesOut(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.kube.setHolder("")
	h.reconcile()
	h.wantHooks()
	h.clk.Step(LeaseWaitTimeout)
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	if !strings.Contains(h.log.String(), "starting as a non-leader") {
		t.Fatal(h.log.String())
	}
}

func TestInstalledUnitDoesNotWaitForLease(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.reconcile()
	h.hooks()
	h.kube.setHolder("")
	h.restart()
	h.reconcile()
	h.wantHooks()
	h.a.Terminate()
	h.reconcile()
	h.wantHooks("stop", "remove")
}
