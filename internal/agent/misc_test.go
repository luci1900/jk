package agent

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

func TestEffectiveConfig(t *testing.T) {
	t.Parallel()
	opts := map[string]ConfigOption{
		"s":    {Type: "string", Default: "d"},
		"i":    {Type: "int", Default: float64(3)},
		"f":    {Type: "float", Default: 0.25},
		"b":    {Type: "boolean", Default: true},
		"sec":  {Type: "secret"},
		"none": {Type: "string"},
		"fi":   {Type: "float", Default: float64(2)},
		"ds":   {Type: "int", Default: "7"},
	}
	tests := []struct {
		name string
		spec map[string]string
		want map[string]any
		bad  int
	}{
		{"defaults only", nil, map[string]any{"s": "d", "i": int64(3), "f": 0.25, "b": true, "fi": float64(2), "ds": int64(7)}, 0},
		{"overrides coerce", map[string]string{"s": "x", "i": " 10 ", "f": "1.5", "b": "false", "sec": "secret:abc", "none": "v"},
			map[string]any{"s": "x", "i": int64(10), "f": 1.5, "b": false, "sec": "secret:abc", "none": "v", "fi": float64(2), "ds": int64(7)}, 0},
		{"invalid values fall back to the default", map[string]string{"i": "x", "f": "y", "b": "maybe"},
			map[string]any{"s": "d", "i": int64(3), "f": 0.25, "b": true, "fi": float64(2), "ds": int64(7)}, 3},
		{"unknown options are ignored", map[string]string{"nope": "1"}, map[string]any{"s": "d", "i": int64(3), "f": 0.25, "b": true, "fi": float64(2), "ds": int64(7)}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, bad := EffectiveConfig(opts, tt.spec)
			if !reflect.DeepEqual(got, tt.want) || len(bad) != tt.bad {
				t.Fatalf("got %#v bad %v, want %#v bad %d", got, bad, tt.want, tt.bad)
			}
		})
	}
	// Equal configs hash equally; different ones do not.
	a, _ := EffectiveConfig(opts, nil)
	b, _ := EffectiveConfig(opts, map[string]string{"s": "d"})
	c, _ := EffectiveConfig(opts, map[string]string{"s": "e"})
	if ConfigHash(a) != ConfigHash(b) || ConfigHash(a) == ConfigHash(c) {
		t.Fatal("hash")
	}
	// Non-string defaults of unexpected types are rejected quietly.
	got, _ := EffectiveConfig(map[string]ConfigOption{"x": {Type: "boolean", Default: 3}, "y": {Type: "int", Default: int64(2)}, "z": {Type: "float", Default: 3}, "w": {Type: "", Default: true}}, nil)
	if _, ok := got["x"]; ok || got["y"] != int64(2) || got["z"] != float64(3) || got["w"] != "true" {
		t.Fatalf("%#v", got)
	}
}

func TestJujuVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		assumes any
		want    string
	}{
		{"none", nil, JujuVersion3},
		{"range containing 3.6", []any{"juju >= 3.5.1, < 4"}, JujuVersion3},
		{"below 4", []any{"juju < 4"}, JujuVersion3},
		{"needs 3.7", []any{"juju >= 3.7"}, JujuVersion4},
		{"needs 4", []any{"juju >= 4.0"}, JujuVersion4},
		{"excludes 3 with !=", []any{"juju != 3.6.29"}, JujuVersion4},
		{"exact", []any{"juju == 3.6.29"}, JujuVersion3},
		{"other features present", []any{"k8s-api", "juju >= 3.1"}, JujuVersion3},
		{"any-of satisfied", []any{map[string]any{"any-of": []any{"juju >= 4", "juju >= 3.1, < 3.5"}}}, JujuVersion4},
		{"any-of satisfied by 3", []any{map[string]any{"any-of": []any{"juju >= 4", "juju >= 3.1"}}}, JujuVersion3},
		{"all-of", []any{map[string]any{"all-of": []any{"juju >= 3.1", "juju < 3.6"}}}, JujuVersion4},
		{"single string", "juju >= 4", JujuVersion4},
		{"unparsable constraint is ignored", []any{"juju ~~ 3"}, JujuVersion3},
		{"feature named juju-something", []any{"juju-foo"}, JujuVersion3},
		{"less or equal and greater", []any{"juju <= 3.6.29, > 3.0"}, JujuVersion3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JujuVersion(tt.assumes); got != tt.want {
				t.Fatalf("JujuVersion = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestLoadCharm(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Nothing on disk and nothing recorded: empty but valid.
	c, err := LoadCharm(dir, nil)
	if err != nil || len(c.Options) != 0 || len(c.Containers) != 0 {
		t.Fatalf("%+v %v", c, err)
	}
	// Falls back to the operator's record.
	resolved := &v1alpha1.ResolvedCharm{
		Metadata:     &v1alpha1.JSON{Raw: []byte(`{"containers":{"b":{},"a":{}},"assumes":["juju >= 4"]}`)},
		ConfigSchema: &v1alpha1.JSON{Raw: []byte(`{"options":{"x":{"type":"int","default":2}}}`)},
	}
	c, err = LoadCharm(dir, resolved)
	if err != nil || !reflect.DeepEqual(c.Containers, []string{"a", "b"}) || c.Options["x"].Type != "int" || JujuVersion(c.Assumes) != JujuVersion4 {
		t.Fatalf("%+v %v", c, err)
	}
	// Files win.
	os.WriteFile(filepath.Join(dir, "metadata.yaml"), []byte("containers:\n  c: {}\n"), 0o644)
	c, _ = LoadCharm(dir, resolved)
	if !reflect.DeepEqual(c.Containers, []string{"c"}) {
		t.Fatalf("%+v", c)
	}
	// Broken files are errors.
	os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("options: [unclosed"), 0o644)
	if _, err := LoadCharm(dir, nil); err == nil {
		t.Fatal("broken config.yaml accepted")
	}
	os.WriteFile(filepath.Join(dir, "metadata.yaml"), []byte(":::"), 0o644)
	if _, err := LoadCharm(dir, nil); err == nil {
		t.Fatal("broken metadata.yaml accepted")
	}
	// An unreadable path (a directory) is an error.
	d2 := t.TempDir()
	os.Mkdir(filepath.Join(d2, "metadata.yaml"), 0o755)
	if _, err := LoadCharm(d2, nil); err == nil {
		t.Fatal("directory as metadata accepted")
	}
}

func TestIdentityFromEnv(t *testing.T) {
	t.Parallel()
	env := map[string]string{"JK_POD_NAME": "pg-2", "JK_NAMESPACE": "db", "JK_MODEL_UUID": "u", "JUJU_CONTAINER_NAMES": "a, b,,"}
	id, err := IdentityFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if id.App != "pg" || id.Ordinal != 2 || id.Namespace != "db" || id.ModelName != "db" || !reflect.DeepEqual(id.Containers, []string{"a", "b"}) {
		t.Fatalf("%+v", id)
	}
	env["JK_APP"], env["JK_MODEL_NAME"] = "other", "m"
	id, _ = IdentityFromEnv(func(k string) string { return env[k] })
	if id.App != "other" || id.ModelName != "m" {
		t.Fatalf("%+v", id)
	}
	for _, bad := range []map[string]string{{}, {"JK_POD_NAME": "nodash", "JK_NAMESPACE": "x"}, {"JK_POD_NAME": "a-b", "JK_NAMESPACE": "x"}, {"JK_POD_NAME": "a-1"}} {
		bad := bad
		if _, err := IdentityFromEnv(func(k string) string { return bad[k] }); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
	if Scheme() == nil {
		t.Fatal("scheme")
	}
}

func TestLeadershipRun(t *testing.T) {
	t.Parallel()
	kube := newFakeKube()
	kube.setHolder("app/0")
	clk := testingclock.NewFakeClock(time.Now())
	l := NewLeadership(kube, "app/0", clk)
	var changes atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.Run(ctx, func() { changes.Add(1) }, func(string, ...any) {})
		close(done)
	}()
	waitFor(t, func() bool { return l.IsLeader() && changes.Load() == 1 }, "first renewal")
	if l.Holder() != "app/0" {
		t.Fatal(l.Holder())
	}
	// Ticks renew every 10s.
	for i := 1; i <= 3; i++ {
		waitFor(t, clk.HasWaiters, "timer")
		clk.Step(LeaseRenewInterval)
		n := i + 1
		waitFor(t, func() bool { kube.mu.Lock(); defer kube.mu.Unlock(); return kube.renewals >= n }, "renewal")
	}
	// API failures: leadership lapses once less than 30s remain, and the loop reports that as a change.
	kube.mu.Lock()
	kube.renewErr = errors.New("api down")
	kube.mu.Unlock()
	for i := 0; i < 4; i++ {
		waitFor(t, clk.HasWaiters, "timer")
		clk.Step(LeaseRenewInterval)
	}
	waitFor(t, func() bool { return !l.IsLeader() && changes.Load() >= 2 }, "lapse")
	// Recovery.
	kube.mu.Lock()
	kube.renewErr = nil
	kube.mu.Unlock()
	waitFor(t, clk.HasWaiters, "timer")
	clk.Step(LeaseRenewInterval)
	waitFor(t, l.IsLeader, "recovery")
	cancel()
	<-done
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLeadershipObserve(t *testing.T) {
	t.Parallel()
	kube := newFakeKube()
	clk := testingclock.NewFakeClock(time.Now())
	l := NewLeadership(kube, "app/0", clk)
	// No lease at all: not leader; Renew reports the error.
	if _, err := l.Renew(context.Background()); err == nil || l.IsLeader() {
		t.Fatalf("err %v leader %v", err, l.IsLeader())
	}
	// Named but not yet renewed: not leader.
	kube.setHolder("app/0")
	lease, _ := kube.Lease(context.Background())
	l.Observe(lease)
	if l.IsLeader() {
		t.Fatal("leader before renewing")
	}
	if changed, err := l.Renew(context.Background()); err != nil || !changed || !l.IsLeader() {
		t.Fatalf("changed %v err %v leader %v", changed, err, l.IsLeader())
	}
	// A custom duration is honoured: 40s lease gives 10s of leadership beyond the guarantee.
	d := int32(40)
	kube.lease.Spec.LeaseDurationSeconds = &d
	l.Renew(context.Background())
	clk.Step(9 * time.Second)
	if !l.IsLeader() {
		t.Fatal("lost leadership early")
	}
	clk.Step(2 * time.Second)
	if l.IsLeader() {
		t.Fatal("kept leadership with less than 30s left")
	}
	// Holder cleared.
	kube.setHolder("")
	lease, _ = kube.Lease(context.Background())
	l.Observe(lease)
	if l.Holder() != "" {
		t.Fatal("holder")
	}
}

func TestUnitStore(t *testing.T) {
	t.Parallel()
	kube := newFakeKube()
	clk := testingclock.NewFakeClock(time.Now())
	s := NewUnitStore(kube, clk)
	ctx := context.Background()
	if err := s.Update(ctx, func(*v1alpha1.UnitDataSpec) {}); err == nil {
		t.Fatal("update before load")
	}
	st, err := s.Load(ctx)
	if err != nil || st.Kind != resolver.RunHook || st.Hook.Kind != resolver.Install {
		t.Fatalf("%+v %v", st, err)
	}
	if kube.unit != nil {
		t.Fatal("Load created UnitData")
	}
	// A failed save leaves the in-memory copy alone.
	kube.failSaves = 1
	if err := s.SetState(ctx, resolver.State{Kind: resolver.Continue, Step: resolver.Pending, Installed: true}); err == nil {
		t.Fatal("expected failure")
	}
	if s.Spec().Operation != nil {
		t.Fatal("in-memory changed despite failure")
	}
	if err := s.SetState(ctx, resolver.State{Kind: resolver.Continue, Step: resolver.Pending, Installed: true}); err != nil {
		t.Fatal(err)
	}
	// Reload from the API server: same state.
	s2 := NewUnitStore(kube, clk)
	st, err = s2.Load(ctx)
	if err != nil || !st.Installed {
		t.Fatalf("%+v %v", st, err)
	}
	// Agent status is only written when it changes.
	saves := kube.saves
	if err := s2.SetAgentStatus(ctx, AgentIdle, ""); err != nil {
		t.Fatal(err)
	}
	if err := s2.SetAgentStatus(ctx, AgentIdle, ""); err != nil || kube.saves != saves+1 {
		t.Fatalf("saves %d -> %d", saves, kube.saves)
	}
	if s2.Now() != clk.Now() {
		t.Fatal("now")
	}
	// Corrupt or invalid saved state is an error, not a silent reset.
	kube.unit.Operation = &v1alpha1.JSON{Raw: []byte(`{"op":"weird","opStep":"queued"}`)}
	if _, err := NewUnitStore(kube, clk).Load(ctx); err == nil {
		t.Fatal("invalid state accepted")
	}
	kube.unit.Operation = &v1alpha1.JSON{Raw: []byte(`[`)}
	if _, err := NewUnitStore(kube, clk).Load(ctx); err == nil {
		t.Fatal("corrupt state accepted")
	}
	// No operation recorded (e.g. UnitData made by hand): initial.
	kube.unit.Operation = nil
	if st, err := NewUnitStore(kube, clk).Load(ctx); err != nil || st.Hook.Kind != resolver.Install {
		t.Fatalf("%+v %v", st, err)
	}
}

func TestSocketPebble(t *testing.T) {
	t.Parallel()
	dir, err := os.MkdirTemp("/tmp", "jkp")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("unix", filepath.Join(dir, "app", "pebble.socket"))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"type":"sync","status-code":200,"result":{"version":"v1.32.1","boot-id":"abc-123"}}`
	status := http.StatusOK
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/system-info" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	})}
	go srv.Serve(l)
	defer srv.Close()

	p := SocketPebble{Dir: dir}
	id, err := p.BootID(context.Background(), "app")
	if err != nil || id != "abc-123" {
		t.Fatalf("%q %v", id, err)
	}
	if _, err := p.BootID(context.Background(), "other"); err == nil {
		t.Fatal("missing socket answered")
	}
	status = http.StatusInternalServerError
	if _, err := p.BootID(context.Background(), "app"); err == nil {
		t.Fatal("500 accepted")
	}
	status, body = http.StatusOK, `{"result":{}}`
	if _, err := p.BootID(context.Background(), "app"); err == nil {
		t.Fatal("missing boot id accepted")
	}
	body = `nope`
	if _, err := p.BootID(context.Background(), "app"); err == nil {
		t.Fatal("bad json accepted")
	}
}

func TestPebblePollerRun(t *testing.T) {
	t.Parallel()
	peb := &fakePebble{}
	clk := testingclock.NewFakeClock(time.Now())
	p := &PebblePoller{Prober: peb, Containers: []string{"a", "b"}, Clock: clk}
	var notified atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx, func() { notified.Add(1) }); close(done) }()
	waitFor(t, clk.HasWaiters, "timer")
	if notified.Load() != 0 {
		t.Fatal("notified with nothing ready")
	}
	if s := p.Snapshot(); len(s) != 2 || s[0].BootID != "" {
		t.Fatalf("%+v", s)
	}
	peb.set("a", "1")
	clk.Step(PebblePollInterval)
	waitFor(t, func() bool { return notified.Load() == 1 }, "ready notification")
	// Unchanged: no further notification. Going down is a change too.
	clk.Step(PebblePollInterval)
	peb.set("a", "")
	waitFor(t, func() bool { clk.Step(PebblePollInterval); return notified.Load() == 2 }, "down notification")
	if s := p.Snapshot(); s[0].BootID != "" {
		t.Fatalf("%+v", s)
	}
	cancel()
	<-done
}

func TestLineWriter(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	w := &lineWriter{out: &out, prefix: "> "}
	w.Write([]byte("one\ntw"))
	w.Write([]byte("o\r\nthree"))
	w.Flush()
	w.Flush()
	if got := out.String(); got != "> one\n> two\n> three\n" {
		t.Fatalf("%q", got)
	}
}

func TestExecRunner(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	script := filepath.Join(dir, "dispatch")
	os.WriteFile(script, []byte("#!/bin/sh\necho \"$FOO\"; echo err >&2; exit 7\n"), 0o755)
	var out syncBuffer
	exit, missing, err := ExecRunner{}.Run(context.Background(), HookSpec{Path: script, Dir: dir, Env: []string{"FOO=bar"}, Output: &out})
	if err != nil || missing || exit != 7 || !strings.Contains(out.String(), "bar") || !strings.Contains(out.String(), "err") {
		t.Fatalf("exit %d missing %v err %v out %q", exit, missing, err, out.String())
	}
	if _, missing, _ := (ExecRunner{}).Run(context.Background(), HookSpec{Path: filepath.Join(dir, "nope")}); !missing {
		t.Fatal("missing script not reported")
	}
	// Not executable: a start error, not a hook exit.
	os.WriteFile(filepath.Join(dir, "noexec"), []byte("x"), 0o644)
	if exit, _, err := (ExecRunner{}).Run(context.Background(), HookSpec{Path: filepath.Join(dir, "noexec"), Output: &out}); err == nil || exit != -1 {
		t.Fatalf("exit %d err %v", exit, err)
	}
	// A hook ignoring SIGTERM is killed after the delay.
	slow := filepath.Join(dir, "slow")
	os.WriteFile(slow, []byte("#!/bin/sh\ntrap '' TERM\nwhile true; do sleep 1; done\n"), 0o755)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	ExecRunner{KillDelay: 300 * time.Millisecond}.Run(ctx, HookSpec{Path: slow, Env: []string{"PATH=/usr/bin:/bin"}, Output: &out})
	if time.Since(start) > 5*time.Second {
		t.Fatal("hook not killed")
	}
}

func TestPortsAndStateTools(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.write("script-install", `open-port 80/tcp; open-port 8000-8010/udp; open-port icmp; state-set a=1 b=2 c=3; state-delete b
opened-ports > "$A/ports-during"
state-get > "$A/state-during"
`)
	h.write("script-config-changed", `opened-ports --format=json > "$A/ports"
opened-ports --endpoints > "$A/ports-ep"
close-port 80/tcp; close-port 9999; open-port 80/tcp; open-port 81
state-get a > "$A/a"; state-delete a; state-get a --strict 2> "$A/strict"; echo $? > "$A/strictcode"
state-get --format=json > "$A/all"
`)
	h.write("script-start", `opened-ports --format=json > "$A/ports-after-cc"`)
	h.reconcile()
	h.wantHooks("install", "config-changed", "start")
	if got := strings.TrimSpace(h.read("ports-during")); got != "" {
		t.Fatalf("opened-ports shows pending opens: %q", got)
	}
	if got := h.read("state-during"); !strings.Contains(got, "a: \"1\"") || strings.Contains(got, "b:") || !strings.Contains(got, "c:") {
		t.Fatalf("state-get during hook: %q", got)
	}
	if got := strings.TrimSpace(h.read("ports")); got != `["icmp","80/tcp","8000-8010/udp"]` {
		t.Fatalf("ports %s", got)
	}
	if got := h.read("ports-ep"); !strings.Contains(got, "80/tcp (*)") {
		t.Fatalf("ports with endpoints %q", got)
	}
	if got := strings.TrimSpace(h.read("ports-after-cc")); got != `["icmp","80/tcp","81/tcp","8000-8010/udp"]` {
		t.Fatalf("ports after config-changed %s", got)
	}
	if strings.TrimSpace(h.read("a")) != "1" || !strings.Contains(h.read("strict"), "not found") || strings.TrimSpace(h.read("strictcode")) != "1" {
		t.Fatalf("a=%q strict=%q", h.read("a"), h.read("strict"))
	}
	sp := h.kube.spec()
	if len(sp.State) != 1 || sp.State["c"] != "3" {
		t.Fatalf("state %v", sp.State)
	}
	if len(sp.OpenedPorts) != 4 {
		t.Fatalf("ports %+v", sp.OpenedPorts)
	}
}

func TestSplitListAndHelpers(t *testing.T) {
	t.Parallel()
	if got := splitList(" a, b ,,c"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatal(got)
	}
	if _, ok := osGetenv("PATH"); !ok {
		t.Fatal("PATH")
	}
	_ = firstIPv4()
	if LeaseName("x") != "x-leader" {
		t.Fatal("lease name")
	}
	ports := applyPortOp(nil, portOp{open: true})
	ports = applyPortOp(ports, portOp{open: true})
	if len(ports) != 1 {
		t.Fatal(ports)
	}
	if len(applyPortOp(ports, portOp{})) != 0 || len(applyPortOp(nil, portOp{})) != 0 {
		t.Fatal("close")
	}
	if got := parseInterval("", time.Minute); got != time.Minute {
		t.Fatal(got)
	}
	if got := parseInterval("nonsense", time.Minute); got != time.Minute {
		t.Fatal(got)
	}
	if got := parseInterval("-5s", time.Minute); got != time.Minute {
		t.Fatal(got)
	}
}

func TestNewDefaults(t *testing.T) {
	t.Parallel()
	a := New(Config{App: "x", Unit: "x/0", Kube: newFakeKube(), DataDir: t.TempDir()})
	if a.cfg.Runner == nil || a.cfg.Log == nil || a.cfg.Getenv == nil || a.cfg.PodIP == nil || a.cfg.Pebble == nil || a.clk == nil {
		t.Fatalf("%+v", a.cfg)
	}
	if a.cfg.RetryMin != DefaultRetryMin || a.cfg.RetryMax != DefaultRetryMax || a.cfg.RetryFactor != 2 {
		t.Fatalf("%+v", a.cfg)
	}
	if !strings.HasSuffix(a.cfg.CharmDir(), "agents/unit-x-0/charm") || !strings.HasSuffix(a.cfg.ToolsDir(), "tools/unit-x-0") {
		t.Fatal(a.cfg.CharmDir(), a.cfg.ToolsDir())
	}
	// Trigger is optional and callable.
	var n int
	a.Trigger = func() { n++ }
	a.Terminate()
	if n != 1 || resolver.Life(a.life.Load()) != resolver.Dying {
		t.Fatal("terminate")
	}
}
