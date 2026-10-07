// Package agentenvtest tests the unit agent's Kubernetes interactions against a real API server (no pods run):
// UnitData create, update and persistence across agent restarts, Lease renewal, AppData written only by the
// leader, the resolved annotation, and the full controller-runtime loop with a fake charm.
package agentenvtest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
	"github.com/luci1900/jk/internal/agent/resolver"
)

var (
	restCfg  *rest.Config
	c        client.Client
	agentBin string
)

func TestMain(m *testing.M) {
	code := func() int {
		env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "..", "config", "crd")}, ErrorIfCRDPathMissing: true}
		cfg, err := env.Start()
		if err != nil {
			fmt.Fprintf(os.Stderr, "starting envtest (set KUBEBUILDER_ASSETS; see `make test-envtest`): %v\n", err)
			return 1
		}
		defer env.Stop()
		restCfg = cfg
		if c, err = client.New(cfg, client.Options{Scheme: agent.Scheme()}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		dir, err := os.MkdirTemp("", "jk-agent-envtest")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer os.RemoveAll(dir)
		agentBin = filepath.Join(dir, "jk-agent")
		cmd := exec.Command("go", "build", "-o", agentBin, "github.com/luci1900/jk/cmd/jk-agent")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "building jk-agent: %v\n%s", err, out)
			return 1
		}
		return m.Run()
	}()
	os.Exit(code)
}

// setup creates a throwaway namespace "ag-<name>" with an Application, the leader Lease and jk-model.
func setup(t *testing.T, name string, scale int32) (ns string, app *v1alpha1.Application) {
	t.Helper()
	ctx := context.Background()
	ns = "ag-" + name
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{v1alpha1.ModelLabel: "true"}}}); err != nil {
		t.Fatal(err)
	}
	app = &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: ns},
		Spec:       v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: "app"}, Scale: &scale},
	}
	if err := c.Create(ctx, app); err != nil {
		t.Fatal(err)
	}
	dur := int32(60)
	if err := c.Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "app-leader", Namespace: ns}, Spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: &dur}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.ModelConfigMap, Namespace: ns}}); err != nil {
		t.Fatal(err)
	}
	return ns, app
}

func setHolder(t *testing.T, ns, holder string) {
	t.Helper()
	var l coordinationv1.Lease
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "app-leader"}, &l); err != nil {
		t.Fatal(err)
	}
	if holder == "" {
		l.Spec.HolderIdentity = nil
	} else {
		l.Spec.HolderIdentity = &holder
	}
	if err := c.Update(context.Background(), &l); err != nil {
		t.Fatal(err)
	}
}

func kubeFor(ns string, ordinal int) *agent.KubeClient {
	return &agent.KubeClient{Reader: c, Client: c, Namespace: ns, App: "app", Unit: fmt.Sprintf("app/%d", ordinal), Pod: fmt.Sprintf("app-%d", ordinal)}
}

func TestUnitDataLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, app := setup(t, "unitdata", 2)
	k := kubeFor(ns, 0)

	if ud, err := k.UnitData(ctx); err != nil || ud != nil {
		t.Fatalf("expected no UnitData: %v %v", ud, err)
	}
	spec := v1alpha1.UnitDataSpec{State: map[string]string{"a": "1"}, WorkloadVersion: "1.0",
		OpenedPorts: []v1alpha1.PortRange{{Protocol: "tcp", From: 80, To: 80}}}
	if err := k.SaveUnitData(ctx, spec); err != nil {
		t.Fatal(err)
	}
	ud, err := k.UnitData(ctx)
	if err != nil || ud == nil {
		t.Fatalf("%v %v", ud, err)
	}
	// Named after the pod, owned by the Application (garbage collected with it), labelled with the app.
	if ud.Name != "app-0" || len(ud.OwnerReferences) != 1 || ud.OwnerReferences[0].UID != app.UID || ud.OwnerReferences[0].Kind != "Application" ||
		ud.Labels[v1alpha1.AppLabel] != "app" {
		t.Fatalf("%+v", ud.ObjectMeta)
	}
	if ud.Spec.State["a"] != "1" || ud.Spec.WorkloadVersion != "1.0" {
		t.Fatalf("%+v", ud.Spec)
	}
	// Update replaces the spec (and tolerates a stale cached resource version).
	spec.State = map[string]string{"b": "2"}
	spec.Operation = &v1alpha1.JSON{Raw: []byte(`{"op":"continue","opStep":"pending","installed":true}`)}
	if err := k.SaveUnitData(ctx, spec); err != nil {
		t.Fatal(err)
	}
	ud, _ = k.UnitData(ctx)
	if _, ok := ud.Spec.State["a"]; ok || ud.Spec.State["b"] != "2" || ud.Spec.Operation == nil {
		t.Fatalf("%+v", ud.Spec)
	}
	// Another unit's data is separate.
	if err := kubeFor(ns, 1).SaveUnitData(ctx, v1alpha1.UnitDataSpec{}); err != nil {
		t.Fatal(err)
	}
	var list v1alpha1.UnitDataList
	if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil || len(list.Items) != 2 {
		t.Fatalf("%v %d", err, len(list.Items))
	}
	// Without an Application the object cannot be created.
	if err := c.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	if err := kubeFor(ns, 5).SaveUnitData(ctx, v1alpha1.UnitDataSpec{}); err == nil {
		t.Fatal("UnitData created without an Application")
	}
}

func TestLeaseRenewal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, _ := setup(t, "lease", 1)
	k := kubeFor(ns, 0)

	// Held by someone else: untouched, and not leader.
	setHolder(t, ns, "app/1")
	before, _ := k.Lease(ctx)
	got, err := k.RenewLease(ctx, "app/0", time.Now())
	if err != nil || *got.Spec.HolderIdentity != "app/1" || got.Spec.RenewTime != nil {
		t.Fatalf("%+v %v", got.Spec, err)
	}
	after, _ := k.Lease(ctx)
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatal("lease written by a non-holder")
	}

	// Held by us: renewTime is set, holder unchanged.
	setHolder(t, ns, "app/0")
	now := time.Now().Truncate(time.Microsecond)
	got, err = k.RenewLease(ctx, "app/0", now)
	if err != nil || got.Spec.RenewTime == nil || !got.Spec.RenewTime.Time.Equal(now) {
		t.Fatalf("%+v %v", got.Spec, err)
	}

	// The leadership tracker: leader after the first renewal, with the lease's own duration.
	lead := agent.NewLeadership(k, "app/0", nil2real())
	if lead.IsLeader() {
		t.Fatal("leader before renewing")
	}
	if changed, err := lead.Renew(ctx); err != nil || !changed || !lead.IsLeader() {
		t.Fatalf("%v %v %v", changed, err, lead.IsLeader())
	}
	// The operator moves the lease: the next renewal notices at once.
	setHolder(t, ns, "app/1")
	if changed, err := lead.Renew(ctx); err != nil || !changed || lead.IsLeader() || lead.Holder() != "app/1" {
		t.Fatalf("%v %v %v %q", changed, err, lead.IsLeader(), lead.Holder())
	}
	// A missing lease is an error, not leadership.
	if err := c.Delete(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "app-leader", Namespace: ns}}); err != nil {
		t.Fatal(err)
	}
	if _, err := lead.Renew(ctx); !apierrors.IsNotFound(err) {
		t.Fatalf("%v", err)
	}
}

func TestAppDataAndResolvedAndModelConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, _ := setup(t, "appdata", 1)
	k := kubeFor(ns, 0)

	if ad, err := k.AppData(ctx); err != nil || ad != nil {
		t.Fatalf("%v %v", ad, err)
	}
	now := metav1.Now()
	set := func(state string) func(*v1alpha1.AppDataSpec) {
		return func(s *v1alpha1.AppDataSpec) { s.Status = &v1alpha1.WorkloadStatus{State: state, Since: &now} }
	}
	if err := k.UpdateAppData(ctx, set("active")); err != nil {
		t.Fatal(err)
	}
	if err := k.UpdateAppData(ctx, func(s *v1alpha1.AppDataSpec) { s.Relations = map[string]v1alpha1.RelationData{"1": {"k": "v"}} }); err != nil {
		t.Fatal(err)
	}
	ad, _ := k.AppData(ctx)
	if ad.Name != "app" || ad.Spec.Status.State != "active" || ad.Spec.Relations["1"]["k"] != "v" || len(ad.OwnerReferences) != 1 {
		t.Fatalf("%+v", ad)
	}

	// Model config.
	cm := &corev1.ConfigMap{}
	_ = c.Get(ctx, client.ObjectKey{Namespace: ns, Name: v1alpha1.ModelConfigMap}, cm)
	cm.Data = map[string]string{"model-config.update-status-hook-interval": "3s"}
	if err := c.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if data, err := k.ModelConfig(ctx); err != nil || data["model-config.update-status-hook-interval"] != "3s" {
		t.Fatalf("%v %v", data, err)
	}
	_ = c.Delete(ctx, cm)
	if data, err := k.ModelConfig(ctx); err != nil || len(data) != 0 {
		t.Fatalf("%v %v", data, err)
	}

	// The resolved annotation on the unit's Pod.
	if r, err := k.Resolved(ctx); err != nil || r != "" {
		t.Fatalf("no pod: %q %v", r, err)
	}
	if err := k.ClearResolved(ctx); err != nil {
		t.Fatalf("clearing on a missing pod: %v", err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: ns, Annotations: map[string]string{agent.ResolvedAnnotation: "retry", "other": "x"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "i"}}}}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if r, err := k.Resolved(ctx); err != nil || r != "retry" {
		t.Fatalf("%q %v", r, err)
	}
	if err := k.ClearResolved(ctx); err != nil {
		t.Fatal(err)
	}
	if r, _ := k.Resolved(ctx); r != "" {
		t.Fatalf("annotation not cleared: %q", r)
	}
	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations["other"] != "x" {
		t.Fatal("other annotations lost")
	}
}

const dispatch = `#!/bin/sh
A="$CHARM_DIR/.."
echo "$JUJU_HOOK_NAME" >> "$A/hooks.log"
[ -f "$A/script-$JUJU_HOOK_NAME" ] && . "$A/script-$JUJU_HOOK_NAME"
exit 0
`

type charmDir struct {
	t         *testing.T
	ordinal   int
	root      string // data dir
	agentDir  string
	hookLog   string
	socketDir string
}

func newCharmDir(t *testing.T) *charmDir { return newCharmDirFor(t, 0) }

// newCharmDirFor is the data dir of unit app/<ordinal>.
func newCharmDirFor(t *testing.T, ordinal int) *charmDir {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "jkev")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	d := &charmDir{t: t, ordinal: ordinal, root: root, agentDir: filepath.Join(root, "agents", fmt.Sprintf("unit-app-%d", ordinal))}
	d.hookLog = filepath.Join(d.agentDir, "hooks.log")
	d.write("charm/metadata.yaml", "name: app\ncontainers:\n  workload: {resource: w}\npeers:\n  database-peers: {interface: pg}\nstorage:\n  data: {type: filesystem}\n")
	d.write("charm/config.yaml", "options:\n  greeting: {type: string, default: hello}\n")
	d.write("charm/dispatch", dispatch)
	d.socketDir = filepath.Join(root, "containers")
	return d
}

func (d *charmDir) write(rel, content string) {
	p := filepath.Join(d.agentDir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		d.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
		d.t.Fatal(err)
	}
}

func (d *charmDir) hooks() []string {
	b, _ := os.ReadFile(d.hookLog)
	return strings.Fields(string(b))
}

// pebble serves a fake Pebble at <socketDir>/<container>/pebble.socket.
func (d *charmDir) pebble(container, bootID string) func() {
	dir := filepath.Join(d.socketDir, container)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		d.t.Fatal(err)
	}
	l, err := net.Listen("unix", filepath.Join(dir, "pebble.socket"))
	if err != nil {
		d.t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"type":"sync","status-code":200,"result":{"boot-id":%q}}`, bootID)
	})}
	go srv.Serve(l)
	return func() { srv.Close() }
}

func (d *charmDir) opts(ns string, ordinal int) agent.RunOptions {
	return agent.RunOptions{
		Identity: agent.Identity{Pod: fmt.Sprintf("app-%d", ordinal), Namespace: ns, ModelName: ns, ModelUUID: "uuid", App: "app", Ordinal: ordinal,
			Containers: []string{"workload"}},
		DataDir: d.root, ContainersDir: d.socketDir, Self: agentBin, RestConfig: restCfg,
		Log: func(f string, a ...any) { d.t.Logf(time.Now().Format("15:04:05.000")+" agent: "+f, a...) },
	}
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func unitState(t *testing.T, ns string, ordinal int) (resolver.State, *v1alpha1.UnitData) {
	t.Helper()
	ud, err := kubeFor(ns, ordinal).UnitData(context.Background())
	if err != nil || ud == nil || ud.Spec.Operation == nil {
		return resolver.State{}, nil
	}
	var st resolver.State
	if err := jsonUnmarshal(ud.Spec.Operation.Raw, &st); err != nil {
		t.Fatal(err)
	}
	return st, ud
}

// running is an agent.Run in a goroutine; Stop delivers the termination notice and waits for Run to return.
type running struct {
	cancel context.CancelFunc
	done   chan error
	err    error
	once   sync.Once
}

func runAgent(t *testing.T, opts agent.RunOptions) *running {
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- agent.Run(ctx, opts) }()
	t.Cleanup(func() { r.Stop() })
	return r
}

func (r *running) Stop() error {
	r.once.Do(func() { r.cancel(); r.err = <-r.done })
	return r.err
}

// TestAgentLoop runs the real controller-runtime loop: fresh unit as leader, config change, update-status, a
// restart (no second install), leadership loss, then removal on scale-down.
func TestAgentLoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, app := setup(t, "loop", 1)
	setHolder(t, ns, "app/0")
	d := newCharmDir(t)
	stopPebble := d.pebble("workload", "boot-1")
	defer stopPebble()
	d.write("script-install", "state-set installed=yes; open-port 8080/tcp\n")
	d.write("script-config-changed", "status-set --application active 'config '$(config-get greeting)\n")
	cm := &corev1.ConfigMap{}
	_ = c.Get(ctx, client.ObjectKey{Namespace: ns, Name: v1alpha1.ModelConfigMap}, cm)
	cm.Data = map[string]string{"model-config.update-status-hook-interval": "2s"}
	_ = c.Update(ctx, cm)

	a := runAgent(t, d.opts(ns, 0))

	eventually(t, "unit started", 60*time.Second, func() bool { st, _ := unitState(t, ns, 0); return st.Started })
	if got := strings.Join(d.hooks()[:5], ","); got != "install,leader-elected,workload-pebble-ready,config-changed,start" {
		t.Fatalf("hooks: %v", d.hooks())
	}
	st, ud := unitState(t, ns, 0)
	if ud.Spec.State["installed"] != "yes" || len(ud.Spec.OpenedPorts) != 1 || ud.Spec.AgentStatus.State != agent.AgentIdle || !st.Leader {
		t.Fatalf("%+v", ud.Spec)
	}

	// Leader logic wrote the application status into AppData.
	eventually(t, "AppData", 10*time.Second, func() bool {
		ad, _ := kubeFor(ns, 0).AppData(ctx)
		return ad != nil && ad.Spec.Status != nil && ad.Spec.Status.Message == "config hello"
	})

	// The Lease is renewed by the agent.
	var lease coordinationv1.Lease
	_ = c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "app-leader"}, &lease)
	if lease.Spec.RenewTime == nil {
		t.Fatal("lease not renewed")
	}

	// Config change through the Application.
	_ = c.Get(ctx, client.ObjectKeyFromObject(app), app)
	app.Spec.Config = map[string]string{"greeting": "bonjour"}
	if err := c.Update(ctx, app); err != nil {
		t.Fatal(err)
	}
	eventually(t, "config-changed", 20*time.Second, func() bool {
		ad, _ := kubeFor(ns, 0).AppData(ctx)
		return ad != nil && ad.Spec.Status.Message == "config bonjour"
	})
	// update-status every 2s.
	eventually(t, "update-status", 20*time.Second, func() bool {
		n := 0
		for _, h := range d.hooks() {
			if h == "update-status" {
				n++
			}
		}
		return n >= 2
	})

	// Restart (the pod is restarted, the Application is unchanged): stop and remove must not run.
	if err := a.Stop(); err != nil {
		t.Fatal(err)
	}
	for _, h := range d.hooks() {
		if h == "stop" || h == "remove" {
			t.Fatalf("hook %s ran on restart: %v", h, d.hooks())
		}
	}
	installs := count(d.hooks(), "install")
	updates := count(d.hooks(), "update-status")
	a = runAgent(t, d.opts(ns, 0))
	eventually(t, "restarted agent idle", 30*time.Second, func() bool {
		_, ud := unitState(t, ns, 0)
		return ud != nil && ud.Spec.AgentStatus != nil && ud.Spec.AgentStatus.State == agent.AgentIdle
	})
	// The restarted agent is running hooks again (the next update-status), so anything it would wrongly rerun has by now.
	eventually(t, "update-status after the restart", 20*time.Second, func() bool {
		return count(d.hooks(), "update-status") > updates
	})
	if count(d.hooks(), "install") != installs || count(d.hooks(), "leader-elected") != 1 || count(d.hooks(), "workload-pebble-ready") != 1 {
		t.Fatalf("hooks rerun after restart: %v", d.hooks())
	}

	// Leadership moves away, then back.
	setHolder(t, ns, "app/1")
	eventually(t, "leadership resigned", 20*time.Second, func() bool { st, _ := unitState(t, ns, 0); return !st.Leader })
	setHolder(t, ns, "app/0")
	eventually(t, "leader-elected again", 30*time.Second, func() bool { return count(d.hooks(), "leader-elected") == 2 })

	// Scale down to zero and terminate: stop and remove run.
	_ = c.Get(ctx, client.ObjectKeyFromObject(app), app)
	zero := int32(0)
	app.Spec.Scale = &zero
	if err := c.Update(ctx, app); err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(); err != nil {
		t.Fatal(err)
	}
	tail := d.hooks()
	if n := len(tail); n < 2 || tail[n-2] != "stop" || tail[n-1] != "remove" {
		t.Fatalf("hooks: %v", tail)
	}
	st, ud = unitState(t, ns, 0)
	if !st.Removed || ud.Spec.WorkloadStatus.State != "terminated" {
		t.Fatalf("%+v %+v", st, ud.Spec.WorkloadStatus)
	}
}

func count(hooks []string, name string) int {
	n := 0
	for _, h := range hooks {
		if h == name {
			n++
		}
	}
	return n
}

// TestFreshUnitAfterUnitDataDeleted: the operator deletes UnitData on scale-down, so a unit that comes back
// starts from install again.
func TestFreshUnitAfterUnitDataDeleted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, _ := setup(t, "fresh", 1)
	setHolder(t, ns, "app/1")
	d := newCharmDir(t)
	runOnce := func() {
		a := runAgent(t, d.opts(ns, 0))
		eventually(t, "started", 60*time.Second, func() bool { st, _ := unitState(t, ns, 0); return st.Started })
		if err := a.Stop(); err != nil {
			t.Fatal(err)
		}
	}
	runOnce()
	if got := strings.Join(d.hooks(), ","); got != "install,config-changed,start" {
		t.Fatalf("%v", d.hooks())
	}
	// Not the leader: no AppData and no leader-elected.
	if ad, _ := kubeFor(ns, 0).AppData(ctx); ad != nil {
		t.Fatalf("AppData written by a non-leader: %+v", ad)
	}
	_, ud := unitState(t, ns, 0)
	if err := c.Delete(ctx, ud); err != nil {
		t.Fatal(err)
	}
	runOnce()
	if got := strings.Join(d.hooks(), ","); got != "install,config-changed,start,install,config-changed,start" {
		t.Fatalf("%v", d.hooks())
	}
}

// TestRemovalWhenApplicationDeleted: the Application is gone at SIGTERM, so the unit runs stop and remove.
func TestRemovalWhenApplicationDeleted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, app := setup(t, "gone", 1)
	setHolder(t, ns, "app/1")
	d := newCharmDir(t)
	a := runAgent(t, d.opts(ns, 0))
	eventually(t, "started", 60*time.Second, func() bool { st, _ := unitState(t, ns, 0); return st.Started })
	if err := c.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	if err := a.Stop(); err != nil {
		t.Fatal(err)
	}
	if tail := d.hooks(); len(tail) < 2 || tail[len(tail)-2] != "stop" || tail[len(tail)-1] != "remove" {
		t.Fatalf("%v", tail)
	}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func nil2real() clock.Clock { return clock.RealClock{} }

// TestFreshUnitWaitsForLeader: with no Lease holder yet the first hook is held back; once the operator names this
// unit, install is followed by leader-elected before config-changed and start.
func TestFreshUnitWaitsForLeader(t *testing.T) {
	t.Parallel()
	ns, _ := setup(t, "wait", 1)
	d := newCharmDir(t)
	a := runAgent(t, d.opts(ns, 0))
	// The agent has started (its UnitData exists) and is holding the first hook back until the Lease has a holder.
	eventually(t, "agent started", 30*time.Second, func() bool {
		ud, _ := kubeFor(ns, 0).UnitData(context.Background())
		return ud != nil && ud.Spec.AgentStatus != nil && ud.Spec.AgentStatus.State == agent.AgentAllocating
	})
	if h := d.hooks(); len(h) != 0 {
		t.Fatalf("hooks ran before a leader was assigned: %v", h)
	}
	setHolder(t, ns, "app/0")
	eventually(t, "started", 60*time.Second, func() bool { st, _ := unitState(t, ns, 0); return st.Started })
	if got := strings.Join(d.hooks(), ","); got != "install,leader-elected,config-changed,start" {
		t.Fatalf("%v", d.hooks())
	}
	_ = a.Stop()
}
