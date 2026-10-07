//go:build e2e

// Package e2e runs jk against the kind-jk-dev cluster (make dev), or the context in JK_E2E_CONTEXT. It needs the test charm built with `make charm`.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/registry"
)

// kubeContext is the kubeconfig context under test: kind-jk-dev by default, JK_E2E_CONTEXT to override (CI on MicroK8s).
var kubeContext = func() string {
	if c := os.Getenv("JK_E2E_CONTEXT"); c != "" {
		return c
	}
	return "kind-jk-dev"
}()

const (
	charmName = "jk-test"
	app       = "jk-test"
	// readyTimeout covers image pulls on a cold node.
	readyTimeout = 4 * time.Minute
	pollEvery    = 500 * time.Millisecond
)

var (
	kube   client.Client
	clset  kubernetes.Interface
	digest string // jk-test, revision 1
	// clientDigest is jk-test-client; rev2Digest is jk-test stamped as revision 2 (the target of the refresh scenario).
	clientDigest string
	rev2Digest   string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg, err := config.GetConfigWithContext(kubeContext)
	if err != nil {
		return fail("kube config for %s: %v", kubeContext, err)
	}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	if kube, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		return fail("client: %v", err)
	}
	if clset, err = kubernetes.NewForConfig(cfg); err != nil {
		return fail("clientset: %v", err)
	}
	reg, closeFn, err := registry.Connect(ctx, cfg)
	if err != nil {
		return fail("connecting to jk-registry: %v", err)
	}
	defer closeFn()
	// The charms are built by `make charm`; JK_TEST_CHARM, JK_TEST_CLIENT_CHARM and JK_TEST_CHARM_REV2 override the paths.
	for _, c := range []struct {
		env, file string
		digest    *string
	}{
		{"JK_TEST_CHARM", "jk-test.charm", &digest},
		{"JK_TEST_CLIENT_CHARM", "jk-test-client.charm", &clientDigest},
		{"JK_TEST_CHARM_REV2", "jk-test-rev2.charm", &rev2Digest},
	} {
		charm := os.Getenv(c.env)
		if charm == "" {
			charm, _ = filepath.Abs(filepath.Join("../../bin", c.file))
		}
		if _, err := os.Stat(charm); err != nil {
			return fail("test charm missing (run `make charm`): %v", err)
		}
		if *c.digest, err = reg.Push(ctx, charm); err != nil {
			return fail("pushing %s: %v", charm, err)
		}
	}
	code := m.Run()
	cleanupModels()
	return code
}

func fail(format string, args ...any) int {
	fmt.Fprintf(os.Stderr, "e2e setup: "+format+"\n", args...)
	return 1
}

// models are the namespaces the tests created; cleanupModels waits for them to go once all tests are done.
var (
	modelsMu sync.Mutex
	models   []string
)

// newModel creates a labelled model namespace with a random name and starts deleting it when the test ends
// (the volumes it leaves behind are removed by cleanupModels, so tests don't wait for the pods' termination).
func newModel(t *testing.T) string {
	t.Helper()
	ns := fmt.Sprintf("e2e-%06x", rand.IntN(1<<24))
	obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{v1alpha1.ModelLabel: "true"}}}
	if err := kube.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("JK_E2E_KEEP") != "" {
		return ns // leave the namespace for inspection (delete it by hand)
	}
	modelsMu.Lock()
	models = append(models, ns)
	modelsMu.Unlock()
	t.Cleanup(func() {
		if t.Failed() {
			dump(t, ns)
		}
		_ = kube.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})
	return ns
}

// dump logs the state of a failed test: pods with the logs of all their containers, Applications, UnitData, AppData, Relations and the namespace's volumes.
func dump(t *testing.T, ns string) {
	ctx := context.Background()
	var pods corev1.PodList
	_ = kube.List(ctx, &pods, client.InNamespace(ns))
	for _, p := range pods.Items {
		t.Logf("pod %s: phase=%s", p.Name, p.Status.Phase)
		for _, cs := range p.Status.ContainerStatuses {
			t.Logf("pod %s container %s: ready=%v restarts=%d state=%+v", p.Name, cs.Name, cs.Ready, cs.RestartCount, cs.State)
		}
		names := []string{}
		for _, c := range p.Spec.InitContainers {
			names = append(names, c.Name)
		}
		for _, c := range p.Spec.Containers {
			names = append(names, c.Name)
		}
		for _, c := range names {
			b, err := clset.CoreV1().Pods(ns).GetLogs(p.Name, &corev1.PodLogOptions{Container: c, TailLines: ptr(int64(80))}).DoRaw(ctx)
			t.Logf("pod %s container %s logs (err=%v):\n%s", p.Name, c, err, b)
		}
	}
	var apps v1alpha1.ApplicationList
	_ = kube.List(ctx, &apps, client.InNamespace(ns))
	for _, a := range apps.Items {
		t.Logf("application %s status: %+v", a.Name, a.Status.Status)
		t.Logf("application %s conditions: %+v leader=%q", a.Name, a.Status.Conditions, a.Status.Leader)
	}
	var uds v1alpha1.UnitDataList
	_ = kube.List(ctx, &uds, client.InNamespace(ns))
	for _, u := range uds.Items {
		t.Logf("unitdata %s: agent=%+v workload=%+v relations=%+v tracked=%+v error=%v", u.Name, u.Spec.AgentStatus, u.Spec.WorkloadStatus, u.Spec.Relations, u.Spec.TrackedSecrets, u.Spec.ErrorState)
	}
	var ads v1alpha1.AppDataList
	_ = kube.List(ctx, &ads, client.InNamespace(ns))
	for _, a := range ads.Items {
		t.Logf("appdata %s: status=%+v relations=%+v", a.Name, a.Spec.Status, a.Spec.Relations)
	}
	var rels v1alpha1.RelationList
	_ = kube.List(ctx, &rels, client.InNamespace(ns))
	for _, r := range rels.Items {
		t.Logf("relation %s: %+v id=%d", r.Name, r.Spec.Endpoints, r.Status.ID)
	}
	for _, pv := range pvsOf(ns) {
		t.Logf("pv %s: phase=%s reclaim=%s claim=%s", pv.Name, pv.Status.Phase, pv.Spec.PersistentVolumeReclaimPolicy, pv.Spec.ClaimRef.Name)
	}
}

// cleanupModels waits for the test namespaces to be gone and then deletes the volumes they leave behind (the operator Retains them), so tests don't leak PVs.
func cleanupModels() {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Minute)
	for _, ns := range models {
		// The volumes are listed while the namespace's claims still name it, and again once it is gone (released ones keep the claimRef).
		pvs := pvsOf(ns)
		for time.Now().Before(deadline) {
			if err := kube.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{}); apierrors.IsNotFound(err) {
				break
			}
			time.Sleep(pollEvery)
		}
		for _, pv := range append(pvs, pvsOf(ns)...) {
			_ = kube.Delete(ctx, &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pv.Name}})
		}
	}
}

func ptr[T any](v T) *T { return &v }

// deploy creates the local test charm Application.
func deploy(t *testing.T, ns string, scale int32, cfg map[string]string) *v1alpha1.Application {
	t.Helper()
	return deployWith(t, ns, scale, func(a *v1alpha1.Application) { a.Spec.Config = cfg })
}

// deployWith is deploy with a callback to adjust the Application before it is created.
func deployWith(t *testing.T, ns string, scale int32, mutate func(*v1alpha1.Application)) *v1alpha1.Application {
	t.Helper()
	a := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: ns},
		Spec: v1alpha1.ApplicationSpec{
			Charm: v1alpha1.CharmSpec{Name: charmName, Source: "local", Sha256: strings.TrimPrefix(digest, "sha256:")},
			Scale: &scale,
		},
	}
	if mutate != nil {
		mutate(a)
	}
	if err := kube.Create(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return a
}

// eventually polls cond until it returns true or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	nextLog := time.Now().Add(time.Minute)
	for {
		ok, err := cond()
		if ok && err == nil {
			return
		}
		last = err
		if time.Now().After(nextLog) {
			t.Logf("still waiting for %s: %v", what, last)
			nextLog = time.Now().Add(time.Minute)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s (last error: %v)", timeout, what, last)
		}
		time.Sleep(pollEvery)
	}
}

func unitName(n int) string { return fmt.Sprintf("%s-%d", app, n) }

func podReady(ns, name string) (bool, error) {
	var p corev1.Pod
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &p); err != nil {
		return false, err
	}
	if p.DeletionTimestamp != nil {
		return false, fmt.Errorf("pod %s terminating", name)
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue, nil
		}
	}
	return false, nil
}

func podUID(t *testing.T, ns, name string) string {
	t.Helper()
	var p corev1.Pod
	if err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &p); err != nil {
		t.Fatal(err)
	}
	return string(p.UID)
}

func unitData(ns string, n int) (*v1alpha1.UnitData, error) {
	var u v1alpha1.UnitData
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: unitName(n)}, &u)
	return &u, err
}

func appData(ns string) (*v1alpha1.AppData, error) {
	var a v1alpha1.AppData
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &a)
	return &a, err
}

func application(ns string) (*v1alpha1.Application, error) {
	var a v1alpha1.Application
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app}, &a)
	return &a, err
}

// waitUnitActive waits for unit n's pod to be ready and its workload status active.
func waitUnitActive(t *testing.T, ns string, n int, msg string) {
	t.Helper()
	eventually(t, readyTimeout, unitName(n)+" ready and active", func() (bool, error) {
		if ok, err := podReady(ns, unitName(n)); !ok || err != nil {
			return false, err
		}
		u, err := unitData(ns, n)
		if err != nil {
			return false, err
		}
		s := u.Spec.WorkloadStatus
		if s == nil || s.State != "active" || !strings.Contains(s.Message, msg) {
			return false, fmt.Errorf("workload status %+v", s)
		}
		return true, nil
	})
}

// ---- agent logs ----

var hookLine = regexp.MustCompile(`jk-agent: hook (\S+) finished exit=(\d+)`)

type hookRun struct {
	Name string
	Exit int
}

// agentLogs returns the current `charm` container log of the unit's pod.
func agentLogs(t *testing.T, ns string, n int) string { return agentLogsOf(t, ns, app, n) }

// agentLogsOf is agentLogs for any application.
func agentLogsOf(t *testing.T, ns, appName string, n int) string {
	t.Helper()
	b, err := clset.CoreV1().Pods(ns).GetLogs(unitOf(appName, n), &corev1.PodLogOptions{Container: "charm"}).DoRaw(context.Background())
	if err != nil {
		t.Fatalf("logs of %s: %v", unitOf(appName, n), err)
	}
	return string(b)
}

func parseHooks(logs string) []hookRun {
	var out []hookRun
	for _, m := range hookLine.FindAllStringSubmatch(logs, -1) {
		code, _ := strconv.Atoi(m[2])
		out = append(out, hookRun{m[1], code})
	}
	return out
}

func hookNames(logs string) []string {
	var out []string
	for _, h := range parseHooks(logs) {
		out = append(out, h.Name)
	}
	return out
}

func count(names []string, name string) int {
	c := 0
	for _, n := range names {
		if n == name {
			c++
		}
	}
	return c
}

func index(names []string, name string) int {
	for i, n := range names {
		if n == name {
			return i
		}
	}
	return -1
}

// waitHooks waits until the unit's log has all the named hooks.
func waitHooks(t *testing.T, ns string, n int, want ...string) []string {
	t.Helper()
	var names []string
	eventually(t, readyTimeout, fmt.Sprintf("hooks %v on %s", want, unitName(n)), func() (bool, error) {
		b, err := clset.CoreV1().Pods(ns).GetLogs(unitName(n), &corev1.PodLogOptions{Container: "charm"}).DoRaw(context.Background())
		if err != nil {
			return false, err
		}
		names = hookNames(string(b))
		for _, w := range want {
			if index(names, w) < 0 {
				return false, fmt.Errorf("have %v", names)
			}
		}
		return true, nil
	})
	return names
}

// logFollower streams a pod's charm container log into a buffer, so a stop hook can be seen after the pod is gone.
type logFollower struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	done chan struct{}
}

func follow(ns, pod string) *logFollower {
	f := &logFollower{done: make(chan struct{})}
	go func() {
		defer close(f.done)
		s, err := clset.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: "charm", Follow: true}).Stream(context.Background())
		if err != nil {
			return
		}
		defer s.Close()
		b := make([]byte, 4096)
		for {
			n, err := s.Read(b)
			f.mu.Lock()
			f.buf.Write(b[:n])
			f.mu.Unlock()
			if err != nil {
				if err != io.EOF {
					f.mu.Lock()
					fmt.Fprintf(&f.buf, "\n[follow error: %v]\n", err)
					f.mu.Unlock()
				}
				return
			}
		}
	}()
	return f
}

func (f *logFollower) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.String()
}

func gone(obj client.Object, key client.ObjectKey) func() (bool, error) {
	return func() (bool, error) {
		err := kube.Get(context.Background(), key, obj)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("%s still exists (err=%v)", key, err)
	}
}
