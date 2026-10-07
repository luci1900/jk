package envtest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
	"github.com/luci1900/jk/pkg/charmhub"
)

// One API server and operator are shared by the operator tests; each test uses its own op-* namespace.
var (
	opOnce   sync.Once
	opEnv    *envtest.Environment
	opClient client.Client
	opCharms = &fakeCharms{charms: map[string]*registry.Charm{}}
	opErr    error
	opCancel context.CancelFunc
	nsSeq    atomic.Int32
)

func TestMain(m *testing.M) {
	code := m.Run()
	if opCancel != nil {
		opCancel() // stop the manager first: the API server waits for open watches when it shuts down
		time.Sleep(200 * time.Millisecond)
	}
	if opEnv != nil {
		_ = opEnv.Stop()
	}
	if opHub != nil {
		opHub.srv.Close()
	}
	os.Exit(code)
}

type fakeCharms struct {
	mu     sync.Mutex
	charms map[string]*registry.Charm
	calls  map[string]int
}

func (f *fakeCharms) Charm(_ context.Context, digest string) (*registry.Charm, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[digest]++
	if c, ok := f.charms[digest]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("manifest unknown: %s", digest)
}

func (f *fakeCharms) add(digest string, c *registry.Charm) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c.Digest = digest
	f.charms[digest] = c
}

func (f *fakeCharms) called(digest string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[digest]
}

const (
	digestSimple = "sha256:1111"
	digestStore  = "sha256:2222"
)

func startOperator(t *testing.T) client.Client {
	t.Helper()
	opOnce.Do(func() {
		opEnv = &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
		cfg, err := opEnv.Start()
		if err != nil {
			opErr = fmt.Errorf("starting envtest (set KUBEBUILDER_ASSETS; see `make test-envtest`): %w", err)
			return
		}
		scheme := runtime.NewScheme()
		_ = clientgoscheme.AddToScheme(scheme)
		_ = v1alpha1.AddToScheme(scheme)
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: operator.CacheOptions()})
		if err != nil {
			opErr = err
			return
		}
		ocfg := operator.DefaultConfig("kind.local/jk-agent:test")
		ocfg.ClusterRetry = 500 * time.Millisecond
		opHub = newFakeHub()
		opStore = &fakeStore{}
		hub := &charmhub.Client{BaseURL: opHub.srv.URL, HTTPClient: opHub.srv.Client()}
		if err := (&operator.ApplicationReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Charms: opCharms, Hub: hub, Store: opStore, Config: ocfg}).SetupWithManager(mgr); err != nil {
			opErr = err
			return
		}
		if err := (&operator.RelationReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr); err != nil {
			opErr = err
			return
		}
		if err := (&operator.OfferReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
			opErr = err
			return
		}
		if err := (&operator.ActionReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr); err != nil {
			opErr = err
			return
		}
		if err := (&operator.ModelReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
			opErr = err
			return
		}
		var mctx context.Context
		mctx, opCancel = context.WithCancel(context.Background())
		go func() { _ = mgr.Start(mctx) }()
		opClient, opErr = client.New(cfg, client.Options{Scheme: scheme})
		if opErr == nil {
			// envtest has no kubelets: one node says which architecture charms are resolved for.
			opErr = opClient.Create(context.Background(), testNode("node-amd64", "amd64"))
		}
		if opErr == nil {
			opErr = opClient.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.SystemNamespace}})
		}
		opCharms.add(digestSimple, &registry.Charm{
			Metadata: map[string]any{
				"name":       "simple",
				"containers": map[string]any{"workload": map[string]any{"resource": "img"}},
				"resources":  map[string]any{"img": map[string]any{"type": "oci-image", "upstream-source": "busybox:1"}},
			},
			Config: map[string]any{"options": map[string]any{"greeting": map[string]any{"type": "string", "default": "hi"}}},
			Base:   registry.Base{Name: "ubuntu", Channel: "22.04"},
		})
		opCharms.add(digestStore, &registry.Charm{
			Metadata: map[string]any{
				"name":       "store",
				"containers": map[string]any{"data": map[string]any{"resource": "img", "mounts": []any{map[string]any{"storage": "vol", "location": "/data"}}}},
				"resources":  map[string]any{"img": map[string]any{"type": "oci-image"}},
				"storage":    map[string]any{"vol": map[string]any{"type": "filesystem", "location": "/data"}},
			},
			Base: registry.Base{Name: "ubuntu", Channel: "24.04"},
		})
	})
	if opErr != nil {
		t.Fatal(opErr)
	}
	return opClient
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 20*time.Second, true, func(context.Context) (bool, error) { return f(), nil })
	if err != nil {
		t.Fatalf("timed out waiting for %s", what)
	}
}

func consistently(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !f() {
			t.Fatalf("%s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func newNamespace(t *testing.T, c client.Client, labelled bool) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("op-%d", nsSeq.Add(1))}}
	if labelled {
		ns.Labels = map[string]string{v1alpha1.ModelLabel: "true"}
	}
	if err := c.Create(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func newApp(t *testing.T, c client.Client, ns, name, digest string, scale int32) *v1alpha1.Application {
	t.Helper()
	app := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.ApplicationSpec{
			Charm: v1alpha1.CharmSpec{Name: name, Source: "local", Sha256: digest},
			Scale: &scale,
		},
	}
	if err := c.Create(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	return app
}

func get(c client.Client, ns, name string, obj client.Object) error {
	return c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, obj)
}

func exists(c client.Client, ns, name string, obj client.Object) bool {
	return get(c, ns, name, obj) == nil
}

func gone(c client.Client, ns, name string, obj client.Object) bool {
	return apierrors.IsNotFound(get(c, ns, name, obj))
}

func ready(c client.Client, ns, name string) *v1alpha1.Application {
	var a v1alpha1.Application
	if get(c, ns, name, &a) != nil {
		return nil
	}
	return &a
}

func createPod(t *testing.T, c client.Client, ns, app string, n int, isReady bool) {
	t.Helper()
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", app, n), Namespace: ns, Labels: operator.PodLabels(app)},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	setPodReady(t, c, ns, pod.Name, isReady)
}

func setPodReady(t *testing.T, c client.Client, ns, name string, isReady bool) {
	t.Helper()
	var pod corev1.Pod
	if err := get(c, ns, name, &pod); err != nil {
		t.Fatal(err)
	}
	st := corev1.ConditionFalse
	if isReady {
		st = corev1.ConditionTrue
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: st}}
	state := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}
	if isReady {
		state = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "charm", State: state}}
	if err := c.Status().Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
}

// setCharmRunning makes the charm container run while the pod stays not Ready (probes haven't passed yet).
func setCharmRunning(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	var pod corev1.Pod
	if err := get(c, ns, name, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "charm", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := c.Status().Update(context.Background(), &pod); err != nil {
		t.Fatal(err)
	}
}

func leaseHolder(c client.Client, ns, app string) string {
	var l coordinationv1.Lease
	if get(c, ns, operator.LeaseName(app), &l) != nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

func TestApplicationCreatesObjects(t *testing.T) {
	// Serial: it counts pushes or registry reads, which the shared fakes keep for the whole package.
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "simple", digestSimple, 2)

	var sts appsv1.StatefulSet
	eventually(t, "statefulset", func() bool { return exists(c, ns, "simple", &sts) })
	var nsObj corev1.Namespace
	if err := c.Get(ctx, client.ObjectKey{Name: ns}, &nsObj); err != nil {
		t.Fatal(err)
	}

	// Every object is owned by the Application and labelled; the pod template carries the identity env.
	eventually(t, "all objects", func() bool {
		return exists(c, ns, "simple", &corev1.Service{}) && exists(c, ns, "simple-endpoints", &corev1.Service{}) &&
			exists(c, ns, "simple", &corev1.ServiceAccount{}) && exists(c, ns, "simple-leader", &coordinationv1.Lease{})
	})
	var ep corev1.Service
	_ = get(c, ns, "simple-endpoints", &ep)
	if ep.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("endpoints service not headless: %q", ep.Spec.ClusterIP)
	}
	objs := []client.Object{&sts, &corev1.Service{}, &corev1.ServiceAccount{}, &coordinationv1.Lease{}}
	names := []string{"simple", "simple", "simple", "simple-leader"}
	for i, o := range objs {
		if err := get(c, ns, names[i], o); err != nil {
			t.Fatal(err)
		}
		if !metav1.IsControlledBy(o, app) {
			t.Errorf("%T %s not controlled by the Application: %v", o, names[i], o.GetOwnerReferences())
		}
		if o.GetLabels()[v1alpha1.AppLabel] != "simple" {
			t.Errorf("%T labels %v", o, o.GetLabels())
		}
	}
	var role rbacv1.Role
	if err := get(c, ns, "simple", &role); err != nil || len(role.Rules) == 0 {
		t.Errorf("role: %v %+v", err, role.Rules)
	}
	if err := get(c, ns, "simple", &rbacv1.RoleBinding{}); err != nil {
		t.Errorf("rolebinding: %v", err)
	}
	if *sts.Spec.Replicas != 2 {
		t.Errorf("replicas %d", *sts.Spec.Replicas)
	}
	env := map[string]string{}
	for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	if env["JK_MODEL_UUID"] != string(nsObj.UID) || env["JK_MODEL_NAME"] != ns || env["JK_APP"] != "simple" || env["JUJU_CONTAINER_NAMES"] != "workload" {
		t.Errorf("env %v", env)
	}
	if got := sts.Spec.Template.Spec.InitContainers[0].Image; got != "kind.local/jk-agent:test" {
		t.Errorf("init image %q", got)
	}
	if got := sts.Spec.Template.Spec.Containers[1].Image; got != "busybox:1" {
		t.Errorf("workload image %q", got)
	}

	// Status: charm from the registry, finalizer, observedGeneration, Ready false until the pods are ready.
	eventually(t, "status", func() bool {
		a := ready(c, ns, "simple")
		return a != nil && a.Status.Charm != nil && meta.FindStatusCondition(a.Status.Conditions, operator.ReadyCondition) != nil
	})
	a := ready(c, ns, "simple")
	if a.Status.Charm.Image != "jk-registry.jk-system.svc:5000/charms@"+digestSimple || a.Status.Charm.Base != "ubuntu@22.04" ||
		a.Status.Charm.Metadata == nil || a.Status.Charm.ConfigSchema == nil || a.Status.Charm.ResourceImages["img"] != "busybox:1" {
		t.Errorf("status.charm = %+v", a.Status.Charm)
	}
	if a.Status.ObservedGeneration != a.Generation {
		t.Errorf("observedGeneration %d, generation %d", a.Status.ObservedGeneration, a.Generation)
	}
	if cond := meta.FindStatusCondition(a.Status.Conditions, operator.ReadyCondition); cond.Status != metav1.ConditionFalse || cond.Reason != "UnitsNotReady" {
		t.Errorf("ready condition %+v", cond)
	}
	found := false
	for _, f := range a.Finalizers {
		found = found || f == operator.Finalizer
	}
	if !found {
		t.Errorf("finalizers %v", a.Finalizers)
	}

	// The StatefulSet controller doesn't run here: report readiness ourselves.
	sts.Status.Replicas, sts.Status.ReadyReplicas = 2, 2
	if err := c.Status().Update(ctx, &sts); err != nil {
		t.Fatal(err)
	}
	eventually(t, "Ready condition", func() bool {
		a := ready(c, ns, "simple")
		cond := meta.FindStatusCondition(a.Status.Conditions, operator.ReadyCondition)
		return cond != nil && cond.Status == metav1.ConditionTrue
	})
	// status.charm is cached: later reconciles don't read the registry again.
	before := opCharms.called(digestSimple)
	a = ready(c, ns, "simple")
	three := int32(3)
	a.Spec.Scale = &three
	if err := c.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	eventually(t, "scale applied", func() bool {
		var s appsv1.StatefulSet
		return get(c, ns, "simple", &s) == nil && *s.Spec.Replicas == 3
	})
	if n := opCharms.called(digestSimple); n != before {
		t.Errorf("registry read again after resolution (%d -> %d)", before, n)
	}
}

func TestSpecResourcesOverrideAndStorage(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "store", digestStore, 1)
	// The charm's resource has no upstream-source: the Application is blocked until spec.resources names one.
	eventually(t, "InvalidApplication", func() bool {
		a := ready(c, ns, "store")
		cond := meta.FindStatusCondition(a.Status.Conditions, operator.ReadyCondition)
		return cond != nil && cond.Reason == "InvalidApplication"
	})
	if !gone(c, ns, "store", &appsv1.StatefulSet{}) {
		t.Error("statefulset created for an invalid application")
	}
	app = ready(c, ns, "store")
	app.Spec.Resources = map[string]string{"img": "example.com/data:9"}
	if err := c.Update(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	var sts appsv1.StatefulSet
	eventually(t, "statefulset", func() bool { return exists(c, ns, "store", &sts) })
	if sts.Spec.Template.Spec.Containers[1].Image != "example.com/data:9" || len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Errorf("sts %+v", sts.Spec)
	}
	eventually(t, "Ready reason changes", func() bool {
		cond := meta.FindStatusCondition(ready(c, ns, "store").Status.Conditions, operator.ReadyCondition)
		return cond != nil && cond.Reason == "UnitsNotReady"
	})
}

func TestScaleUpAndDown(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 1)
	replicas := func() int32 {
		var sts appsv1.StatefulSet
		if get(c, ns, "simple", &sts) != nil {
			return -1
		}
		return *sts.Spec.Replicas
	}
	eventually(t, "1 replica", func() bool { return replicas() == 1 })
	for _, want := range []int32{3, 1, 0} {
		a := ready(c, ns, "simple")
		a.Spec.Scale = &want
		if err := c.Update(ctx, a); err != nil {
			t.Fatal(err)
		}
		eventually(t, fmt.Sprintf("%d replicas", want), func() bool { return replicas() == want })
	}
	// Scale 0 with no pods is trivially ready.
	eventually(t, "Ready at scale 0", func() bool {
		cond := meta.FindStatusCondition(ready(c, ns, "simple").Status.Conditions, operator.ReadyCondition)
		return cond != nil && cond.Status == metav1.ConditionTrue
	})
}

func TestUnlabelledNamespaceIsIgnored(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ns := newNamespace(t, c, false)
	newApp(t, c, ns, "simple", digestSimple, 1)
	consistently(t, "operator acted in an unlabelled namespace", func() bool {
		a := ready(c, ns, "simple")
		return len(a.Finalizers) == 0 && a.Status.Charm == nil && gone(c, ns, "simple", &appsv1.StatefulSet{}) && gone(c, ns, "jk-model", &corev1.ConfigMap{})
	})
	// Labelling the namespace later brings the application to life.
	var n corev1.Namespace
	if err := c.Get(context.Background(), client.ObjectKey{Name: ns}, &n); err != nil {
		t.Fatal(err)
	}
	n.Labels = map[string]string{v1alpha1.ModelLabel: "true"}
	if err := c.Update(context.Background(), &n); err != nil {
		t.Fatal(err)
	}
	eventually(t, "statefulset after labelling", func() bool { return exists(c, ns, "simple", &appsv1.StatefulSet{}) })
	// Deleting an Application in an unlabelled namespace never blocks.
	other := newNamespace(t, c, false)
	a := newApp(t, c, other, "simple", digestSimple, 1)
	if err := c.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	eventually(t, "deletion", func() bool { return gone(c, other, "simple", &v1alpha1.Application{}) })
}

func TestModelConfigMap(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	labelled := newNamespace(t, c, true)
	unlabelled := newNamespace(t, c, false)
	var cm corev1.ConfigMap
	eventually(t, "jk-model", func() bool { return exists(c, labelled, v1alpha1.ModelConfigMap, &cm) })
	for k, v := range map[string]string{"relation-id": "0", "action-id": "0", "model-config.update-status-hook-interval": "5m"} {
		if cm.Data[k] != v {
			t.Errorf("jk-model[%s] = %q, want %q", k, cm.Data[k], v)
		}
	}
	consistently(t, "jk-model created in an unlabelled namespace", func() bool { return gone(c, unlabelled, v1alpha1.ModelConfigMap, &corev1.ConfigMap{}) })

	// A ConfigMap that exists is never touched; a deleted one comes back.
	pre := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: unlabelled, Name: v1alpha1.ModelConfigMap}, Data: map[string]string{"relation-id": "41"}}
	if err := c.Create(ctx, pre); err != nil {
		t.Fatal(err)
	}
	var n corev1.Namespace
	_ = c.Get(ctx, client.ObjectKey{Name: unlabelled}, &n)
	n.Labels = map[string]string{v1alpha1.ModelLabel: "true"}
	if err := c.Update(ctx, &n); err != nil {
		t.Fatal(err)
	}
	consistently(t, "existing jk-model overwritten", func() bool {
		var got corev1.ConfigMap
		return get(c, unlabelled, v1alpha1.ModelConfigMap, &got) == nil && got.Data["relation-id"] == "41" && len(got.Data) == 1
	})
	if err := c.Delete(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	eventually(t, "jk-model recreated", func() bool { return exists(c, labelled, v1alpha1.ModelConfigMap, &corev1.ConfigMap{}) })
}

func TestLeaderAssignmentAndReassignment(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 3)
	eventually(t, "lease", func() bool { return exists(c, ns, "simple-leader", &coordinationv1.Lease{}) })
	var lease coordinationv1.Lease
	_ = get(c, ns, "simple-leader", &lease)
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 60 || leaseHolder(c, ns, "simple") != "" {
		t.Fatalf("fresh lease %+v", lease.Spec)
	}

	// Nobody ready: nobody leads. Pods 1 and 2 become ready: the lowest ready ordinal gets the lease.
	createPod(t, c, ns, "simple", 0, false)
	createPod(t, c, ns, "simple", 1, false)
	createPod(t, c, ns, "simple", 2, false)
	consistently(t, "lease assigned with no ready pod", func() bool { return leaseHolder(c, ns, "simple") == "" })
	setPodReady(t, c, ns, "simple-2", true)
	eventually(t, "simple/2 leads", func() bool { return leaseHolder(c, ns, "simple") == "simple/2" })
	setPodReady(t, c, ns, "simple-1", true)
	eventually(t, "status.leader", func() bool { return ready(c, ns, "simple").Status.Leader == "simple/2" })
	// A valid holder is not displaced by a lower ready unit.
	setPodReady(t, c, ns, "simple-0", true)
	consistently(t, "leader moved while its lease was valid", func() bool { return leaseHolder(c, ns, "simple") == "simple/2" })

	// The agent stops renewing: once expired the lowest ready unit takes over, with a fresh lease.
	_ = get(c, ns, "simple-leader", &lease)
	old := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
	lease.Spec.RenewTime = &old
	if err := c.Update(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	eventually(t, "simple/0 takes over", func() bool { return leaseHolder(c, ns, "simple") == "simple/0" })
	_ = get(c, ns, "simple-leader", &lease)
	if time.Since(lease.Spec.RenewTime.Time) > 10*time.Second || lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions != 1 {
		t.Errorf("reassigned lease %+v", lease.Spec)
	}
	eventually(t, "status.leader follows", func() bool { return ready(c, ns, "simple").Status.Leader == "simple/0" })

	// Scaling below the leader's ordinal moves leadership at once.
	scale := int32(1)
	a := ready(c, ns, "simple")
	a.Spec.Scale = &scale
	if err := c.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	consistently(t, "leader changed although simple/0 is below scale", func() bool { return leaseHolder(c, ns, "simple") == "simple/0" })
	// Kill the leader's readiness and let its lease lapse: with nobody else under scale, leadership stays vacant-ish.
	_ = get(c, ns, "simple-leader", &lease)
	lease.Spec.RenewTime = &old
	if err := c.Update(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	setPodReady(t, c, ns, "simple-0", false)
	eventually(t, "no valid leader", func() bool { return ready(c, ns, "simple").Status.Leader == "" })
}

func TestLeaderAssignedBeforePodReady(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 2)
	createPod(t, c, ns, "simple", 0, false)
	createPod(t, c, ns, "simple", 1, false)
	setCharmRunning(t, c, ns, "simple-1")
	start := time.Now()
	// Running but not Ready is enough, and the holder appears quickly.
	eventually(t, "simple/1 leads while not Ready", func() bool { return leaseHolder(c, ns, "simple") == "simple/1" })
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("assignment took %s", d)
	}
	// Lowest ordinal preferred for a new election, but a valid holder is kept.
	setCharmRunning(t, c, ns, "simple-0")
	consistently(t, "valid holder displaced", func() bool { return leaseHolder(c, ns, "simple") == "simple/1" })
}

func TestLeaderMovesWhenScaledAway(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 2)
	createPod(t, c, ns, "simple", 0, true)
	createPod(t, c, ns, "simple", 1, true)
	eventually(t, "simple/0 leads", func() bool { return leaseHolder(c, ns, "simple") == "simple/0" })
	// Make simple/1 the holder, as after a failover, then scale to 1.
	var lease coordinationv1.Lease
	_ = get(c, ns, "simple-leader", &lease)
	h := "simple/1"
	lease.Spec.HolderIdentity = &h
	if err := c.Update(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	scale := int32(1)
	a := ready(c, ns, "simple")
	a.Spec.Scale = &scale
	if err := c.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	eventually(t, "simple/0 leads after scale-down", func() bool { return leaseHolder(c, ns, "simple") == "simple/0" })
}

func TestFinalizerWaitsForPods(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "simple", digestSimple, 2)
	eventually(t, "statefulset", func() bool { return exists(c, ns, "simple", &appsv1.StatefulSet{}) })
	eventually(t, "finalizer", func() bool { return len(ready(c, ns, "simple").Finalizers) == 1 })
	createPod(t, c, ns, "simple", 0, true)
	createPod(t, c, ns, "simple", 1, true)

	if err := c.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	// Scaled to zero, and held while the pods (running their stop hooks) exist.
	eventually(t, "scaled to zero", func() bool {
		var sts appsv1.StatefulSet
		return get(c, ns, "simple", &sts) == nil && *sts.Spec.Replicas == 0
	})
	consistently(t, "application released while pods exist", func() bool { return ready(c, ns, "simple") != nil })
	if a := ready(c, ns, "simple"); a.DeletionTimestamp == nil {
		t.Fatal("application not deleting")
	}
	// A re-created sts must not be scaled up while deleting: the reconciler doesn't touch it any more.
	_ = c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "simple-0"}})
	consistently(t, "application released with a pod left", func() bool { return ready(c, ns, "simple") != nil })
	_ = c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "simple-1"}})
	eventually(t, "application released", func() bool { return ready(c, ns, "simple") == nil })
}

func TestFinalizerDroppedInTerminatingNamespace(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "simple", digestSimple, 1)
	eventually(t, "finalizer", func() bool { a := ready(c, ns, "simple"); return a != nil && len(a.Finalizers) == 1 })
	createPod(t, c, ns, "simple", 0, true)

	// envtest has no namespace controller: the namespace stays Terminating, which is all we need.
	if err := c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "namespace terminating", func() bool {
		var n corev1.Namespace
		return c.Get(ctx, client.ObjectKey{Name: ns}, &n) == nil && n.DeletionTimestamp != nil
	})
	if err := c.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	// The pod is still there; the finalizer is dropped anyway.
	eventually(t, "application released immediately", func() bool { return ready(c, ns, "simple") == nil })
	if !exists(c, ns, "simple-0", &corev1.Pod{}) {
		t.Error("test setup: pod should still exist")
	}
}

func TestUnitDataCleanup(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "simple", digestSimple, 1)
	eventually(t, "statefulset", func() bool { return exists(c, ns, "simple", &appsv1.StatefulSet{}) })
	app = ready(c, ns, "simple")

	owned := func(name string) *v1alpha1.UnitData {
		return &v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(app, v1alpha1.GroupVersion.WithKind("Application"))}}}
	}
	foreign := &v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "other-1"}}
	// Unit 1 is above the scale but still terminating; its pod exists before its UnitData does, so a busy operator can't see one without the other.
	createPod(t, c, ns, "simple", 1, true)
	for _, u := range []*v1alpha1.UnitData{owned("simple-0"), owned("simple-1"), owned("simple-2"), foreign} {
		if err := c.Create(ctx, u); err != nil {
			t.Fatal(err)
		}
	}

	// simple-2 has no pod: deleted. simple-1 waits for its pod. simple-0 (in scale) and other-1 stay.
	eventually(t, "simple-2 deleted", func() bool { return gone(c, ns, "simple-2", &v1alpha1.UnitData{}) })
	consistently(t, "UnitData deleted too early or wrongly", func() bool {
		return exists(c, ns, "simple-1", &v1alpha1.UnitData{}) && exists(c, ns, "simple-0", &v1alpha1.UnitData{}) && exists(c, ns, "other-1", &v1alpha1.UnitData{})
	})
	if err := c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "simple-1"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "simple-1 deleted after its pod is gone", func() bool { return gone(c, ns, "simple-1", &v1alpha1.UnitData{}) })
	if !exists(c, ns, "simple-0", &v1alpha1.UnitData{}) || !exists(c, ns, "other-1", &v1alpha1.UnitData{}) {
		t.Error("UnitData that must stay was deleted")
	}
}

func TestStatusFromAppDataAndUnavailableCharm(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 1)
	eventually(t, "statefulset", func() bool { return exists(c, ns, "simple", &appsv1.StatefulSet{}) })
	app := ready(c, ns, "simple")
	ad := &v1alpha1.AppData{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "simple", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(app, v1alpha1.GroupVersion.WithKind("Application"))}},
		Spec:       v1alpha1.AppDataSpec{Status: &v1alpha1.WorkloadStatus{State: "active", Message: "all good"}},
	}
	if err := c.Create(ctx, ad); err != nil {
		t.Fatal(err)
	}
	eventually(t, "application status from AppData", func() bool {
		s := ready(c, ns, "simple").Status.Status
		return s != nil && s.State == "active" && s.Message == "all good"
	})

	// A digest the registry doesn't have: Ready=False/CharmUnavailable, retried later, nothing built.
	newApp(t, c, ns, "missing", "sha256:dead", 1)
	eventually(t, "CharmUnavailable", func() bool {
		cond := meta.FindStatusCondition(ready(c, ns, "missing").Status.Conditions, operator.ReadyCondition)
		return cond != nil && cond.Reason == "CharmUnavailable"
	})
	if !gone(c, ns, "missing", &appsv1.StatefulSet{}) {
		t.Error("statefulset for an unavailable charm")
	}
}
