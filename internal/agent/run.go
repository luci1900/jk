package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/luci1900/jk/api/v1alpha1"
)

// TerminationGrace is the time stop and remove get after SIGTERM: the pod's grace period (juju's 30s) less a margin.
const TerminationGrace = 25 * time.Second

// Scheme is the scheme the agent uses.
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// Identity is the unit's identity from the environment the operator sets.
type Identity struct {
	Pod, Namespace, ModelName, ModelUUID, App string
	Ordinal                                   int
	Containers                                []string
}

// IdentityFromEnv reads JK_POD_NAME, JK_NAMESPACE, JK_MODEL_NAME, JK_MODEL_UUID, JK_APP and JUJU_CONTAINER_NAMES.
// JK_APP defaults to the pod name without its ordinal; JK_NAMESPACE to the service account namespace.
func IdentityFromEnv(getenv func(string) string) (Identity, error) {
	id := Identity{
		Pod: getenv("JK_POD_NAME"), Namespace: getenv("JK_NAMESPACE"), ModelName: getenv("JK_MODEL_NAME"),
		ModelUUID: getenv("JK_MODEL_UUID"), App: getenv("JK_APP"), Containers: splitList(getenv("JUJU_CONTAINER_NAMES")),
	}
	i := strings.LastIndexByte(id.Pod, '-')
	if i <= 0 {
		return id, fmt.Errorf("JK_POD_NAME %q is not <app>-<ordinal>", id.Pod)
	}
	n, err := strconv.Atoi(id.Pod[i+1:])
	if err != nil {
		return id, fmt.Errorf("JK_POD_NAME %q is not <app>-<ordinal>", id.Pod)
	}
	id.Ordinal = n
	if id.App == "" {
		id.App = id.Pod[:i]
	}
	if id.Namespace == "" {
		if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			id.Namespace = strings.TrimSpace(string(b))
		}
	}
	if id.Namespace == "" {
		return id, fmt.Errorf("JK_NAMESPACE is not set")
	}
	if id.ModelName == "" {
		id.ModelName = id.Namespace
	}
	return id, nil
}

// RunOptions are the process-level inputs of Run.
type RunOptions struct {
	Identity      Identity
	DataDir       string
	ContainersDir string
	Self          string
	RestConfig    *rest.Config // in-cluster config by default
	Log           func(string, ...any)
}

// WillComeBack is the termination decision of docs/design.md (Unit agent; juju's caasunitterminationworker): the unit comes
// back, so stop and remove must not run, when its ordinal is below spec.scale and the Application is not being deleted.
func WillComeBack(app *v1alpha1.Application, ordinal int) bool {
	if app == nil || app.DeletionTimestamp != nil {
		return false
	}
	scale := int32(1)
	if app.Spec.Scale != nil {
		scale = *app.Spec.Scale
	}
	return int32(ordinal) < scale
}

// Run runs the unit agent until ctx ends, which is the termination notice (SIGTERM, wired up by the caller). It then
// decides between a restart (exit at once) and removal (run stop and remove within the grace period, then exit).
func Run(ctx context.Context, opts RunOptions) error {
	logf := opts.Log
	if logf == nil {
		logf = func(f string, a ...any) { fmt.Fprintf(os.Stdout, "jk-agent: "+f+"\n", a...) }
	}
	ctrl.SetLogger(logr.FromSlogHandler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	id := opts.Identity
	cfg := opts.RestConfig
	if cfg == nil {
		var err error
		if cfg, err = rest.InClusterConfig(); err != nil {
			return fmt.Errorf("in-cluster config: %w", err)
		}
	}
	// client-go's defaults (5 QPS, burst 10) throttle a hook's dozen API calls into about a second per hook.
	cfg = rest.CopyConfig(cfg)
	if cfg.QPS == 0 {
		cfg.QPS = 50
	}
	if cfg.Burst == 0 {
		cfg.Burst = 100
	}
	ns := id.Namespace
	unit := fmt.Sprintf("%s/%d", id.App, id.Ordinal)
	// The cache holds the namespace's Actions (history included), but only this unit's keep their content.
	slimAction := func(obj any) (any, error) {
		if act, ok := obj.(*v1alpha1.Action); ok && act.Spec.Unit != unit {
			slim := &v1alpha1.Action{ObjectMeta: act.ObjectMeta, Spec: v1alpha1.ActionSpec{Unit: act.Spec.Unit, Name: act.Spec.Name}}
			slim.Status.ID, slim.Status.State = act.Status.ID, act.Status.State
			return slim, nil
		}
		return obj, nil
	}
	byName := func(name string) cache.ByObject {
		return cache.ByObject{Field: fields.OneTermEqualSelector("metadata.name", name)}
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 Scheme(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{ns: {}},
			ByObject: map[client.Object]cache.ByObject{
				&v1alpha1.Application{}: byName(id.App),
				&corev1.ConfigMap{}:     byName(v1alpha1.ModelConfigMap),
				&coordinationv1.Lease{}: byName(LeaseName(id.App)),
				&v1alpha1.Action{}:      {Transform: slimAction},
			},
		},
	})
	if err != nil {
		return err
	}
	direct, err := client.New(cfg, client.Options{Scheme: Scheme()})
	if err != nil {
		return err
	}
	kube := &KubeClient{Reader: mgr.GetClient(), Client: direct, Namespace: ns, App: id.App, Unit: unit, Pod: id.Pod}
	// Termination decisions and the first reads must not depend on the cache.
	freshKube := &KubeClient{Reader: direct, Client: direct, Namespace: ns, App: id.App, Unit: unit, Pod: id.Pod}

	a := New(Config{
		App: id.App, Unit: unit, Ordinal: id.Ordinal, Namespace: ns, ModelName: id.ModelName, ModelUUID: id.ModelUUID,
		DataDir: opts.DataDir, ContainersDir: opts.ContainersDir, Containers: id.Containers, Self: opts.Self,
		Kube: kube,
	})

	// All events collapse into one request: this unit.
	key := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: unit}}
	toUnit := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{key}
	})
	// Only Actions addressed to this unit wake it.
	toUnitActions := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		if act, ok := o.(*v1alpha1.Action); ok && act.Spec.Unit == unit {
			return []reconcile.Request{key}
		}
		return nil
	})
	events := make(chan event.GenericEvent, 1)
	a.Trigger = func() {
		select {
		case events <- event.GenericEvent{Object: &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: id.App}}}:
		default:
		}
	}
	err = ctrl.NewControllerManagedBy(mgr).
		Named("unit").
		Watches(&v1alpha1.Application{}, toUnit).
		Watches(&corev1.ConfigMap{}, toUnit).
		Watches(&coordinationv1.Lease{}, toUnit).
		Watches(&v1alpha1.Relation{}, toUnit).
		Watches(&v1alpha1.UnitData{}, toUnit).
		Watches(&v1alpha1.AppData{}, toUnit).
		Watches(&v1alpha1.RemoteData{}, toUnit).
		Watches(&v1alpha1.Action{}, toUnitActions).
		WatchesRawSource(source.Channel(events, toUnit)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1, SkipNameValidation: ptr(true)}).
		Complete(reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
			d, err := a.Reconcile(ctx)
			return reconcile.Result{RequeueAfter: d}, err
		}))
	if err != nil {
		return err
	}
	// Background loops: initialisation, the lease renewal loop and Pebble polling.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		for {
			if err := a.Init(ctx); err == nil {
				break
			} else if ctx.Err() == nil {
				logf("initialising: %v", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
		}
		go a.lead.Run(ctx, a.trigger, logf)
		go a.peb.Run(ctx, a.trigger)
		a.trigger()
		<-ctx.Done()
		return nil
	})); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(runCtx) }()

	select {
	case err := <-mgrDone:
		return err
	case <-ctx.Done():
	}

	// Terminating: will this unit come back (restart, drain, eviction) or go away (scale-down, removal)?
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	app, err := freshKube.Application(dctx)
	dcancel()
	comesBack := true
	switch {
	case err == nil:
		comesBack = WillComeBack(app, id.Ordinal)
	case apierrors.IsNotFound(err):
		comesBack = false
	}
	if comesBack {
		logf("terminating; unit will come back, not running stop and remove")
		cancel()
		<-mgrDone
		return nil
	}
	logf("terminating; unit is going away, running stop and remove")
	a.Terminate()
	select {
	case <-a.Done():
	case <-time.After(TerminationGrace):
		logf("grace period over before stop and remove finished")
	case err := <-mgrDone:
		return err
	}
	cancel()
	<-mgrDone
	return nil
}

func ptr[T any](v T) *T { return &v }
