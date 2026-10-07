package operator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/registry"
)

const (
	// Finalizer is the only finalizer jk uses (docs/design.md, Operator).
	Finalizer = v1alpha1.Group + "/application"
	// FieldOwner is the server-side-apply field manager.
	FieldOwner = "jk-operator"
	// ReadyCondition is the Application's summary condition.
	ReadyCondition = "Ready"
	// CharmUpToDateCondition is true while status.charm is the charm spec asks for; it turns false, with the reason,
	// when a refresh cannot be completed (the previous charm keeps running).
	CharmUpToDateCondition = "CharmUpToDate"
)

// CharmSource reads a charm from jk-registry; *registry.Client implements it.
type CharmSource interface {
	Charm(ctx context.Context, digest string) (*registry.Charm, error)
}

// ApplicationReconciler reconciles Applications in jk model namespaces.
type ApplicationReconciler struct {
	client.Client
	// Charms reads charm images from jk-registry.
	Charms CharmSource
	// Hub and Store resolve, download and cache Charmhub charms; nil disables Charmhub (local charms only).
	Hub   CharmHub
	Store CharmStore
	// Reader reads without caching (the manager's API reader); Client is used when nil.
	Reader client.Reader
	Config Config
	// Now is time.Now unless a test overrides it.
	Now func() time.Time

	verified  verifiedCache
	refreshes refreshCache
}

func (r *ApplicationReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// SetupWithManager registers the controller and its watches.
func (r *ApplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	appsInNamespace := func(ctx context.Context, ns string) []reconcile.Request {
		var apps v1alpha1.ApplicationList
		if err := r.List(ctx, &apps, client.InNamespace(ns)); err != nil {
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(apps.Items))
		for _, a := range apps.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&a)})
		}
		return reqs
	}
	byAppLabel := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		if app := o.GetLabels()[v1alpha1.AppLabel]; app != "" {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: app}}}
		}
		return nil
	})
	byNameLabel := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		if app := o.GetLabels()[nameLabel]; app != "" {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: app}}}
		}
		return nil
	})
	// UnitData is created by agents; map by owner reference, else by its "<app>-<n>" name.
	unitData := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		for _, ref := range o.GetOwnerReferences() {
			if ref.Kind == "Application" {
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: ref.Name}}}
			}
		}
		if i := strings.LastIndex(o.GetName(), "-"); i > 0 {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: o.GetName()[:i]}}}
		}
		return nil
	})
	namespaces := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		return appsInNamespace(ctx, o.GetName())
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("application").
		For(&v1alpha1.Application{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&coordinationv1.Lease{}).
		Owns(&v1alpha1.AppData{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&v1alpha1.Relation{}).
		Watches(&v1alpha1.Relation{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			rel, ok := o.(*v1alpha1.Relation)
			if !ok {
				return nil
			}
			var reqs []reconcile.Request
			for _, e := range rel.Spec.Endpoints {
				reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: rel.Namespace, Name: e.Application}})
			}
			return reqs
		})).
		Watches(&corev1.Pod{}, byAppLabel, builder.WithPredicates(hasAppLabel())).
		Watches(&corev1.PersistentVolumeClaim{}, byNameLabel, builder.WithPredicates(hasNameLabel())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			return secretRequests(o)
		})).
		Watches(&v1alpha1.UnitData{}, unitData).
		Watches(&corev1.Namespace{}, namespaces).
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}). // charm downloads can take a while
		Complete(r)
}

func hasNameLabel() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetLabels()[nameLabel] != "" })
}

func hasAppLabel() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetLabels()[v1alpha1.AppLabel] != "" })
}

// Reconcile drives one Application towards its desired objects.
func (r *ApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var app v1alpha1.Application
	if err := r.Get(ctx, req.NamespacedName, &app); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var ns corev1.Namespace
	nsErr := r.Get(ctx, client.ObjectKey{Name: app.Namespace}, &ns)
	if nsErr != nil && !apierrors.IsNotFound(nsErr) {
		return ctrl.Result{}, nsErr
	}
	nsGone := apierrors.IsNotFound(nsErr) || ns.DeletionTimestamp != nil

	if app.DeletionTimestamp != nil {
		return r.finalize(ctx, &app, nsGone || ns.Labels[v1alpha1.ModelLabel] != "true")
	}
	if nsGone || ns.Labels[v1alpha1.ModelLabel] != "true" {
		return ctrl.Result{}, nil // not a jk model (or on its way out): act only in labelled namespaces
	}

	if controllerutil.AddFinalizer(&app, Finalizer) {
		if err := r.Update(ctx, &app); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := EnsureModelConfigMap(ctx, r.Client, app.Namespace); err != nil {
		return ctrl.Result{}, err
	}

	orig := app.DeepCopy()
	if c := app.Status.Charm; r.Reader != nil && (c == nil || c.Image == "" || c.Pin != PinKey(&app)) {
		// The cache may not have our own last status patch yet (the finalizer update re-queues us at once).
		// Resolving twice would download the charm twice, so look at the live object before doing it.
		var fresh v1alpha1.Application
		if err := r.Reader.Get(ctx, req.NamespacedName, &fresh); err == nil {
			app.Status, orig.Status = *fresh.Status.DeepCopy(), *fresh.Status.DeepCopy()
		}
	}
	status := &app.Status
	status.ObservedGeneration = app.Generation
	res, err := r.reconcileObjects(ctx, &app, string(ns.UID))
	if err != nil && !errors.Is(err, errInvalid) && !errors.Is(err, errCharmUnavailable) && !errors.Is(err, errCharmNotFound) {
		return res, err // transient: retry with backoff, status unchanged
	}
	if err != nil {
		// Permanent (or already reported) problems become the Ready condition; the spec changing re-triggers.
		setReady(&app, metav1.ConditionFalse, reasonFor(err), err.Error())
	}
	if !equality.Semantic.DeepEqual(orig.Status, app.Status) {
		if serr := r.Status().Patch(ctx, &app, client.MergeFrom(orig)); serr != nil {
			return res, serr
		}
	}
	switch {
	case errors.Is(err, errCharmUnavailable):
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	case errors.Is(err, errCharmNotFound):
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	case errors.Is(err, errMixedArch) || errors.Is(err, errNoNodes):
		return ctrl.Result{RequeueAfter: cmp.Or(r.Config.ClusterRetry, 5*time.Second)}, nil // the cluster may change under us
	}
	return res, nil
}

var errCharmUnavailable = errors.New("charm unavailable")

func reasonFor(err error) string {
	switch {
	case errors.Is(err, errCharmUnavailable):
		return "CharmUnavailable"
	case errors.Is(err, errCharmNotFound):
		return "CharmNotFound"
	case errors.Is(err, errInvalid):
		return "InvalidApplication"
	}
	return "Error"
}

func setReady(app *v1alpha1.Application, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&app.Status.Conditions, metav1.Condition{
		Type: ReadyCondition, Status: status, Reason: reason, Message: msg, ObservedGeneration: app.Generation,
	})
}

func scaleOf(app *v1alpha1.Application) int32 {
	if app.Spec.Scale != nil {
		return *app.Spec.Scale
	}
	return 1
}

// reconcileObjects resolves the charm, applies the objects, assigns the leader, cleans UnitData and fills
// app.Status in memory (the caller patches it).
func (r *ApplicationReconciler) reconcileObjects(ctx context.Context, app *v1alpha1.Application, nsUID string) (ctrl.Result, error) {
	refreshErr, err := r.resolveCharm(ctx, app)
	if err != nil {
		return ctrl.Result{}, err
	}
	setCharmUpToDate(app, refreshErr)
	md, err := parseMetadata(app.Status.Charm.Metadata)
	if err != nil {
		return ctrl.Result{}, err
	}
	app.Status.Charm.ResourceImages = ResourceImages(md, app.Spec.Resources)

	var units v1alpha1.UnitDataList
	if err := r.List(ctx, &units, client.InNamespace(app.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, client.InNamespace(app.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	var rels v1alpha1.RelationList
	if err := r.List(ctx, &rels, client.InNamespace(app.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	live := map[int64]bool{}
	for i := range rels.Items {
		live[rels.Items[i].Status.ID] = true
	}
	owned, granted := SecretNames(app.Name, secrets.Items, func(id int64) bool { return live[id] })
	desired, err := Build(app, Inputs{
		NamespaceUID: nsUID,
		OpenedPorts:  openedPorts(app, units.Items),
		OwnedSecrets: owned, GrantedSecrets: granted,
	}, r.Config)
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, obj := range desired.Objects() {
		if err := r.apply(ctx, obj); err != nil {
			return ctrl.Result{}, fmt.Errorf("applying %s %s: %w", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), err)
		}
	}
	if err := r.reconcileClusterObjects(ctx, clientApp{app.Name, app.Namespace}, desired.ClusterObjects()); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileVolumes(ctx, app, sortedKeys(md.Storage)); err != nil {
		return ctrl.Result{}, err
	}

	var res ctrl.Result
	lease, err := r.reconcileLease(ctx, app)
	if err != nil {
		return ctrl.Result{}, err
	}
	now := r.now()
	app.Status.Leader = CurrentLeader(&lease.Spec, now)
	if exp := LeaseExpiry(&lease.Spec); !exp.IsZero() && exp.After(now) {
		res.RequeueAfter = exp.Sub(now) + time.Second // look again when renewals might have stopped
	}

	if err := r.cleanupUnitData(ctx, app, units.Items); err != nil {
		return ctrl.Result{}, err
	}

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired.StatefulSet), &sts); err != nil {
		return ctrl.Result{}, err
	}
	var ad v1alpha1.AppData
	app.Status.Status = nil
	if err := r.Get(ctx, client.ObjectKey{Namespace: app.Namespace, Name: app.Name}, &ad); err == nil {
		app.Status.Status = ad.Spec.Status
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	setReadiness(app, sts.Status.ReadyReplicas)
	if err := r.reportUnits(ctx, app, units.Items); err != nil {
		return ctrl.Result{}, err
	}
	if refreshErr != nil {
		retry := 30 * time.Second
		if errors.Is(refreshErr, errCharmNotFound) {
			retry = 5 * time.Minute
		}
		if res.RequeueAfter == 0 || retry < res.RequeueAfter {
			res.RequeueAfter = retry
		}
	}
	return res, nil
}

// reportUnits fills status.units: the charm each existing pod was started with.
func (r *ApplicationReconciler) reportUnits(ctx context.Context, app *v1alpha1.Application, units []v1alpha1.UnitData) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabels{v1alpha1.AppLabel: app.Name}); err != nil {
		return err
	}
	data := map[string]*v1alpha1.UnitData{}
	for i := range units {
		data[units[i].Name] = &units[i]
	}
	app.Status.Units = UnitCharmStatuses(app, pods.Items, data)
	return nil
}

// UnitCharmStatuses reports the charm of each pod: the image and revision recorded on the pod template, whether it is
// the application's current charm, and whether the unit has run it (UnitData.charmURL, empty until the first hooks).
func UnitCharmStatuses(app *v1alpha1.Application, pods []corev1.Pod, data map[string]*v1alpha1.UnitData) []v1alpha1.UnitCharmStatus {
	current := ""
	if app.Status.Charm != nil {
		current = app.Status.Charm.Image
	}
	type unit struct {
		n  int
		st v1alpha1.UnitCharmStatus
	}
	var all []unit
	for i := range pods {
		p := &pods[i]
		n, ok := PodOrdinal(app.Name, p.Name)
		if !ok {
			continue
		}
		image := p.Annotations[v1alpha1.CharmImageAnnotation]
		if image == "" {
			image = podCharmImage(p)
		}
		rev, _ := strconv.Atoi(p.Annotations[v1alpha1.CharmRevisionAnnotation])
		st := v1alpha1.UnitCharmStatus{Name: fmt.Sprintf("%s/%d", app.Name, n), Revision: rev, Image: image}
		ran := true
		if ud := data[p.Name]; ud != nil && ud.Spec.CharmURL != "" && ud.Spec.CharmURL != image {
			ran = false
		}
		st.UpToDate = image != "" && image == current && ran
		all = append(all, unit{n, st})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n < all[j].n })
	var out []v1alpha1.UnitCharmStatus
	for _, u := range all {
		out = append(out, u.st)
	}
	return out
}

// podCharmImage reads the charm image from the init container's arguments, for pods that predate the annotation.
func podCharmImage(p *corev1.Pod) string {
	for _, c := range p.Spec.InitContainers {
		for _, a := range c.Args {
			if v, ok := strings.CutPrefix(a, "--charm-image="); ok {
				return v
			}
		}
	}
	return ""
}

func setCharmUpToDate(app *v1alpha1.Application, refreshErr error) {
	c := metav1.Condition{Type: CharmUpToDateCondition, Status: metav1.ConditionTrue, Reason: "UpToDate", ObservedGeneration: app.Generation}
	if rc := app.Status.Charm; rc != nil && rc.Revision != 0 {
		c.Message = fmt.Sprintf("revision %d", rc.Revision)
	}
	if refreshErr != nil {
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, reasonFor(refreshErr), refreshErr.Error()
	}
	meta.SetStatusCondition(&app.Status.Conditions, c)
}

func setReadiness(app *v1alpha1.Application, ready int32) {
	scale := scaleOf(app)
	if ready >= scale {
		setReady(app, metav1.ConditionTrue, "Ready", fmt.Sprintf("%d/%d units ready", ready, scale))
		return
	}
	setReady(app, metav1.ConditionFalse, "UnitsNotReady", fmt.Sprintf("%d/%d units ready", ready, scale))
}

// resolveCharm fills status.charm: from the registry for local charms, through Charmhub otherwise. When the charm
// is already running and only a refresh fails, the old charm stays in status and the failure is returned as
// refreshErr (reported by the CharmUpToDate condition); err is for problems with no charm to run.
func (r *ApplicationReconciler) resolveCharm(ctx context.Context, app *v1alpha1.Application) (refreshErr, err error) {
	prev := app.Status.Charm.DeepCopy()
	err = r.resolveCharmSource(ctx, app)
	if err == nil {
		return nil, nil
	}
	if prev != nil && prev.Image != "" && prev.Metadata != nil &&
		(errors.Is(err, errInvalid) || errors.Is(err, errCharmUnavailable) || errors.Is(err, errCharmNotFound) || errors.Is(err, errMixedArch) || errors.Is(err, errNoNodes)) {
		app.Status.Charm = prev
		return err, nil
	}
	return nil, err
}

func (r *ApplicationReconciler) resolveCharmSource(ctx context.Context, app *v1alpha1.Application) error {
	c := app.Spec.Charm
	switch c.Source {
	case "", "charmhub":
		return r.resolveCharmhub(ctx, app)
	case "local":
	default:
		return fmt.Errorf("%w: unknown charm source %q", errInvalid, c.Source)
	}
	if c.Sha256 == "" {
		return fmt.Errorf("%w: spec.charm.sha256 is required for local charms", errInvalid)
	}
	digest := NormalizeDigest(c.Sha256)
	if cur := app.Status.Charm; cur != nil && cur.Sha256 == digest && cur.Metadata != nil {
		cur.Pin = PinKey(app)
		return nil
	}
	ch, err := r.Charms.Charm(ctx, digest)
	if err != nil {
		return fmt.Errorf("%w: reading %s from jk-registry: %v", errCharmUnavailable, digest, err)
	}
	rc, err := ResolveCharm(ch, r.Config.RegistryEndpoint)
	if err != nil {
		return fmt.Errorf("%w: %v", errInvalid, err)
	}
	rc.Pin = PinKey(app)
	app.Status.Charm = rc
	return nil
}

// apply server-side-applies obj (status stripped, so the resource's own status is never claimed).
func (r *ApplicationReconciler) apply(ctx context.Context, obj client.Object) error {
	m, err := applyConfiguration(obj)
	if err != nil {
		return err
	}
	return r.Patch(ctx, &unstructured.Unstructured{Object: m}, client.Apply, client.FieldOwner(FieldOwner), client.ForceOwnership)
}

// applyConfiguration converts obj for server-side apply. Typed Go structs serialize empty non-pointer structs as
// `{}`; applying those would claim ownership of fields the operator does not set, so the ones that matter are
// removed: the resource's status, creationTimestamp, and a StatefulSet's updateStrategy (postgresql-k8s patches
// the rollout partition; the operator must neither own nor revert it).
func applyConfiguration(obj client.Object) (map[string]any, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	delete(m, "status")
	if md, ok := m["metadata"].(map[string]any); ok {
		delete(md, "creationTimestamp")
	}
	if spec, ok := m["spec"].(map[string]any); ok {
		if us, ok := spec["updateStrategy"].(map[string]any); ok && len(us) == 0 {
			delete(spec, "updateStrategy")
		}
	}
	return m, nil
}

// reconcileLease creates the leader Lease if needed and assigns its holder.
func (r *ApplicationReconciler) reconcileLease(ctx context.Context, app *v1alpha1.Application) (*coordinationv1.Lease, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabels{v1alpha1.AppLabel: app.Name}); err != nil {
		return nil, err
	}
	ready := EligibleOrdinals(app.Name, scaleOf(app), pods.Items)
	now := r.now()

	key := client.ObjectKey{Namespace: app.Namespace, Name: LeaseName(app.Name)}
	var lease coordinationv1.Lease
	err := r.Get(ctx, key, &lease)
	if apierrors.IsNotFound(err) {
		lease = coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name: key.Name, Namespace: key.Namespace, Labels: Labels(app.Name),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(app, v1alpha1.GroupVersion.WithKind("Application"))},
			},
		}
		if holder, ok := NextLeader(app.Name, scaleOf(app), nil, ready, now); ok {
			lease.Spec = NewLeaseSpec(nil, holder, now)
		} else {
			d := int32(LeaseDurationSeconds)
			lease.Spec.LeaseDurationSeconds = &d
		}
		if err := r.Create(ctx, &lease); err != nil {
			return nil, err
		}
		return &lease, nil
	} else if err != nil {
		return nil, err
	}
	if holder, ok := NextLeader(app.Name, scaleOf(app), &lease.Spec, ready, now); ok {
		lease.Spec = NewLeaseSpec(&lease.Spec, holder, now)
		if err := r.Update(ctx, &lease); err != nil { // optimistic: loses to a concurrent renewal
			return nil, err
		}
	}
	return &lease, nil
}

// cleanupUnitData deletes UnitData of units above spec.scale once their pods are gone, so a reused unit
// name starts clean (docs/design.md, API). Waiting for the pod lets the unit's stop hooks commit first.
func (r *ApplicationReconciler) cleanupUnitData(ctx context.Context, app *v1alpha1.Application, units []v1alpha1.UnitData) error {
	scale := scaleOf(app)
	for i := range units {
		u := &units[i]
		n, ok := PodOrdinal(app.Name, u.Name)
		if !ok || int32(n) < scale || !ownedByOrUnowned(u, app) {
			continue
		}
		err := r.Get(ctx, client.ObjectKey{Namespace: app.Namespace, Name: u.Name}, &corev1.Pod{})
		if err == nil {
			continue // pod still terminating
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if err := r.Delete(ctx, u); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func ownedByOrUnowned(o client.Object, app *v1alpha1.Application) bool {
	refs := o.GetOwnerReferences()
	if len(refs) == 0 {
		return true
	}
	for _, ref := range refs {
		if ref.UID == app.UID {
			return true
		}
	}
	return false
}

// finalize runs the Application finalizer: scale to zero, wait for the pods (their agents run stop hooks),
// clean up cluster-scoped objects, release. dropNow skips waiting (namespace deleting or not a model).
func (r *ApplicationReconciler) finalize(ctx context.Context, app *v1alpha1.Application, dropNow bool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(app, Finalizer) {
		return ctrl.Result{}, nil
	}
	if !dropNow {
		// The application's relations go first, in order: both sides run relation-departed and relation-broken
		// while the pods still run. Their finalizer holds them until the units have, so wait for them to be gone.
		waiting, err := r.removeRelations(ctx, app)
		if err != nil {
			return ctrl.Result{}, err
		}
		if waiting {
			return ctrl.Result{RequeueAfter: relationRecheck}, nil
		}
		var sts appsv1.StatefulSet
		err = r.Get(ctx, client.ObjectKey{Namespace: app.Namespace, Name: app.Name}, &sts)
		if err == nil && (sts.Spec.Replicas == nil || *sts.Spec.Replicas != 0) {
			patch := client.MergeFrom(sts.DeepCopy())
			zero := int32(0)
			sts.Spec.Replicas = &zero
			if err := r.Patch(ctx, &sts, patch); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(app.Namespace), client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(PodLabels(app.Name))}); err != nil {
			return ctrl.Result{}, err
		}
		if len(pods.Items) > 0 {
			// Pod deletions re-trigger us; the requeue is a safety net.
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	// Cluster-scoped objects and volumes must be handled even when the namespace is going away.
	if err := r.cleanupClusterObjects(ctx, app); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(app, Finalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Update(ctx, app))
}

// removeRelations deletes the application's relations with other applications (peer relations go with the
// application) and reports whether any is still there.
func (r *ApplicationReconciler) removeRelations(ctx context.Context, app *v1alpha1.Application) (bool, error) {
	var rels v1alpha1.RelationList
	if err := r.List(ctx, &rels, client.InNamespace(app.Namespace)); err != nil {
		return false, err
	}
	waiting := false
	for i := range rels.Items {
		rel := &rels.Items[i]
		if isPeerRelation(rel) || !relationMentions(rel, app.Name) {
			continue
		}
		waiting = true
		if rel.DeletionTimestamp == nil {
			if err := r.Delete(ctx, rel); client.IgnoreNotFound(err) != nil {
				return false, err
			}
		}
	}
	return waiting, nil
}

// cleanupClusterObjects deletes the cluster-scoped objects labelled with the application (trust: cluster
// bindings) and, if the Application asks for it (jk.luci1900.github.io/destroy-storage: "true"), marks its volumes for
// deletion. Charm-created cluster-scoped objects come with the admission policy of docs/design.md (Security).
func (r *ApplicationReconciler) cleanupClusterObjects(ctx context.Context, app *v1alpha1.Application) error {
	if err := r.deleteClusterObjects(ctx, clientApp{app.Name, app.Namespace}, nil); err != nil {
		return err
	}
	if app.Annotations[v1alpha1.DestroyStorageAnnotation] == "true" {
		return r.destroyVolumes(ctx, app, storageNames(app))
	}
	return nil
}

// openedPorts is the union of the opened ports of the app's current units (ordinals below the scale).
func openedPorts(app *v1alpha1.Application, units []v1alpha1.UnitData) []v1alpha1.PortRange {
	scale := scaleOf(app)
	var all [][]v1alpha1.PortRange
	for i := range units {
		u := &units[i]
		if n, ok := PodOrdinal(app.Name, u.Name); ok && int32(n) < scale && ownedByOrUnowned(u, app) {
			all = append(all, u.Spec.OpenedPorts)
		}
	}
	return UnionPorts(all...)
}
