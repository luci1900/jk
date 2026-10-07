package operator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
)

// Counter keys in the jk-model ConfigMap.
const (
	RelationIDKey = "relation-id"
	ActionIDKey   = "action-id"
)

func peerMeta(m metav1.ObjectMeta, endpoint string) metav1.ObjectMeta {
	labels := map[string]string{}
	for k, v := range m.Labels {
		labels[k] = v
	}
	labels[v1alpha1.PeerEndpointLabel] = endpoint
	m.Labels = labels
	return m
}

// AllocateID takes the next id from a counter of the namespace's jk-model ConfigMap. The counter holds the
// last id handed out (ids start at 1, so 0 can mean "unassigned"); it is advanced with an optimistic update, so
// concurrent allocators never get the same id, and an id lost to a failure later is never reused. reader should
// bypass caches (the manager's API reader) so a stale read only costs a retry.
func AllocateID(ctx context.Context, reader client.Reader, w client.Writer, namespace, key string) (int64, error) {
	var id int64
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cm corev1.ConfigMap
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: v1alpha1.ModelConfigMap}, &cm); err != nil {
			return err
		}
		last := int64(0)
		if v, ok := cm.Data[key]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return fmt.Errorf("%s: counter %q is %q, not a non-negative integer", v1alpha1.ModelConfigMap, key, v)
			}
			last = n
		}
		id = last + 1
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[key] = strconv.FormatInt(id, 10)
		return w.Update(ctx, &cm)
	})
	return id, err
}

// RelationFinalizer holds a non-peer Relation until the units of both applications have run relation-departed and
// relation-broken for it (their UnitData no longer has it in scope).
const RelationFinalizer = v1alpha1.Group + "/relation"

// relationRecheck is how often a Relation waiting for its units looks again, as a safety net for missed events.
const relationRecheck = 5 * time.Second

// RelationReconciler validates Relations, assigns their ids and runs their orderly removal. Peer Relations are
// created by the Application controller.
type RelationReconciler struct {
	client.Client
	// Reader reads without caching; the manager's API reader. Client is used when nil.
	Reader client.Reader
}

func (r *RelationReconciler) reader() client.Reader {
	if r.Reader != nil {
		return r.Reader
	}
	return r.Client
}

// Reconcile validates a Relation in a jk model, gives it its id once it is valid, and removes it in order.
func (r *RelationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rel v1alpha1.Relation
	if err := r.Get(ctx, req.NamespacedName, &rel); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if isMirror(&rel) {
		return r.reconcileMirror(ctx, &rel)
	}
	var ns corev1.Namespace
	nsErr := r.Get(ctx, client.ObjectKey{Name: rel.Namespace}, &ns)
	if nsErr != nil && !apierrors.IsNotFound(nsErr) {
		return ctrl.Result{}, nsErr
	}
	nsGone := apierrors.IsNotFound(nsErr) || ns.DeletionTimestamp != nil
	if rel.DeletionTimestamp != nil {
		return r.finalize(ctx, &rel, nsGone || ns.Labels[v1alpha1.ModelLabel] != "true")
	}
	if nsGone || ns.Labels[v1alpha1.ModelLabel] != "true" {
		return ctrl.Result{}, nil
	}
	if !isPeerRelation(&rel) && controllerutil.AddFinalizer(&rel, RelationFinalizer) {
		if err := r.Update(ctx, &rel); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := EnsureModelConfigMap(ctx, r.Client, rel.Namespace); err != nil {
		return ctrl.Result{}, err
	}

	orig := rel.DeepCopy()
	prob, err := r.validate(ctx, &rel, rel.Status.ID == 0)
	if err != nil {
		return ctrl.Result{}, err
	}
	setRelationValid(&rel, prob)
	if prob == nil && rel.Status.ID == 0 || prob == nil && crossNamespace(&rel) && rel.Status.RemoteID == 0 {
		// The cache may lag behind our own write; do not spend an id on a Relation that has one.
		var fresh v1alpha1.Relation
		if err := r.reader().Get(ctx, req.NamespacedName, &fresh); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if fresh.Status.ID != 0 && (!crossNamespace(&rel) || fresh.Status.RemoteID != 0) {
			return ctrl.Result{}, nil
		}
		// If a later write fails the id is lost and the retry takes a new one: ids are unique, not dense.
		if rel.Status.ID == 0 {
			id, err := AllocateID(ctx, r.reader(), r.Client, rel.Namespace, RelationIDKey)
			if err != nil {
				return ctrl.Result{}, err
			}
			rel.Status.ID = id
		}
		// Each model numbers its own relations: the offering side has an id from its own namespace.
		if remote := remoteEndpoint(&rel); remote != nil && rel.Status.RemoteID == 0 {
			if err := EnsureModelConfigMap(ctx, r.Client, remote.Namespace); err != nil {
				return ctrl.Result{}, err
			}
			id, err := AllocateID(ctx, r.reader(), r.Client, remote.Namespace, RelationIDKey)
			if err != nil {
				return ctrl.Result{}, err
			}
			rel.Status.RemoteID = id
		}
	}
	setRelationReady(&rel, prob)
	if !equality.Semantic.DeepEqual(orig.Status, rel.Status) {
		// Optimistic, so a stale cache cannot make a second reconcile replace an id handed out a moment ago.
		return ctrl.Result{}, r.Status().Patch(ctx, &rel, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
	}
	if crossNamespace(&rel) {
		return r.syncRemote(ctx, &rel, prob)
	}
	return ctrl.Result{}, nil
}

// isPeerRelation is true for the single-endpoint Relations the operator creates for a charm's peers endpoints.
func isPeerRelation(rel *v1alpha1.Relation) bool { return len(rel.Spec.Endpoints) == 1 }

// setRelationReady sets the Ready condition: the Relation is valid and its id is assigned.
func setRelationReady(rel *v1alpha1.Relation, prob *relationProblem) {
	c := metav1.Condition{Type: v1alpha1.RelationReady, Status: metav1.ConditionTrue, Reason: "Ready", Message: "the relation is established", ObservedGeneration: rel.Generation}
	switch {
	case prob != nil:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, prob.Reason, prob.Message
	case rel.Status.ID == 0:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, "Pending", "the relation has no id yet"
	}
	meta.SetStatusCondition(&rel.Status.Conditions, c)
}

func setRelationValid(rel *v1alpha1.Relation, prob *relationProblem) {
	c := metav1.Condition{Type: v1alpha1.RelationValid, Status: metav1.ConditionTrue, Reason: "Valid", Message: "the endpoints can be related", ObservedGeneration: rel.Generation}
	if prob != nil {
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, prob.Reason, prob.Message
	}
	meta.SetStatusCondition(&rel.Status.Conditions, c)
	rel.Status.ObservedGeneration = rel.Generation
}

// validate runs ValidateRelation with the current applications and Relations. live reads the Relations from the API
// server, for decisions that hand out an id.
func (r *RelationReconciler) validate(ctx context.Context, rel *v1alpha1.Relation, live bool) (*relationProblem, error) {
	apps := map[string]relationApp{}
	for _, e := range rel.Spec.Endpoints {
		key := appKey(e.Namespace, e.Application)
		if _, done := apps[key]; done || (e.Namespace != rel.Namespace && !crossNamespace(rel)) {
			continue
		}
		var app v1alpha1.Application
		if err := r.Get(ctx, client.ObjectKey{Namespace: e.Namespace, Name: e.Application}, &app); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		ra := relationApp{}
		if app.Status.Charm != nil && app.Status.Charm.Metadata != nil {
			if md, err := parseMetadata(app.Status.Charm.Metadata); err == nil {
				ra.Metadata = md
			}
		}
		apps[key] = ra
	}
	list, err := r.relationsOf(ctx, rel.Namespace, live)
	if err != nil {
		return nil, err
	}
	var others []relationRef
	for i := range list {
		o := &list[i]
		if o.Name != rel.Name {
			others = append(others, relationRef{Name: o.Name, Endpoints: o.Spec.Endpoints, ID: o.Status.ID, Created: o.CreationTimestamp.Time})
		}
	}
	var cross *crossInfo
	if remote := remoteEndpoint(rel); remote != nil || crossNamespace(rel) {
		if cross, others, err = r.crossInfoFor(ctx, rel, list, others, live); err != nil {
			return nil, err
		}
	} else if rel.Spec.Offer != "" {
		return problem(ReasonOfferMissing, "spec.offer is for a relation to an application in another namespace"), nil
	}
	_, peerLabelled := rel.Labels[v1alpha1.PeerEndpointLabel]
	return ValidateRelation(rel.Namespace, rel.Spec.Endpoints, peerLabelled, rel.Name, rel.Status.ID, rel.CreationTimestamp.Time, apps, others, cross), nil
}

func (r *RelationReconciler) relationsOf(ctx context.Context, namespace string, live bool) ([]v1alpha1.Relation, error) {
	var list v1alpha1.RelationList
	var err error
	if live {
		err = r.reader().List(ctx, &list, client.InNamespace(namespace))
	} else {
		err = r.List(ctx, &list, client.InNamespace(namespace))
	}
	return list.Items, err
}

// finalize holds a deleted non-peer Relation until no unit of its applications has it in scope. dropNow skips waiting
// (the namespace is going away, or is not a model).
func (r *RelationReconciler) finalize(ctx context.Context, rel *v1alpha1.Relation, dropNow bool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(rel, RelationFinalizer) {
		return ctrl.Result{}, nil
	}
	if err := r.deleteMirror(ctx, rel); err != nil {
		return ctrl.Result{}, err
	}
	if !dropNow && rel.Annotations[v1alpha1.ForceRemoveAnnotation] != "true" && rel.Status.ID != 0 {
		waiting, err := r.unitsInScope(ctx, rel)
		if err != nil {
			return ctrl.Result{}, err
		}
		orig := rel.DeepCopy()
		if len(waiting) > 0 {
			meta.SetStatusCondition(&rel.Status.Conditions, metav1.Condition{Type: v1alpha1.RelationRemoving, Status: metav1.ConditionTrue, Reason: "WaitingForUnits",
				Message:            "waiting for relation-departed and relation-broken on " + strings.Join(waiting, ", ") + " (annotate " + v1alpha1.ForceRemoveAnnotation + "=true to skip)",
				ObservedGeneration: rel.Generation})
			meta.SetStatusCondition(&rel.Status.Conditions, metav1.Condition{Type: v1alpha1.RelationReady, Status: metav1.ConditionFalse, Reason: "Removing",
				Message: "the relation is being removed", ObservedGeneration: rel.Generation})
			if !equality.Semantic.DeepEqual(orig.Status, rel.Status) {
				if err := r.Status().Patch(ctx, rel, client.MergeFrom(orig)); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{RequeueAfter: relationRecheck}, nil
		}
	}
	if err := r.dropSecrets(ctx, rel); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(rel, RelationFinalizer)
	return ctrl.Result{}, client.IgnoreNotFound(r.Update(ctx, rel))
}

// unitsInScope lists the units of the Relation's applications that have not finished leaving it: those of its own
// namespace, and for a relation across namespaces those of the offering side too (with the id of its namespace).
func (r *RelationReconciler) unitsInScope(ctx context.Context, rel *v1alpha1.Relation) ([]string, error) {
	sides := []struct {
		namespace string
		id        int64
		apps      []string
	}{{namespace: rel.Namespace, id: rel.Status.ID}}
	for _, e := range rel.Spec.Endpoints {
		if e.Namespace == rel.Namespace {
			sides[0].apps = append(sides[0].apps, e.Application)
		} else {
			sides = append(sides, struct {
				namespace string
				id        int64
				apps      []string
			}{e.Namespace, rel.Status.RemoteID, []string{e.Application}})
		}
	}
	var waiting []string
	for _, side := range sides {
		if side.id == 0 {
			continue
		}
		var units v1alpha1.UnitDataList
		if err := r.List(ctx, &units, client.InNamespace(side.namespace)); err != nil {
			return nil, err
		}
		var podErr error
		exists := func(name string) bool {
			err := r.Get(ctx, client.ObjectKey{Namespace: side.namespace, Name: name}, &corev1.Pod{})
			if err != nil && !apierrors.IsNotFound(err) {
				podErr = err
			}
			return err == nil
		}
		w := UnitsInScope(side.id, side.apps, units.Items, exists)
		if podErr != nil {
			return nil, podErr
		}
		for _, u := range w {
			if side.namespace != rel.Namespace {
				u = side.namespace + "/" + u
			}
			waiting = append(waiting, u)
		}
	}
	return waiting, nil
}

// SetupWithManager registers the controller. Events that can change a Relation's validity or let a removal
// proceed (other Relations, Applications, UnitData and pods) enqueue the Relations of the namespace.
func (r *RelationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	inNamespace := func(onlyDeleting bool, mention func(client.Object) string) handler.MapFunc {
		return func(ctx context.Context, o client.Object) []reconcile.Request {
			var rels v1alpha1.RelationList
			if err := r.List(ctx, &rels, client.InNamespace(o.GetNamespace())); err != nil {
				return nil
			}
			app := ""
			if mention != nil {
				app = mention(o)
			}
			var reqs []reconcile.Request
			for i := range rels.Items {
				rel := &rels.Items[i]
				if onlyDeleting && rel.DeletionTimestamp == nil {
					continue
				}
				if app != "" && !relationMentions(rel, app) {
					continue
				}
				reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(rel)})
			}
			return reqs
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("relation").
		For(&v1alpha1.Relation{}).
		// Another Relation being created, getting its id or going away changes duplicate and limit decisions.
		Watches(&v1alpha1.Relation{}, handler.EnqueueRequestsFromMapFunc(inNamespace(false, nil)),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					a, b := e.ObjectOld.(*v1alpha1.Relation), e.ObjectNew.(*v1alpha1.Relation)
					return a.Status.ID != b.Status.ID || (a.DeletionTimestamp == nil) != (b.DeletionTimestamp == nil)
				},
				DeleteFunc: func(event.DeleteEvent) bool { return true },
			})).
		Watches(&v1alpha1.Application{}, handler.EnqueueRequestsFromMapFunc(inNamespace(false, func(o client.Object) string { return o.GetName() }))).
		Watches(&v1alpha1.UnitData{}, handler.EnqueueRequestsFromMapFunc(inNamespace(true, nil))).
		// Relations across namespaces: a mirror that changed or went away wakes its original; an application, an offer,
		// or the data of a unit on either side wakes the relations that involve it.
		Watches(&v1alpha1.Relation{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			rel, ok := o.(*v1alpha1.Relation)
			if !ok || !isMirror(rel) {
				return nil
			}
			if ns, name, ok := remoteOf(rel); ok {
				return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}}
			}
			return nil
		})).
		Watches(&v1alpha1.Application{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.crossRelationsMentioning(ctx, o.GetNamespace(), o.GetName())
		})).
		Watches(&v1alpha1.Offer{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.crossRelationsOfOffer(ctx, o.GetNamespace(), o.GetName())
		})).
		Watches(&v1alpha1.UnitData{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.crossRelationsMentioning(ctx, o.GetNamespace(), "")
		})).
		Watches(&v1alpha1.AppData{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.crossRelationsMentioning(ctx, o.GetNamespace(), o.GetName())
		})).
		// A secret that was granted, changed or revoked changes what the other side may read.
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.crossRelationsMentioning(ctx, o.GetNamespace(), "")
		})).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(inNamespace(true, nil)), builder.WithPredicates(hasAppLabel())).
		Complete(r)
}

func relationMentions(rel *v1alpha1.Relation, app string) bool {
	for _, e := range rel.Spec.Endpoints {
		if e.Application == app {
			return true
		}
	}
	return false
}

// RelationProblem says why a Relation cannot be valid.
type RelationProblem = relationProblem

// AppKey keys the applications given to CheckRelation by namespace and name.
func AppKey(namespace, app string) string { return appKey(namespace, app) }

// CheckRelation validates endpoints against the applications' resolved charms (apps, keyed by AppKey: those that
// exist, with status.charm unset while unresolved) and the Relations already in the namespace, as the operator will.
// It is for relations inside one namespace; relations to an offer are validated by the operator.
func CheckRelation(namespace string, eps []v1alpha1.EndpointRef, apps map[string]*v1alpha1.Application, existing []v1alpha1.Relation) *RelationProblem {
	known := map[string]relationApp{}
	for key, app := range apps {
		ra := relationApp{}
		if app != nil && app.Status.Charm != nil && app.Status.Charm.Metadata != nil {
			if md, err := parseMetadata(app.Status.Charm.Metadata); err == nil {
				ra.Metadata = md
			}
		}
		known[key] = ra
	}
	var others []relationRef
	for i := range existing {
		o := &existing[i]
		others = append(others, relationRef{Name: o.Name, Endpoints: o.Spec.Endpoints, ID: o.Status.ID, Created: o.CreationTimestamp.Time})
	}
	return ValidateRelation(namespace, eps, false, "", 0, time.Now(), known, others, nil)
}
