package operator

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
)

// OfferAccessKeyAll is the suffix of the key that lets every model consume an offering namespace's offers.
const OfferAccessKeyAll = "_all"

// OfferAccessKey is the key in the jk-offer-access ConfigMap that lets consumerNS relate to offers of offerNS. Namespace
// names are DNS labels, so neither the dot nor the underscore can be part of them.
func OfferAccessKey(offerNS, consumerNS string) string { return offerNS + "." + consumerNS }

// OfferAccess is the content of the jk-offer-access ConfigMap for the Offers: a key for each offering namespace and
// each model its offers allow ("*" becomes <offering-ns>._all). The admission policy of Relations across namespaces
// reads it.
func OfferAccess(offers []v1alpha1.Offer) map[string]string {
	out := map[string]string{}
	for i := range offers {
		o := &offers[i]
		if o.DeletionTimestamp != nil {
			continue
		}
		for _, m := range o.Spec.AllowedModels {
			if m == v1alpha1.AllowAllModels {
				out[OfferAccessKey(o.Namespace, OfferAccessKeyAll)] = ""
			} else if m != "" {
				out[OfferAccessKey(o.Namespace, m)] = ""
			}
		}
	}
	return out
}

// OfferReconciler keeps Offers' status, the access ConfigMap, and removes an Offer's connections when it is deleted.
type OfferReconciler struct {
	client.Client
}

// Reconcile checks an Offer in a jk model and records its connections.
func (r *OfferReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var offer v1alpha1.Offer
	if err := r.Get(ctx, req.NamespacedName, &offer); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.syncAccess(ctx)
		}
		return ctrl.Result{}, err
	}
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: offer.Namespace}, &ns); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	nsGone := ns.Name == "" || ns.DeletionTimestamp != nil
	if offer.DeletionTimestamp != nil {
		return r.finalize(ctx, &offer, nsGone)
	}
	if nsGone || ns.Labels[v1alpha1.ModelLabel] != "true" {
		return ctrl.Result{}, nil
	}
	if controllerutil.AddFinalizer(&offer, v1alpha1.OfferFinalizer) {
		if err := r.Update(ctx, &offer); err != nil {
			return ctrl.Result{}, err
		}
	}
	orig := offer.DeepCopy()
	if err := r.setStatus(ctx, &offer); err != nil {
		return ctrl.Result{}, err
	}
	if !equality.Semantic.DeepEqual(orig.Status, offer.Status) {
		if err := r.Status().Patch(ctx, &offer, client.MergeFrom(orig)); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	return ctrl.Result{}, r.syncAccess(ctx)
}

// setStatus fills the Ready condition and the connections.
func (r *OfferReconciler) setStatus(ctx context.Context, offer *v1alpha1.Offer) error {
	c := metav1.Condition{Type: v1alpha1.OfferReady, Status: metav1.ConditionTrue, Reason: "Ready", Message: "the endpoints can be consumed", ObservedGeneration: offer.Generation}
	var app v1alpha1.Application
	err := r.Get(ctx, client.ObjectKey{Namespace: offer.Namespace, Name: offer.Spec.Application}, &app)
	switch {
	case apierrors.IsNotFound(err):
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, "ApplicationNotFound", fmt.Sprintf("application %q does not exist", offer.Spec.Application)
	case err != nil:
		return err
	case app.Status.Charm == nil || app.Status.Charm.Metadata == nil:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, "CharmNotResolved", fmt.Sprintf("the charm of %q is not resolved yet", app.Name)
	default:
		md, err := parseMetadata(app.Status.Charm.Metadata)
		if err != nil {
			return err
		}
		offer.Status.Endpoints = nil
		for _, ep := range offer.Spec.Endpoints {
			info, ok := lookupEndpoint(md, ep)
			if ok && info.Role != rolePeer {
				role := "provider"
				if info.Role == roleRequires {
					role = "requirer"
				}
				offer.Status.Endpoints = append(offer.Status.Endpoints, v1alpha1.OfferEndpoint{Name: ep, Interface: info.Interface, Role: role})
			}
			switch {
			case !ok:
				c.Status, c.Reason, c.Message = metav1.ConditionFalse, "EndpointNotFound", fmt.Sprintf("application %q has no endpoint %q (endpoints: %s)", app.Name, ep, endpointNames(md))
			case info.Role == rolePeer:
				c.Status, c.Reason, c.Message = metav1.ConditionFalse, "PeerEndpoint", fmt.Sprintf("endpoint %q is a peer endpoint, which cannot be offered", ep)
			}
			if c.Status == metav1.ConditionFalse {
				break
			}
		}
	}
	meta.SetStatusCondition(&offer.Status.Conditions, c)
	offer.Status.ObservedGeneration = offer.Generation

	var rels v1alpha1.RelationList
	if err := r.List(ctx, &rels); err != nil {
		return err
	}
	offer.Status.Connections = nil
	for i := range rels.Items {
		rel := &rels.Items[i]
		remote := remoteEndpoint(rel)
		if isMirror(rel) || remote == nil || rel.Spec.Offer != offer.Name || remote.Namespace != offer.Namespace {
			continue
		}
		var local v1alpha1.EndpointRef
		for _, e := range rel.Spec.Endpoints {
			if e.Namespace == rel.Namespace {
				local = e
			}
		}
		status := "joining"
		switch {
		case rel.DeletionTimestamp != nil:
			status = "removing"
		case meta.IsStatusConditionTrue(rel.Status.Conditions, v1alpha1.RelationReady):
			status = "joined"
		}
		offer.Status.Connections = append(offer.Status.Connections, v1alpha1.OfferConnection{
			Namespace: rel.Namespace, Relation: rel.Name, Application: local.Application, Endpoint: local.Endpoint, Status: status,
		})
	}
	sort.Slice(offer.Status.Connections, func(i, j int) bool {
		a, b := offer.Status.Connections[i], offer.Status.Connections[j]
		return a.Namespace+"/"+a.Relation < b.Namespace+"/"+b.Relation
	})
	return nil
}

// finalize removes the Relations that consume a deleted Offer (each in its orderly way) and then lets the Offer go.
func (r *OfferReconciler) finalize(ctx context.Context, offer *v1alpha1.Offer, nsGone bool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(offer, v1alpha1.OfferFinalizer) {
		return ctrl.Result{}, nil
	}
	if !nsGone {
		var rels v1alpha1.RelationList
		if err := r.List(ctx, &rels); err != nil {
			return ctrl.Result{}, err
		}
		left := 0
		for i := range rels.Items {
			rel := &rels.Items[i]
			remote := remoteEndpoint(rel)
			if isMirror(rel) || remote == nil || rel.Spec.Offer != offer.Name || remote.Namespace != offer.Namespace {
				continue
			}
			left++
			if rel.DeletionTimestamp == nil {
				if err := r.Delete(ctx, rel); client.IgnoreNotFound(err) != nil {
					return ctrl.Result{}, err
				}
			}
		}
		if left > 0 {
			return ctrl.Result{RequeueAfter: relationRecheck}, nil
		}
	}
	controllerutil.RemoveFinalizer(offer, v1alpha1.OfferFinalizer)
	if err := r.Update(ctx, offer); client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.syncAccess(ctx)
}

// syncAccess makes the jk-offer-access ConfigMap say what the Offers allow.
func (r *OfferReconciler) syncAccess(ctx context.Context) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var offers v1alpha1.OfferList
		if err := r.List(ctx, &offers); err != nil {
			return err
		}
		var live []v1alpha1.Offer
		for _, o := range offers.Items {
			var ns corev1.Namespace
			if err := r.Get(ctx, client.ObjectKey{Name: o.Namespace}, &ns); err == nil && ns.Labels[v1alpha1.ModelLabel] == "true" && ns.DeletionTimestamp == nil {
				live = append(live, o)
			}
		}
		want := OfferAccess(live)
		var cm corev1.ConfigMap
		key := client.ObjectKey{Namespace: v1alpha1.SystemNamespace, Name: v1alpha1.OfferAccessConfigMap}
		err := r.Get(ctx, key, &cm)
		if apierrors.IsNotFound(err) {
			cm = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{managedByLabel: "jk"}}, Data: want}
			return r.Create(ctx, &cm)
		}
		if err != nil {
			return err
		}
		if equality.Semantic.DeepEqual(cm.Data, want) || (len(cm.Data) == 0 && len(want) == 0) {
			return nil
		}
		cm.Data = want
		return r.Update(ctx, &cm)
	})
}

// SetupWithManager registers the controller; Applications and Relations that change what an Offer shows wake it.
func (r *OfferReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("offer").
		For(&v1alpha1.Offer{}).
		Watches(&v1alpha1.Application{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			var offers v1alpha1.OfferList
			if err := r.List(ctx, &offers, client.InNamespace(o.GetNamespace())); err != nil {
				return nil
			}
			var reqs []reconcile.Request
			for _, of := range offers.Items {
				if of.Spec.Application == o.GetName() {
					reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&of)})
				}
			}
			return reqs
		})).
		Watches(&v1alpha1.Relation{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			rel, ok := o.(*v1alpha1.Relation)
			if !ok || isMirror(rel) || rel.Spec.Offer == "" {
				return nil
			}
			if remote := remoteEndpoint(rel); remote != nil {
				return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: remote.Namespace, Name: rel.Spec.Offer}}}
			}
			return nil
		})).
		Complete(r)
}
