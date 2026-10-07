package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
)

// crossRecheck is how often a valid Relation across namespaces is looked at again, as a safety net for missed events,
// and how long an invalid one waits for its offer to appear.
const crossRecheck = 20 * time.Second

// isMirror is true for the copy of a consumer's Relation that the operator keeps in the offering namespace.
func isMirror(rel *v1alpha1.Relation) bool { return rel.Labels[v1alpha1.RemoteLabel] == "true" }

// crossNamespace is true for a Relation whose endpoints are in two namespaces.
func crossNamespace(rel *v1alpha1.Relation) bool {
	e := rel.Spec.Endpoints
	return len(e) == 2 && e[0].Namespace != e[1].Namespace
}

// remoteEndpoint is the endpoint of the Relation that is in another namespace than the Relation itself (nil if none).
func remoteEndpoint(rel *v1alpha1.Relation) *v1alpha1.EndpointRef {
	for i := range rel.Spec.Endpoints {
		if rel.Spec.Endpoints[i].Namespace != rel.Namespace {
			return &rel.Spec.Endpoints[i]
		}
	}
	return nil
}

// aliasOf is the name the application on the other side has for the Relation's namespace: spec.alias, else the offer's name.
func aliasOf(rel *v1alpha1.Relation) string {
	if rel.Spec.Alias != "" {
		return rel.Spec.Alias
	}
	return rel.Spec.Offer
}

// mirrorName is the name of the mirror of a consumer's Relation in the offering namespace.
func mirrorName(rel *v1alpha1.Relation) string { return "remote." + rel.Namespace + "." + rel.Name }

// mirrorAlias is the name the consuming application has in the offering namespace (juju's "remote-<uuid>").
func mirrorAlias(rel *v1alpha1.Relation) string {
	sum := sha256.Sum256([]byte(rel.Namespace + "/" + rel.Name))
	return "remote-" + hex.EncodeToString(sum[:4])
}

// remoteOf is the consumer's Relation a mirror copies.
func remoteOf(rel *v1alpha1.Relation) (namespace, name string, ok bool) {
	namespace, name, ok = strings.Cut(rel.Annotations[v1alpha1.RemoteOfAnnotation], "/")
	return namespace, name, ok && namespace != "" && name != ""
}

// crossInfoFor gathers what validating a Relation across namespaces needs: the Offer, the aliases in use and the
// Relations of the offering namespace (which count towards the offered endpoint's limit).
func (r *RelationReconciler) crossInfoFor(ctx context.Context, rel *v1alpha1.Relation, local []v1alpha1.Relation, others []relationRef, live bool) (*crossInfo, []relationRef, error) {
	remote := remoteEndpoint(rel)
	if remote == nil {
		return &crossInfo{}, others, nil
	}
	ci := &crossInfo{OfferName: rel.Spec.Offer, Alias: aliasOf(rel), LocalApps: map[string]bool{}, AliasUsed: map[string]bool{}}
	var apps v1alpha1.ApplicationList
	if err := r.List(ctx, &apps, client.InNamespace(rel.Namespace)); err != nil {
		return nil, nil, err
	}
	for _, a := range apps.Items {
		ci.LocalApps[a.Name] = true
	}
	for i := range local {
		if o := &local[i]; o.Name != rel.Name {
			if alias := aliasOf(o); alias != "" {
				ci.AliasUsed[alias] = true
			}
		}
	}
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: remote.Namespace}, &ns); err != nil && !apierrors.IsNotFound(err) {
		return nil, nil, err
	}
	ci.RemoteIsModel = ns.Labels[v1alpha1.ModelLabel] == "true" && ns.DeletionTimestamp == nil
	if ci.RemoteIsModel && rel.Spec.Offer != "" {
		var offer v1alpha1.Offer
		if err := r.Get(ctx, client.ObjectKey{Namespace: remote.Namespace, Name: rel.Spec.Offer}, &offer); err == nil {
			ci.Offer = &offer
		} else if !apierrors.IsNotFound(err) {
			return nil, nil, err
		}
	}
	if ci.RemoteIsModel {
		theirs, err := r.relationsOf(ctx, remote.Namespace, live)
		if err != nil {
			return nil, nil, err
		}
		for i := range theirs {
			o := &theirs[i]
			if ns, name, ok := remoteOf(o); ok && ns == rel.Namespace && name == rel.Name {
				continue // our own mirror
			}
			others = append(others, relationRef{Name: o.Namespace + "/" + o.Name, Endpoints: o.Spec.Endpoints, ID: o.Status.ID, Created: o.CreationTimestamp.Time})
		}
	}
	return ci, others, nil
}

// reconcileMirror removes a mirror whose Relation is gone; the Relation's own reconcile keeps the live ones up to date.
func (r *RelationReconciler) reconcileMirror(ctx context.Context, mirror *v1alpha1.Relation) (ctrl.Result, error) {
	ns, name, ok := remoteOf(mirror)
	if ok {
		var src v1alpha1.Relation
		err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &src)
		if err == nil && src.DeletionTimestamp == nil {
			return ctrl.Result{}, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, mirror))
}

func (r *RelationReconciler) deleteMirror(ctx context.Context, rel *v1alpha1.Relation) error {
	remote := remoteEndpoint(rel)
	if remote == nil || !crossNamespace(rel) {
		return nil
	}
	m := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: remote.Namespace, Name: mirrorName(rel)}}
	return client.IgnoreNotFound(r.Delete(ctx, m))
}

// syncRemote keeps a valid Relation across namespaces connected: the mirror in the offering namespace and the data
// each side reads about the other. It runs after every change of the Relation, of the units on either side, or of the
// offer.
func (r *RelationReconciler) syncRemote(ctx context.Context, rel *v1alpha1.Relation, prob *relationProblem) (ctrl.Result, error) {
	if prob != nil {
		return ctrl.Result{RequeueAfter: crossRecheck}, nil // an offer or application that is not there yet may appear
	}
	remote := remoteEndpoint(rel)
	if remote == nil || rel.Status.ID == 0 || rel.Status.RemoteID == 0 || rel.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	var local *v1alpha1.EndpointRef
	for i := range rel.Spec.Endpoints {
		if rel.Spec.Endpoints[i].Namespace == rel.Namespace {
			local = &rel.Spec.Endpoints[i]
		}
	}
	mirror, err := r.ensureMirror(ctx, rel)
	if err != nil {
		return ctrl.Result{}, err
	}
	alias, theirs := aliasOf(rel), mirrorAlias(rel)
	// Secrets first, so that a secret's id is never visible before the secret.
	toConsumer, toOffering, err := r.syncSecrets(ctx, rel, local.Application, remote)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.syncData(ctx, rel, remote.Namespace, remote.Application, alias, rel.Status.ID, rel.Status.RemoteID, rel.Name, rel, MirroredRevisions(toConsumer)); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.syncData(ctx, mirror, local.Namespace, local.Application, theirs, rel.Status.RemoteID, rel.Status.ID, mirror.Name, mirror, MirroredRevisions(toOffering)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: crossRecheck}, nil
}

// syncSecrets keeps in each namespace the copies of the secrets the other side's application has granted to this one
// (to the whole application or to this relation), and returns them.
func (r *RelationReconciler) syncSecrets(ctx context.Context, rel *v1alpha1.Relation, consumerApp string, offered *v1alpha1.EndpointRef) (toConsumer, toOffering []corev1.Secret, err error) {
	inOffering, err := r.secretsIn(ctx, offered.Namespace)
	if err != nil {
		return nil, nil, err
	}
	inConsumer, err := r.secretsIn(ctx, rel.Namespace)
	if err != nil {
		return nil, nil, err
	}
	id := MirrorID(rel.Namespace, rel.Name)
	toConsumer = MirrorSecrets(inOffering, MirrorParams{SrcApp: offered.Application, Grantee: mirrorAlias(rel), RelationID: rel.Status.RemoteID,
		DstNS: rel.Namespace, DstApp: consumerApp, OwnerAlias: aliasOf(rel), MirrorID: id})
	toOffering = MirrorSecrets(inConsumer, MirrorParams{SrcApp: consumerApp, Grantee: aliasOf(rel), RelationID: rel.Status.ID,
		DstNS: offered.Namespace, DstApp: offered.Application, OwnerAlias: mirrorAlias(rel), MirrorID: id})
	if err := r.syncMirrors(ctx, rel.Namespace, id, toConsumer); err != nil {
		return nil, nil, err
	}
	return toConsumer, toOffering, r.syncMirrors(ctx, offered.Namespace, id, toOffering)
}

// dropSecrets removes the copies of secrets a relation made in both namespaces.
func (r *RelationReconciler) dropSecrets(ctx context.Context, rel *v1alpha1.Relation) error {
	remote := remoteEndpoint(rel)
	if remote == nil || !crossNamespace(rel) {
		return nil
	}
	id := MirrorID(rel.Namespace, rel.Name)
	for _, ns := range []string{rel.Namespace, remote.Namespace} {
		if err := r.syncMirrors(ctx, ns, id, nil); err != nil {
			return err
		}
	}
	return nil
}

// ensureMirror creates or updates the mirror Relation of rel in the offering namespace, with its ids swapped.
func (r *RelationReconciler) ensureMirror(ctx context.Context, rel *v1alpha1.Relation) (*v1alpha1.Relation, error) {
	remote := remoteEndpoint(rel)
	want := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: remote.Namespace, Name: mirrorName(rel),
			Labels:      map[string]string{v1alpha1.RemoteLabel: "true"},
			Annotations: map[string]string{v1alpha1.RemoteOfAnnotation: rel.Namespace + "/" + rel.Name},
		},
		Spec: v1alpha1.RelationSpec{Endpoints: rel.Spec.Endpoints, Offer: rel.Spec.Offer, Alias: mirrorAlias(rel)},
	}
	var cur v1alpha1.Relation
	err := r.Get(ctx, client.ObjectKeyFromObject(want), &cur)
	switch {
	case apierrors.IsNotFound(err):
		if err := r.Create(ctx, want); err != nil {
			return nil, err
		}
		cur = *want
	case err != nil:
		return nil, err
	case !reflect.DeepEqual(cur.Spec, want.Spec):
		cur.Spec = want.Spec
		if err := r.Update(ctx, &cur); err != nil {
			return nil, err
		}
	}
	orig := cur.DeepCopy()
	cur.Status.ID, cur.Status.RemoteID = rel.Status.RemoteID, rel.Status.ID
	cur.Status.ObservedGeneration = cur.Generation
	setRelationValid(&cur, nil)
	setRelationReady(&cur, nil)
	if !equality.Semantic.DeepEqual(orig.Status, cur.Status) {
		if err := r.Status().Patch(ctx, &cur, client.MergeFrom(orig)); err != nil {
			return nil, err
		}
	}
	return &cur, nil
}

// syncData writes the RemoteData named name in the namespace of owner: the settings, in relation remoteID, of the units
// of app in remoteNS (those in scope) and of the application itself, under the alias. id is the relation's id in the
// owner's namespace.
func (r *RelationReconciler) syncData(ctx context.Context, owner *v1alpha1.Relation, remoteNS, app, alias string, id, remoteID int64, name string, ownerObj *v1alpha1.Relation, secrets map[string]int) error {
	units, appData, err := r.remoteSettings(ctx, remoteNS, app, alias, remoteID)
	if err != nil {
		return err
	}
	want := v1alpha1.RemoteDataSpec{Application: alias, Source: remoteNS + "/" + app, RelationID: id, Units: units, AppData: appData, Secrets: secrets}
	var cur v1alpha1.RemoteData
	err = r.Get(ctx, client.ObjectKey{Namespace: owner.Namespace, Name: name}, &cur)
	switch {
	case apierrors.IsNotFound(err):
		cur = v1alpha1.RemoteData{
			ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: name, OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1alpha1.GroupVersion.String(), Kind: "Relation", Name: ownerObj.Name, UID: ownerObj.UID,
			}}},
			Spec: want,
		}
		return r.Create(ctx, &cur)
	case err != nil:
		return err
	case equality.Semantic.DeepEqual(cur.Spec, want):
		return nil
	}
	cur.Spec = want
	return r.Update(ctx, &cur)
}

// remoteSettings reads what the units of app in ns have set in the relation with the given id there.
func (r *RelationReconciler) remoteSettings(ctx context.Context, ns, app, alias string, id int64) (map[string]v1alpha1.RemoteUnit, v1alpha1.RelationData, error) {
	var units v1alpha1.UnitDataList
	if err := r.List(ctx, &units, client.InNamespace(ns)); err != nil {
		return nil, nil, err
	}
	return RemoteSettings(units.Items, app, alias, id, r.appData(ctx, ns, app))
}

func (r *RelationReconciler) appData(ctx context.Context, ns, app string) *v1alpha1.AppData {
	var ad v1alpha1.AppData
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: app}, &ad); err != nil {
		return nil
	}
	return &ad
}

// RemoteSettings is what a relation shows the other side: the settings of the units of app that are in the relation
// (whose id in their namespace is id), named alias/<n>, and the application's settings.
func RemoteSettings(units []v1alpha1.UnitData, app, alias string, id int64, appData *v1alpha1.AppData) (map[string]v1alpha1.RemoteUnit, v1alpha1.RelationData, error) {
	out := map[string]v1alpha1.RemoteUnit{}
	for i := range units {
		u := &units[i]
		n, ok := PodOrdinal(app, u.Name)
		if !ok {
			continue
		}
		st, in := u.Spec.Relations[relKey(id)]
		if !in || !st.InScope {
			continue
		}
		out[fmt.Sprintf("%s/%d", alias, n)] = v1alpha1.RemoteUnit{Data: copyData(st.Data)}
	}
	var appSettings v1alpha1.RelationData
	if appData != nil {
		appSettings = copyData(appData.Spec.Relations[relKey(id)])
	}
	if len(out) == 0 {
		out = nil
	}
	return out, appSettings, nil
}

func copyData(d v1alpha1.RelationData) v1alpha1.RelationData {
	if len(d) == 0 {
		return nil
	}
	out := make(v1alpha1.RelationData, len(d))
	for k, v := range d {
		out[k] = v
	}
	return out
}

// crossRelationsMentioning lists the Relations (of any namespace) across namespaces, and not mirrors, that have an
// endpoint on the application, or on every application of the namespace when app is empty.
func (r *RelationReconciler) crossRelationsMentioning(ctx context.Context, ns, app string) []reconcile.Request {
	var rels v1alpha1.RelationList
	if err := r.List(ctx, &rels); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range rels.Items {
		rel := &rels.Items[i]
		if isMirror(rel) || !crossNamespace(rel) {
			continue
		}
		for _, e := range rel.Spec.Endpoints {
			if e.Namespace == ns && (app == "" || e.Application == app) {
				reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(rel)})
				break
			}
		}
	}
	return reqs
}

// crossRelationsOfOffer lists the Relations that consume the offer.
func (r *RelationReconciler) crossRelationsOfOffer(ctx context.Context, ns, offer string) []reconcile.Request {
	var rels v1alpha1.RelationList
	if err := r.List(ctx, &rels); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range rels.Items {
		rel := &rels.Items[i]
		if isMirror(rel) || rel.Spec.Offer != offer {
			continue
		}
		if remote := remoteEndpoint(rel); remote != nil && remote.Namespace == ns {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(rel)})
		}
	}
	return reqs
}
