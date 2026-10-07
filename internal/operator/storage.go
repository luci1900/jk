package operator

import (
	"context"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// storageID is the short id in the volume names of an application. Like juju's storage unique id it changes when the
// application is created again, so new volumes never reattach to the data of an application that was removed.
func storageID(app *v1alpha1.Application) string {
	id := strings.ReplaceAll(string(app.UID), "-", "")
	if len(id) < 8 {
		return "00000000"
	}
	return id[:8]
}

// volumeName is the name of a storage's volume claim template: <app>-<storage>-<id>, as in juju. Charms rely on it
// (prometheus-k8s finds its claim with the pattern <app>-database-.*-<pod>).
func volumeName(app *v1alpha1.Application, storage string) string {
	return app.Name + "-" + storage + "-" + storageID(app)
}

// PVCNameMatches reports whether a PersistentVolumeClaim name is one a StatefulSet makes for app: the claim template
// name <app>-<storage>-<id> followed by the pod name <app>-<ordinal> (docs/design.md, Pod and object layout).
func PVCNameMatches(app string, storages []string, pvc string) bool {
	for _, s := range storages {
		if re := regexp.MustCompile("^" + regexp.QuoteMeta(app+"-"+s) + "-[0-9a-f]{8}-" + regexp.QuoteMeta(app) + "-[0-9]+$"); re.MatchString(pvc) {
			return true
		}
	}
	return false
}

// OwnsVolume reports whether the PV belonged to the app's volumes: it carries the app's labels (set when it was
// bound), or its claim has the name of one of the app's claims.
func OwnsVolume(pv *corev1.PersistentVolume, app, namespace string, storages []string) bool {
	if pv.Labels[v1alpha1.AppLabel] == app && pv.Labels[v1alpha1.NamespaceLabel] == namespace {
		return true
	}
	ref := pv.Spec.ClaimRef
	return ref != nil && ref.Namespace == namespace && PVCNameMatches(app, storages, ref.Name)
}

// reconcileVolumes patches the PVs of the app's bound claims to Retain and labels them, so a removed unit's
// data survives as a Released volume (docs/design.md, Pod and object layout).
func (r *ApplicationReconciler) reconcileVolumes(ctx context.Context, app *v1alpha1.Application, storages []string) error {
	if len(storages) == 0 {
		return nil
	}
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(app.Namespace), client.MatchingLabels{nameLabel: app.Name}); err != nil {
		return err
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvc.Spec.VolumeName == "" || pvc.Status.Phase != corev1.ClaimBound || !PVCNameMatches(app.Name, storages, pvc.Name) {
			continue
		}
		var pv corev1.PersistentVolume
		if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, &pv); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain &&
			pv.Labels[v1alpha1.AppLabel] == app.Name && pv.Labels[v1alpha1.NamespaceLabel] == app.Namespace {
			continue
		}
		orig := pv.DeepCopy()
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}
		pv.Labels[v1alpha1.AppLabel] = app.Name
		pv.Labels[v1alpha1.NamespaceLabel] = app.Namespace
		if err := r.Patch(ctx, &pv, client.MergeFrom(orig)); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// destroyVolumes sets the app's PVs to reclaim policy Delete, which makes Kubernetes delete the volumes (and,
// for the ones still bound, when their claims go). Called by the finalizer when the Application carries
// jk.luci1900.github.io/destroy-storage: "true".
func (r *ApplicationReconciler) destroyVolumes(ctx context.Context, app *v1alpha1.Application, storages []string) error {
	var pvs corev1.PersistentVolumeList
	if err := r.List(ctx, &pvs); err != nil {
		return err
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if !OwnsVolume(pv, app.Name, app.Namespace, storages) || pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
			continue
		}
		orig := pv.DeepCopy()
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
		if err := r.Patch(ctx, pv, client.MergeFrom(orig)); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// storageNames lists the charm's storage names from its metadata (empty if unknown).
func storageNames(app *v1alpha1.Application) []string {
	if app.Status.Charm == nil || app.Status.Charm.Metadata == nil {
		return nil
	}
	md, err := parseMetadata(app.Status.Charm.Metadata)
	if err != nil {
		return nil
	}
	return sortedKeys(md.Storage)
}
