// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/provider/kubernetes/application/trust.go
// (the trust rules).

package operator

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// fullAccess is juju's trust rule: everything, everywhere in the scope of the (Cluster)Role.
func fullAccess() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{APIGroups: []string{rbacv1.APIGroupAll}, Resources: []string{rbacv1.ResourceAll}, Verbs: []string{rbacv1.VerbAll}}}
}

// TrustRules are the rules added to the app's Role for a trust level. Without trust a charm gets juju's
// default namespace rules (read its namespace, get/list/patch pods and services, exec in pods); trust namespace
// is juju's `trust=true` namespace Role (everything in the namespace); trust cluster adds a ClusterRole with
// everything (Build creates it) on top.
func TrustRules(t v1alpha1.Trust, namespace string) []rbacv1.PolicyRule {
	if t == v1alpha1.TrustNamespace || t == v1alpha1.TrustCluster {
		return fullAccess()
	}
	return []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"namespaces"}, ResourceNames: []string{namespace}, Verbs: []string{"get", "list"}},
		{APIGroups: []string{""}, Resources: []string{"pods", "services"}, Verbs: []string{"get", "list", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"create"}},
	}
}

// ClusterObjectName is the name of the cluster-scoped objects of an app: jk-<namespace>-<app>.
func ClusterObjectName(namespace, app string) string { return "jk-" + namespace + "-" + app }

// ClusterLabels are the labels on cluster-scoped objects of an app; the finalizer finds them by these.
func ClusterLabels(namespace, app string) map[string]string {
	return map[string]string{managedByLabel: "jk", v1alpha1.AppLabel: app, v1alpha1.NamespaceLabel: namespace}
}

// clusterKinds are the cluster-scoped kinds the operator creates for apps and cleans up.
func clusterKinds() []client.ObjectList {
	return []client.ObjectList{&rbacv1.ClusterRoleList{}, &rbacv1.ClusterRoleBindingList{}}
}

// reconcileClusterObjects applies the app's cluster-scoped objects and deletes the ones it no longer wants
// (trust lowered from cluster).
func (r *ApplicationReconciler) reconcileClusterObjects(ctx context.Context, app clientApp, desired []client.Object) error {
	keep := map[string]bool{}
	for _, obj := range desired {
		if err := r.apply(ctx, obj); err != nil {
			return fmt.Errorf("applying %s %s: %w", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), err)
		}
		keep[fmt.Sprintf("%T/%s", obj, obj.GetName())] = true
	}
	return r.deleteClusterObjects(ctx, app, func(o client.Object) bool { return keep[fmt.Sprintf("%T/%s", o, o.GetName())] })
}

// deleteClusterObjects deletes the cluster-scoped objects labelled with the app and namespace, except those keep returns true for.
func (r *ApplicationReconciler) deleteClusterObjects(ctx context.Context, app clientApp, keep func(client.Object) bool) error {
	sel := client.MatchingLabels{v1alpha1.AppLabel: app.name, v1alpha1.NamespaceLabel: app.namespace}
	for _, list := range clusterKinds() {
		if err := r.List(ctx, list, sel); err != nil {
			return err
		}
		var items []client.Object
		switch l := list.(type) {
		case *rbacv1.ClusterRoleList:
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
		case *rbacv1.ClusterRoleBindingList:
			for i := range l.Items {
				items = append(items, &l.Items[i])
			}
		}
		for _, o := range items {
			if keep != nil && keep(o) {
				continue
			}
			if err := r.Delete(ctx, o); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

// clientApp identifies an application for cleanups.
type clientApp struct{ name, namespace string }
