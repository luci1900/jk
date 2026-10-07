package operator

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
)

// ModelConfig is the initial content of a model's jk-model ConfigMap: the id counters and model config.
func ModelConfig() map[string]string {
	return map[string]string{
		"relation-id": "0",
		"action-id":   "0",
		"model-config.update-status-hook-interval": "5m",
	}
}

// EnsureModelConfigMap creates the jk-model ConfigMap if it is missing. An existing one is never touched.
func EnsureModelConfigMap(ctx context.Context, c client.Client, namespace string) error {
	key := client.ObjectKey{Namespace: namespace, Name: v1alpha1.ModelConfigMap}
	err := c.Get(ctx, key, &corev1.ConfigMap{})
	if err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: v1alpha1.ModelConfigMap, Labels: map[string]string{managedByLabel: "jk"}},
		Data:       ModelConfig(),
	}
	return client.IgnoreAlreadyExists(c.Create(ctx, cm))
}

// ModelReconciler creates jk-model in namespaces labelled as jk models.
type ModelReconciler struct{ client.Client }

// Reconcile ensures the model ConfigMap for a labelled, live namespace.
func (r *ModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ns corev1.Namespace
	if err := r.Get(ctx, req.NamespacedName, &ns); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if ns.Labels[v1alpha1.ModelLabel] != "true" || ns.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, EnsureModelConfigMap(ctx, r.Client, ns.Name)
}

// SetupWithManager registers the controller.
func (r *ModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("model").
		For(&corev1.Namespace{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetLabels()[v1alpha1.ModelLabel] == "true"
		}))).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: o.GetNamespace()}}}
		}), builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetName() == v1alpha1.ModelConfigMap }))).
		Complete(r)
}
