package sdk

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
)

// ModelConfigPrefix is the prefix of model config keys in the jk-model ConfigMap.
const ModelConfigPrefix = "model-config."

// Model is a namespace marked as a jk model.
type Model struct {
	Name         string
	Applications int
	// Status is "available" or "destroying".
	Status string
}

func (c *Client) namespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var ns corev1.Namespace
	err := c.Kube.Get(ctx, client.ObjectKey{Name: name}, &ns)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("model %q not found", name)
	}
	if err != nil {
		return nil, err
	}
	if ns.Labels[v1alpha1.ModelLabel] != "true" {
		return nil, fmt.Errorf("model %q not found: namespace %q is not a jk model", name, name)
	}
	return &ns, nil
}

// AddModel creates a namespace and marks it as a jk model. It never adopts an existing namespace, as in juju.
func (c *Client) AddModel(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("model name is required")
	}
	if err := c.requireInstalled(ctx); err != nil {
		return err
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.ModelLabel: "true"}}}
	if err := c.Kube.Create(ctx, ns); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("model %q already exists", name)
		}
		return err
	}
	return operator.EnsureModelConfigMap(ctx, c.Kube, name)
}

// Models lists the jk models.
func (c *Client) Models(ctx context.Context) ([]Model, error) {
	if err := c.requireInstalled(ctx); err != nil {
		return nil, err
	}
	var nss corev1.NamespaceList
	if err := c.Kube.List(ctx, &nss, client.MatchingLabels{v1alpha1.ModelLabel: "true"}); err != nil {
		return nil, err
	}
	var out []Model
	for i := range nss.Items {
		ns := &nss.Items[i]
		m := Model{Name: ns.Name, Status: "available"}
		if ns.DeletionTimestamp != nil {
			m.Status = "destroying"
		}
		var apps v1alpha1.ApplicationList
		if err := c.Kube.List(ctx, &apps, client.InNamespace(ns.Name)); err == nil {
			m.Applications = len(apps.Items)
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DestroyOptions say how a model is destroyed.
type DestroyOptions struct {
	// Force skips the graceful removal of relations and applications and deletes the namespace at once; teardown
	// hooks still get the pods' grace period.
	Force bool
	// DestroyStorage deletes the volumes of the model's applications; by default they are kept.
	DestroyStorage bool
	// Timeout bounds the graceful removal and the wait for the namespace to go (default 10 minutes each).
	Timeout time.Duration
	// NoWait returns once the namespace is deleted, without waiting for it to disappear.
	NoWait bool
}

// DestroyModel removes a model: gracefully (relations and applications are removed, their teardown hooks run, then the
// namespace is deleted) or, with Force, by deleting the namespace straight away.
func (c *Client) DestroyModel(ctx context.Context, name string, opts DestroyOptions) error {
	if _, err := c.namespace(ctx, name); err != nil {
		return err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Minute
	}
	mc := c.InNamespace(name)
	var apps v1alpha1.ApplicationList
	if err := c.Kube.List(ctx, &apps, client.InNamespace(name)); err != nil {
		return err
	}
	if opts.DestroyStorage {
		for i := range apps.Items {
			if err := mc.annotateDestroyStorage(ctx, apps.Items[i].Name); err != nil {
				return err
			}
		}
	}
	if !opts.Force {
		for i := range apps.Items {
			if err := c.Kube.Delete(ctx, &apps.Items[i]); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
		if err := c.wait(ctx, timeout, "applications to be removed", func(ctx context.Context) (bool, string, error) {
			var left v1alpha1.ApplicationList
			if err := c.Kube.List(ctx, &left, client.InNamespace(name)); err != nil {
				return false, "", err
			}
			names := make([]string, 0, len(left.Items))
			for _, a := range left.Items {
				names = append(names, a.Name)
			}
			return len(names) == 0, "still removing " + strings.Join(names, ", "), nil
		}); err != nil {
			return fmt.Errorf("%w (use --force to delete the model anyway)", err)
		}
	}
	if err := c.Kube.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); client.IgnoreNotFound(err) != nil {
		return err
	}
	if opts.NoWait {
		return nil
	}
	return c.wait(ctx, timeout, fmt.Sprintf("model %q to be removed", name), func(ctx context.Context) (bool, string, error) {
		var ns corev1.Namespace
		err := c.Kube.Get(ctx, client.ObjectKey{Name: name}, &ns)
		if apierrors.IsNotFound(err) {
			return true, "", nil
		}
		return false, "the namespace is terminating", err
	})
}

func (c *Client) annotateDestroyStorage(ctx context.Context, app string) error {
	_, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error {
		if a.Annotations == nil {
			a.Annotations = map[string]string{}
		}
		a.Annotations[v1alpha1.DestroyStorageAnnotation] = "true"
		return nil
	})
	return err
}

// ModelConfig returns the model config (keys without the prefix).
func (c *Client) ModelConfig(ctx context.Context) (map[string]string, error) {
	if err := c.requireModel(ctx); err != nil {
		return nil, err
	}
	var cm corev1.ConfigMap
	if err := c.Kube.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: v1alpha1.ModelConfigMap}, &cm); err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	out := map[string]string{}
	for k, v := range cm.Data {
		if strings.HasPrefix(k, ModelConfigPrefix) {
			out[strings.TrimPrefix(k, ModelConfigPrefix)] = v
		}
	}
	return out, nil
}

// SetModelConfig sets keys of the model config; an empty value resets the key to its default.
func (c *Client) SetModelConfig(ctx context.Context, kv map[string]string) error {
	if err := c.requireModel(ctx); err != nil {
		return err
	}
	if err := operator.EnsureModelConfigMap(ctx, c.Kube, c.Namespace); err != nil {
		return err
	}
	defaults := operator.ModelConfig()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cm corev1.ConfigMap
		if err := c.Kube.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: v1alpha1.ModelConfigMap}, &cm); err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		for k, v := range kv {
			key := ModelConfigPrefix + k
			if v == "" {
				if d, ok := defaults[key]; ok {
					cm.Data[key] = d
				} else {
					delete(cm.Data, key)
				}
				continue
			}
			cm.Data[key] = v
		}
		return c.Kube.Update(ctx, &cm)
	})
}
