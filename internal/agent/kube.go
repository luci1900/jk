// Package agent is the jk unit agent: a controller-runtime loop keyed on "this unit" that feeds a snapshot of
// the world to the resolver (internal/agent/resolver) and executes the operations it returns.
package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// ResolvedAnnotation is set on the unit's Pod by `resolved`: "retry" retries the failed hook now, "no-retry" skips it.
const ResolvedAnnotation = v1alpha1.Group + "/resolved"

// ModelConfigPrefix prefixes model config keys in the jk-model ConfigMap.
const ModelConfigPrefix = "model-config."

// Kube is the narrow set of Kubernetes operations the agent needs, so that the resolver and executor
// can be tested without a cluster. All objects are in the agent's namespace and named after its application and unit.
type Kube interface {
	// Application returns the unit's Application (apierrors.IsNotFound when it is gone).
	Application(ctx context.Context) (*v1alpha1.Application, error)
	// ModelConfig returns the data of the jk-model ConfigMap (empty when it does not exist).
	ModelConfig(ctx context.Context) (map[string]string, error)
	// UnitData returns this unit's UnitData, or nil when it does not exist yet.
	UnitData(ctx context.Context) (*v1alpha1.UnitData, error)
	// SaveUnitData creates or replaces the spec of this unit's UnitData (owned by the Application).
	SaveUnitData(ctx context.Context, spec v1alpha1.UnitDataSpec) error
	// AppData returns the application's AppData, or nil when it does not exist yet.
	AppData(ctx context.Context) (*v1alpha1.AppData, error)
	// UpdateAppData creates or updates the AppData spec with mutate. Only the leader calls it.
	UpdateAppData(ctx context.Context, mutate func(*v1alpha1.AppDataSpec)) error
	// Lease returns the application's leader Lease.
	Lease(ctx context.Context) (*coordinationv1.Lease, error)
	// RenewLease sets renewTime to now if holder holds the Lease; either way it returns the Lease as stored.
	RenewLease(ctx context.Context, holder string, now time.Time) (*coordinationv1.Lease, error)
	// Resolved returns the unit Pod's resolved annotation ("", "retry" or "no-retry").
	Resolved(ctx context.Context) (string, error)
	// ClearResolved removes the annotation.
	ClearResolved(ctx context.Context) error

	// Relations returns the Relation objects of the namespace (informer-backed).
	Relations(ctx context.Context) ([]v1alpha1.Relation, error)
	// UnitDatas returns every UnitData of the namespace (informer-backed): the other units' relation data and tracked secrets.
	UnitDatas(ctx context.Context) ([]v1alpha1.UnitData, error)
	// AppDataOf returns the AppData of the named application (informer-backed), or nil.
	AppDataOf(ctx context.Context, app string) (*v1alpha1.AppData, error)

	// RemoteDatas returns the RemoteData of the namespace (informer-backed): what the other side of each relation
	// to another namespace has published, copied in by the operator.
	RemoteDatas(ctx context.Context) ([]v1alpha1.RemoteData, error)
	// AppDatas returns the AppData of every application of the namespace (informer-backed): the owners' secret indexes.
	AppDatas(ctx context.Context) ([]v1alpha1.AppData, error)

	// Actions returns the Actions addressed to this unit (informer-backed), in no particular order.
	Actions(ctx context.Context) ([]v1alpha1.Action, error)
	// UpdateActionStatus reads the named Action fresh, applies mutate and writes its status; mutate returns false to
	// leave it alone (nothing is written). A conflict is retried. A missing Action is not an error.
	UpdateActionStatus(ctx context.Context, name string, mutate func(*v1alpha1.Action) bool) error

	// GetSecret, CreateSecret, UpdateSecret and DeleteSecret work on the Secrets that back juju secrets, by name.
	// GetSecret returns an apierrors NotFound or Forbidden error as the API server does; DeleteSecret ignores NotFound.
	GetSecret(ctx context.Context, name string) (*corev1.Secret, error)
	CreateSecret(ctx context.Context, s *corev1.Secret) error
	UpdateSecret(ctx context.Context, s *corev1.Secret) error
	DeleteSecret(ctx context.Context, name string) error
}

// KubeClient implements Kube with controller-runtime. Reader serves cheap reads of the objects the agent
// watches (a cache-backed client is fine); Client is used for writes and for reads that must be fresh.
type KubeClient struct {
	Reader    client.Reader
	Client    client.Client
	Namespace string
	App       string
	Unit      string // <app>/<n>
	Pod       string // <app>-<n>

	// lastUD is the UnitData as this agent last wrote it. The agent is its only writer, so the next write can go out
	// with its resourceVersion and skip the read; a conflict falls back to reading.
	udMu   sync.Mutex
	lastUD *v1alpha1.UnitData
}

var _ Kube = (*KubeClient)(nil)

func (k *KubeClient) key(name string) client.ObjectKey {
	return client.ObjectKey{Namespace: k.Namespace, Name: name}
}

// LeaseName is the name of the application's leader Lease.
func LeaseName(app string) string { return app + "-leader" }

func (k *KubeClient) Application(ctx context.Context) (*v1alpha1.Application, error) {
	var app v1alpha1.Application
	if err := k.Reader.Get(ctx, k.key(k.App), &app); err != nil {
		return nil, err
	}
	return &app, nil
}

func (k *KubeClient) ModelConfig(ctx context.Context) (map[string]string, error) {
	var cm corev1.ConfigMap
	if err := k.Reader.Get(ctx, k.key(v1alpha1.ModelConfigMap), &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	return cm.Data, nil
}

func (k *KubeClient) UnitData(ctx context.Context) (*v1alpha1.UnitData, error) {
	var ud v1alpha1.UnitData
	if err := k.Client.Get(ctx, k.key(k.Pod), &ud); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &ud, nil
}

func (k *KubeClient) SaveUnitData(ctx context.Context, spec v1alpha1.UnitDataSpec) error {
	k.udMu.Lock()
	cached := k.lastUD
	k.lastUD = nil
	k.udMu.Unlock()
	if cached != nil {
		cached = cached.DeepCopy()
		cached.Spec = spec
		if err := k.Client.Update(ctx, cached); err == nil {
			k.udMu.Lock()
			k.lastUD = cached
			k.udMu.Unlock()
			return nil
		} else if !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return retry.OnError(retry.DefaultRetry, func(err error) bool { return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) }, func() error {
		existing, err := k.UnitData(ctx)
		if err != nil {
			return err
		}
		if existing != nil {
			existing.Spec = spec
			if err := k.Client.Update(ctx, existing); err != nil {
				return err
			}
			k.remember(existing)
			return nil
		}
		app, err := k.Application(ctx)
		if err != nil {
			return fmt.Errorf("creating UnitData: %w", err)
		}
		ud := &v1alpha1.UnitData{
			ObjectMeta: metav1.ObjectMeta{
				Name: k.Pod, Namespace: k.Namespace,
				Labels:          map[string]string{v1alpha1.AppLabel: k.App},
				OwnerReferences: []metav1.OwnerReference{ownerRef(app)},
			},
			Spec: spec,
		}
		if err := k.Client.Create(ctx, ud); err != nil {
			return err
		}
		k.remember(ud)
		return nil
	})
}

func (k *KubeClient) remember(ud *v1alpha1.UnitData) {
	k.udMu.Lock()
	k.lastUD = ud
	k.udMu.Unlock()
}

func ownerRef(app *v1alpha1.Application) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: v1alpha1.GroupVersion.String(), Kind: "Application", Name: app.Name, UID: app.UID,
	}
}

func (k *KubeClient) AppData(ctx context.Context) (*v1alpha1.AppData, error) {
	var ad v1alpha1.AppData
	if err := k.Client.Get(ctx, k.key(k.App), &ad); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &ad, nil
}

func (k *KubeClient) UpdateAppData(ctx context.Context, mutate func(*v1alpha1.AppDataSpec)) error {
	return retry.OnError(retry.DefaultRetry, func(err error) bool { return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) }, func() error {
		existing, err := k.AppData(ctx)
		if err != nil {
			return err
		}
		if existing != nil {
			mutate(&existing.Spec)
			return k.Client.Update(ctx, existing)
		}
		app, err := k.Application(ctx)
		if err != nil {
			return fmt.Errorf("creating AppData: %w", err)
		}
		ad := &v1alpha1.AppData{
			ObjectMeta: metav1.ObjectMeta{
				Name: k.App, Namespace: k.Namespace,
				Labels:          map[string]string{v1alpha1.AppLabel: k.App},
				OwnerReferences: []metav1.OwnerReference{ownerRef(app)},
			},
		}
		mutate(&ad.Spec)
		return k.Client.Create(ctx, ad)
	})
}

func (k *KubeClient) Lease(ctx context.Context) (*coordinationv1.Lease, error) {
	var l coordinationv1.Lease
	if err := k.Reader.Get(ctx, k.key(LeaseName(k.App)), &l); err != nil {
		return nil, err
	}
	return &l, nil
}

func (k *KubeClient) RenewLease(ctx context.Context, holder string, now time.Time) (*coordinationv1.Lease, error) {
	var out *coordinationv1.Lease
	err := retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		var l coordinationv1.Lease
		if err := k.Client.Get(ctx, k.key(LeaseName(k.App)), &l); err != nil {
			return err
		}
		out = &l
		if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != holder {
			return nil
		}
		t := metav1.NewMicroTime(now)
		l.Spec.RenewTime = &t
		return k.Client.Update(ctx, &l)
	})
	return out, err
}

func (k *KubeClient) Resolved(ctx context.Context) (string, error) {
	var pod corev1.Pod
	if err := k.Client.Get(ctx, k.key(k.Pod), &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return pod.Annotations[ResolvedAnnotation], nil
}

func (k *KubeClient) ClearResolved(ctx context.Context) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, ResolvedAnnotation))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: k.Pod, Namespace: k.Namespace}}
	err := k.Client.Patch(ctx, pod, client.RawPatch("application/merge-patch+json", patch))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (k *KubeClient) Relations(ctx context.Context) ([]v1alpha1.Relation, error) {
	var l v1alpha1.RelationList
	if err := k.Reader.List(ctx, &l, client.InNamespace(k.Namespace)); err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (k *KubeClient) UnitDatas(ctx context.Context) ([]v1alpha1.UnitData, error) {
	var l v1alpha1.UnitDataList
	if err := k.Reader.List(ctx, &l, client.InNamespace(k.Namespace)); err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (k *KubeClient) RemoteDatas(ctx context.Context) ([]v1alpha1.RemoteData, error) {
	var l v1alpha1.RemoteDataList
	if err := k.Reader.List(ctx, &l, client.InNamespace(k.Namespace)); err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (k *KubeClient) AppDataOf(ctx context.Context, app string) (*v1alpha1.AppData, error) {
	var ad v1alpha1.AppData
	if err := k.Reader.Get(ctx, k.key(app), &ad); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &ad, nil
}

func (k *KubeClient) AppDatas(ctx context.Context) ([]v1alpha1.AppData, error) {
	var l v1alpha1.AppDataList
	if err := k.Reader.List(ctx, &l, client.InNamespace(k.Namespace)); err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (k *KubeClient) Actions(ctx context.Context) ([]v1alpha1.Action, error) {
	var l v1alpha1.ActionList
	if err := k.Reader.List(ctx, &l, client.InNamespace(k.Namespace)); err != nil {
		return nil, err
	}
	var out []v1alpha1.Action
	for _, a := range l.Items {
		if a.Spec.Unit == k.Unit {
			out = append(out, a)
		}
	}
	return out, nil
}

func (k *KubeClient) UpdateActionStatus(ctx context.Context, name string, mutate func(*v1alpha1.Action) bool) error {
	err := retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		var a v1alpha1.Action
		if err := k.Client.Get(ctx, k.key(name), &a); err != nil {
			return err
		}
		if !mutate(&a) {
			return nil
		}
		return k.Client.Status().Update(ctx, &a)
	})
	return client.IgnoreNotFound(err)
}

// Secrets are read and written directly (never through the cache): the agent may only get them by name.
func (k *KubeClient) GetSecret(ctx context.Context, name string) (*corev1.Secret, error) {
	var s corev1.Secret
	if err := k.Client.Get(ctx, k.key(name), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (k *KubeClient) CreateSecret(ctx context.Context, s *corev1.Secret) error {
	s.Namespace = k.Namespace
	return k.Client.Create(ctx, s)
}

func (k *KubeClient) UpdateSecret(ctx context.Context, s *corev1.Secret) error {
	return k.Client.Update(ctx, s)
}

func (k *KubeClient) DeleteSecret(ctx context.Context, name string) error {
	err := k.Client.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace}})
	return client.IgnoreNotFound(err)
}
