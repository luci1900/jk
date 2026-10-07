package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/luci1900/jk/api/v1alpha1"
)

// fakeKube is an in-memory Kube.
type fakeKube struct {
	mu       sync.Mutex
	app      *v1alpha1.Application
	model    map[string]string
	unit     *v1alpha1.UnitDataSpec
	appData  *v1alpha1.AppDataSpec
	lease    *coordinationv1.Lease
	resolved string

	relations []v1alpha1.Relation
	remote    []v1alpha1.RemoteData
	// others are the other units' UnitData by object name (app-1); appDatas are other applications' AppData.
	others   map[string]*v1alpha1.UnitDataSpec
	appDatas map[string]*v1alpha1.AppDataSpec
	secrets  map[string]*corev1.Secret
	// forbidden names of secrets that cannot be read yet; failCreates fails secret creation after n successes.
	forbidden   map[string]bool
	failSecrets int
	failAppData int
	secretOps   []string
	// actions are the Actions in the namespace by object name; actionWrites counts status writes.
	actions      map[string]*v1alpha1.Action
	actionWrites int

	saves       int
	failSaves   int // fail this many SaveUnitData calls
	appWrites   int
	cleared     int
	renewals    int
	renewErr    error
	unitHistory []v1alpha1.UnitDataSpec
}

func newFakeKube() *fakeKube {
	scale := int32(1)
	return &fakeKube{
		app: &v1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "uid"},
			Spec:       v1alpha1.ApplicationSpec{Scale: &scale},
		},
		model: map[string]string{},
	}
}

func (k *fakeKube) setHolder(holder string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	d := int32(60)
	k.lease = &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "app-leader", Namespace: "ns"},
		Spec:       coordinationv1.LeaseSpec{LeaseDurationSeconds: &d},
	}
	if holder != "" {
		k.lease.Spec.HolderIdentity = &holder
	}
}

func (k *fakeKube) setConfig(kv map[string]string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.app.Spec.Config = kv
}

func (k *fakeKube) setTrust(t v1alpha1.Trust) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.app.Spec.Trust = t
}

func (k *fakeKube) Application(context.Context) (*v1alpha1.Application, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.app == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "applications"}, "app")
	}
	return k.app.DeepCopy(), nil
}

func (k *fakeKube) ModelConfig(context.Context) (map[string]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := map[string]string{}
	for a, b := range k.model {
		out[a] = b
	}
	return out, nil
}

func (k *fakeKube) UnitData(context.Context) (*v1alpha1.UnitData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.unit == nil {
		return nil, nil
	}
	return &v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: "app-0", UID: "unit-uid"}, Spec: *k.unit.DeepCopy()}, nil
}

func (k *fakeKube) SaveUnitData(_ context.Context, spec v1alpha1.UnitDataSpec) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failSaves > 0 {
		k.failSaves--
		return errors.New("save failed")
	}
	k.saves++
	k.unit = spec.DeepCopy()
	k.unitHistory = append(k.unitHistory, *spec.DeepCopy())
	return nil
}

func (k *fakeKube) AppData(context.Context) (*v1alpha1.AppData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.appData == nil {
		return nil, nil
	}
	return &v1alpha1.AppData{Spec: *k.appData.DeepCopy()}, nil
}

func (k *fakeKube) UpdateAppData(_ context.Context, mutate func(*v1alpha1.AppDataSpec)) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failAppData > 0 {
		k.failAppData--
		return errors.New("appdata write failed")
	}
	if k.appData == nil {
		k.appData = &v1alpha1.AppDataSpec{}
	}
	mutate(k.appData)
	k.appWrites++
	return nil
}

func (k *fakeKube) Lease(context.Context) (*coordinationv1.Lease, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.lease == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "leases"}, "app-leader")
	}
	return k.lease.DeepCopy(), nil
}

func (k *fakeKube) RenewLease(_ context.Context, holder string, now time.Time) (*coordinationv1.Lease, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.renewErr != nil {
		return nil, k.renewErr
	}
	if k.lease == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "leases"}, "app-leader")
	}
	if k.lease.Spec.HolderIdentity != nil && *k.lease.Spec.HolderIdentity == holder {
		t := metav1.NewMicroTime(now)
		k.lease.Spec.RenewTime = &t
		k.renewals++
	}
	return k.lease.DeepCopy(), nil
}

func (k *fakeKube) Resolved(context.Context) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.resolved, nil
}

func (k *fakeKube) ClearResolved(context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.resolved = ""
	k.cleared++
	return nil
}

func (k *fakeKube) spec() v1alpha1.UnitDataSpec {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.unit == nil {
		return v1alpha1.UnitDataSpec{}
	}
	return *k.unit.DeepCopy()
}

// fakePebble answers with a boot ID per container; missing means not up.
type fakePebble struct {
	mu   sync.Mutex
	boot map[string]string
}

func (p *fakePebble) set(container, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.boot == nil {
		p.boot = map[string]string{}
	}
	if id == "" {
		delete(p.boot, container)
	} else {
		p.boot[container] = id
	}
}

func (p *fakePebble) BootID(_ context.Context, container string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.boot[container]; ok {
		return id, nil
	}
	return "", errors.New("pebble not up")
}

func metav1Now() metav1.Time { return metav1.Now() }

func metaWithDeletion(t metav1.Time) metav1.ObjectMeta {
	return metav1.ObjectMeta{DeletionTimestamp: &t}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func (k *fakeKube) Relations(context.Context) ([]v1alpha1.Relation, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]v1alpha1.Relation(nil), k.relations...), nil
}

func (k *fakeKube) RemoteDatas(context.Context) ([]v1alpha1.RemoteData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]v1alpha1.RemoteData(nil), k.remote...), nil
}

func (k *fakeKube) UnitDatas(context.Context) ([]v1alpha1.UnitData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var out []v1alpha1.UnitData
	if k.unit != nil {
		out = append(out, v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: "app-0", UID: "unit-uid", Labels: map[string]string{v1alpha1.AppLabel: "app"}}, Spec: *k.unit.DeepCopy()})
	}
	for name, sp := range k.others {
		app := "app"
		if i := strings.LastIndexByte(name, '-'); i > 0 {
			app = name[:i]
		}
		out = append(out, v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.AppLabel: app}}, Spec: *sp.DeepCopy()})
	}
	return out, nil
}

func (k *fakeKube) AppDataOf(_ context.Context, app string) (*v1alpha1.AppData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if app == "app" {
		if k.appData == nil {
			return nil, nil
		}
		return &v1alpha1.AppData{Spec: *k.appData.DeepCopy()}, nil
	}
	if sp, ok := k.appDatas[app]; ok {
		return &v1alpha1.AppData{Spec: *sp.DeepCopy()}, nil
	}
	return nil, nil
}

func (k *fakeKube) GetSecret(_ context.Context, name string) (*corev1.Secret, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.forbidden[name] {
		return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, name, errors.New("no access"))
	}
	s, ok := k.secrets[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	return s.DeepCopy(), nil
}

func (k *fakeKube) CreateSecret(_ context.Context, s *corev1.Secret) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failSecrets > 0 {
		k.failSecrets--
		return errors.New("create failed")
	}
	if k.secrets == nil {
		k.secrets = map[string]*corev1.Secret{}
	}
	if _, ok := k.secrets[s.Name]; ok {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, s.Name)
	}
	k.secretOps = append(k.secretOps, "create "+s.Name)
	k.secrets[s.Name] = s.DeepCopy()
	return nil
}

func (k *fakeKube) UpdateSecret(_ context.Context, s *corev1.Secret) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	cur, ok := k.secrets[s.Name]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, s.Name)
	}
	if cur.Immutable != nil && *cur.Immutable {
		return apierrors.NewInvalid(schema.GroupKind{Kind: "Secret"}, s.Name, nil)
	}
	k.secretOps = append(k.secretOps, "update "+s.Name)
	k.secrets[s.Name] = s.DeepCopy()
	return nil
}

func (k *fakeKube) DeleteSecret(_ context.Context, name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.secretOps = append(k.secretOps, "delete "+name)
	delete(k.secrets, name)
	return nil
}
