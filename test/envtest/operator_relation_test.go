package envtest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
)

const (
	digestProvider = "sha256:aa01"
	digestClient   = "sha256:aa02"
)

var relCharmsOnce sync.Once

// addRelationCharms registers a provider with a container and a client without any (like data-integrator).
func addRelationCharms() {
	relCharmsOnce.Do(func() {
		opCharms.add(digestProvider, &registry.Charm{
			Metadata: map[string]any{
				"name":       "provider",
				"containers": map[string]any{"workload": map[string]any{"resource": "img"}},
				"resources":  map[string]any{"img": map[string]any{"type": "oci-image", "upstream-source": "busybox:1"}},
				"provides": map[string]any{
					"database": map[string]any{"interface": "pg"},
					"limited":  map[string]any{"interface": "lim", "limit": 1},
				},
				"requires": map[string]any{"certs": map[string]any{"interface": "tls"}},
				"peers":    map[string]any{"cluster": map[string]any{"interface": "pgp"}},
			},
			Base: registry.Base{Name: "ubuntu", Channel: "22.04"},
		})
		opCharms.add(digestClient, &registry.Charm{
			Metadata: map[string]any{
				"name": "client",
				"requires": map[string]any{
					"db": map[string]any{"interface": "pg"}, "db2": map[string]any{"interface": "pg"}, "bad": map[string]any{"interface": "other"},
					"lim": map[string]any{"interface": "lim"}, "lim2": map[string]any{"interface": "lim"},
				},
				"provides": map[string]any{"out": map[string]any{"interface": "pg"}},
			},
			Base: registry.Base{Name: "ubuntu", Channel: "22.04"},
		})
	})
}

func epRef(ns, app, ep string) v1alpha1.EndpointRef {
	return v1alpha1.EndpointRef{Namespace: ns, Application: app, Endpoint: ep}
}

func newRelation(t *testing.T, c client.Client, ns, name string, a, b v1alpha1.EndpointRef) *v1alpha1.Relation {
	t.Helper()
	rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{a, b}}}
	if err := c.Create(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	return rel
}

func relationState(c client.Client, ns, name string) (valid *metav1.Condition, id int64) {
	var r v1alpha1.Relation
	if get(c, ns, name, &r) != nil {
		return nil, 0
	}
	return meta.FindStatusCondition(r.Status.Conditions, v1alpha1.RelationValid), r.Status.ID
}

// readyCondition is the Relation's Ready condition (nil until set).
func readyCondition(c client.Client, ns, name string) *metav1.Condition {
	var r v1alpha1.Relation
	if get(c, ns, name, &r) != nil {
		return nil
	}
	return meta.FindStatusCondition(r.Status.Conditions, v1alpha1.RelationReady)
}

func waitInvalid(t *testing.T, c client.Client, ns, name, reason string) {
	t.Helper()
	eventually(t, name+" invalid: "+reason, func() bool {
		cond, _ := relationState(c, ns, name)
		return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == reason && cond.Message != ""
	})
	if _, id := relationState(c, ns, name); id != 0 {
		t.Errorf("invalid relation %s got id %d", name, id)
	}
	eventually(t, name+" not ready, for the same reason", func() bool {
		cond := readyCondition(c, ns, name)
		return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == reason
	})
}

func waitValid(t *testing.T, c client.Client, ns, name string) int64 {
	t.Helper()
	var id int64
	eventually(t, name+" valid with an id", func() bool {
		var cond *metav1.Condition
		cond, id = relationState(c, ns, name)
		return cond != nil && cond.Status == metav1.ConditionTrue && id != 0
	})
	eventually(t, name+" ready", func() bool {
		cond := readyCondition(c, ns, name)
		return cond != nil && cond.Status == metav1.ConditionTrue
	})
	return id
}

// scopeUnit creates a pod and UnitData for unit app-n with the relation in scope (or not).
func scopeUnit(t *testing.T, c client.Client, ns, app string, n int, relID int64, inScope bool, withPod bool) {
	t.Helper()
	if withPod {
		createPod(t, c, ns, app, n, true)
	}
	ud := &v1alpha1.UnitData{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("%s-%d", app, n)},
		Spec:       v1alpha1.UnitDataSpec{Relations: map[string]v1alpha1.UnitRelationState{strconv.FormatInt(relID, 10): {InScope: inScope}}},
	}
	if err := c.Create(context.Background(), ud); err != nil {
		t.Fatal(err)
	}
}

func leaveScope(t *testing.T, c client.Client, ns, name string, relID int64) {
	t.Helper()
	var ud v1alpha1.UnitData
	if err := get(c, ns, name, &ud); err != nil {
		t.Fatal(err)
	}
	ud.Spec.Relations[strconv.FormatInt(relID, 10)] = v1alpha1.UnitRelationState{InScope: false}
	if err := c.Update(context.Background(), &ud); err != nil {
		t.Fatal(err)
	}
}

func TestRelationValidation(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "db", digestProvider, 1)
	newApp(t, c, ns, "client", digestClient, 1)
	newApp(t, c, ns, "client2", digestClient, 1)
	for _, a := range []string{"db", "client", "client2"} {
		eventually(t, a+" charm resolved", func() bool { r := ready(c, ns, a); return r != nil && r.Status.Charm != nil })
	}

	good := newRelation(t, c, ns, "good", epRef(ns, "db", "database"), epRef(ns, "client", "db"))
	goodID := waitValid(t, c, ns, "good")
	eventually(t, "finalizer", func() bool {
		_ = get(c, ns, "good", good)
		return len(good.Finalizers) == 1 && good.Finalizers[0] == operator.RelationFinalizer
	})

	cases := []struct {
		name, reason string
		a, b         v1alpha1.EndpointRef
	}{
		{"missing-endpoint", "EndpointNotFound", epRef(ns, "db", "database"), epRef(ns, "client", "nope")},
		{"missing-app", "ApplicationNotFound", epRef(ns, "db", "database"), epRef(ns, "ghost", "db")},
		{"both-provide", "IncompatibleEndpoints", epRef(ns, "db", "database"), epRef(ns, "client", "out")},
		{"itself", "SameApplication", epRef(ns, "db", "database"), epRef(ns, "db", "certs")},
		{"interface", "InterfaceMismatch", epRef(ns, "db", "database"), epRef(ns, "client", "bad")},
		{"duplicate", "Duplicate", epRef(ns, "client", "db"), epRef(ns, "db", "database")},
		{"namespace", "OfferMissing", epRef(ns, "db", "database"), epRef("other", "client", "db")},
		{"peer-by-user", "IncompatibleEndpoints", epRef(ns, "db", "cluster"), epRef(ns, "client", "db")},
	}
	for _, tc := range cases {
		newRelation(t, c, ns, tc.name, tc.a, tc.b)
	}
	for _, tc := range cases {
		waitInvalid(t, c, ns, tc.name, tc.reason)
	}

	// The limit of 1 on db:limited lets the first relation in and holds the second back.
	newRelation(t, c, ns, "lim1", epRef(ns, "db", "limited"), epRef(ns, "client", "lim"))
	waitValid(t, c, ns, "lim1")
	newRelation(t, c, ns, "lim2", epRef(ns, "db", "limited"), epRef(ns, "client2", "lim"))
	waitInvalid(t, c, ns, "lim2", "LimitExceeded")

	// An Application that appears later makes a waiting Relation valid; its id comes from the shared counter.
	newRelation(t, c, ns, "late", epRef(ns, "db", "database"), epRef(ns, "late", "db2"))
	waitInvalid(t, c, ns, "late", "ApplicationNotFound")
	newApp(t, c, ns, "late", digestClient, 1)
	lateID := waitValid(t, c, ns, "late")
	if lateID == goodID {
		t.Errorf("ids repeat: %d", lateID)
	}

	// Removing the first limited relation lets the held-back one in.
	if err := c.Delete(context.Background(), &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "lim1"}}); err != nil {
		t.Fatal(err)
	}
	waitValid(t, c, ns, "lim2")
}

func TestRelationRemovalWaitsForUnits(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "db", digestProvider, 2)
	newApp(t, c, ns, "client", digestClient, 1)
	eventually(t, "apps", func() bool {
		a, b := ready(c, ns, "db"), ready(c, ns, "client")
		return a != nil && b != nil && a.Status.Charm != nil && b.Status.Charm != nil
	})
	newRelation(t, c, ns, "r", epRef(ns, "db", "database"), epRef(ns, "client", "db"))
	id := waitValid(t, c, ns, "r")
	scopeUnit(t, c, ns, "db", 0, id, true, true)
	scopeUnit(t, c, ns, "db", 1, id, true, false) // no pod: nothing would ever clear it, so nothing waits for it
	scopeUnit(t, c, ns, "client", 0, id, true, true)

	rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "r"}}
	if err := c.Delete(ctx, rel); err != nil {
		t.Fatal(err)
	}
	eventually(t, "Removing condition names the waiting units", func() bool {
		var r v1alpha1.Relation
		cond := meta.FindStatusCondition(func() []metav1.Condition { _ = get(c, ns, "r", &r); return r.Status.Conditions }(), v1alpha1.RelationRemoving)
		return cond != nil && cond.Status == metav1.ConditionTrue && strings.Contains(cond.Message, "db/0") && strings.Contains(cond.Message, "client/0") && !strings.Contains(cond.Message, "db/1")
	})
	eventually(t, "Ready turns false while the relation is being removed", func() bool {
		cond := readyCondition(c, ns, "r")
		return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == "Removing"
	})
	leaveScope(t, c, ns, "db-0", id)
	consistently(t, "relation removed while client/0 is in scope", func() bool { return exists(c, ns, "r", &v1alpha1.Relation{}) })
	leaveScope(t, c, ns, "client-0", id)
	eventually(t, "relation removed", func() bool { return gone(c, ns, "r", &v1alpha1.Relation{}) })
}

func TestRelationRemovalEscapes(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "db", digestProvider, 1)
	newApp(t, c, ns, "client", digestClient, 1)
	eventually(t, "apps", func() bool {
		a, b := ready(c, ns, "db"), ready(c, ns, "client")
		return a != nil && b != nil && a.Status.Charm != nil && b.Status.Charm != nil
	})
	newRelation(t, c, ns, "forced", epRef(ns, "db", "database"), epRef(ns, "client", "db"))
	forcedID := waitValid(t, c, ns, "forced")
	scopeUnit(t, c, ns, "db", 0, forcedID, true, true)
	if err := c.Delete(ctx, &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "forced"}}); err != nil {
		t.Fatal(err)
	}
	consistently(t, "forced relation gone early", func() bool { return exists(c, ns, "forced", &v1alpha1.Relation{}) })
	var r v1alpha1.Relation
	if err := get(c, ns, "forced", &r); err != nil {
		t.Fatal(err)
	}
	r.Annotations = map[string]string{v1alpha1.ForceRemoveAnnotation: "true"}
	if err := c.Update(ctx, &r); err != nil {
		t.Fatal(err)
	}
	eventually(t, "forced relation removed", func() bool { return gone(c, ns, "forced", &v1alpha1.Relation{}) })

	// A terminating namespace drops the finalizer at once, even with units in scope.
	newRelation(t, c, ns, "doomed", epRef(ns, "db", "database"), epRef(ns, "client", "db2"))
	doomedID := waitValid(t, c, ns, "doomed")
	scopeUnit(t, c, ns, "client", 0, doomedID, true, true)
	if err := c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "namespace terminating", func() bool {
		var n corev1.Namespace
		return c.Get(ctx, client.ObjectKey{Name: ns}, &n) == nil && n.DeletionTimestamp != nil
	})
	if err := c.Delete(ctx, &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "doomed"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "relation dropped in a terminating namespace", func() bool { return gone(c, ns, "doomed", &v1alpha1.Relation{}) })
}

func TestApplicationRemovalRemovesItsRelationsFirst(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "db", digestProvider, 1)
	newApp(t, c, ns, "client", digestClient, 1)
	eventually(t, "apps", func() bool {
		a, b := ready(c, ns, "db"), ready(c, ns, "client")
		return a != nil && b != nil && a.Status.Charm != nil && b.Status.Charm != nil && len(a.Finalizers) == 1
	})
	newRelation(t, c, ns, "r", epRef(ns, "db", "database"), epRef(ns, "client", "db"))
	id := waitValid(t, c, ns, "r")
	scopeUnit(t, c, ns, "db", 0, id, true, true)
	scopeUnit(t, c, ns, "client", 0, id, true, true)

	if err := c.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	eventually(t, "relation deleted by the application's removal", func() bool {
		var r v1alpha1.Relation
		return get(c, ns, "r", &r) == nil && r.DeletionTimestamp != nil
	})
	// The pods keep running (their hooks need them) until the relation is gone.
	consistently(t, "application scaled down while its relation is being removed", func() bool {
		var sts appsv1.StatefulSet
		return get(c, ns, "db", &sts) == nil && *sts.Spec.Replicas == 1 && ready(c, ns, "db") != nil
	})
	leaveScope(t, c, ns, "db-0", id)
	leaveScope(t, c, ns, "client-0", id)
	eventually(t, "relation gone", func() bool { return gone(c, ns, "r", &v1alpha1.Relation{}) })
	eventually(t, "scaled to zero", func() bool {
		var sts appsv1.StatefulSet
		return get(c, ns, "db", &sts) == nil && *sts.Spec.Replicas == 0
	})
	if err := c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "db-0"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "application released", func() bool { return ready(c, ns, "db") == nil })
	if !exists(c, ns, "client", &v1alpha1.Application{}) {
		t.Error("the other application must stay")
	}
}

func TestGrantScopedToARelationEndsWithIt(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "db", digestProvider, 1)
	newApp(t, c, ns, "client", digestClient, 1)
	eventually(t, "apps", func() bool {
		a, b := ready(c, ns, "db"), ready(c, ns, "client")
		return a != nil && b != nil && a.Status.Charm != nil && b.Status.Charm != nil
	})
	newRelation(t, c, ns, "r", epRef(ns, "db", "database"), epRef(ns, "client", "db"))
	id := waitValid(t, c, ns, "r")
	grants := []v1alpha1.SecretGrant{{Application: "client", Relation: fmt.Sprintf("db:%d", id)}}
	for _, s := range []*corev1.Secret{secretObj(ns, "jk-secret-rel", "rel", "db", grants), secretObj(ns, "jk-secret-rel-1", "rel", "db", nil)} {
		if err := c.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "client may read the secret", func() bool { return secretsRole(c, ns, "client")["jk-secret-rel-1"] == "get,list,watch" })
	if err := c.Delete(ctx, &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "r"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "access ends with the relation", func() bool {
		return gone(c, ns, "r", &v1alpha1.Relation{}) && len(secretsRole(c, ns, "client")) == 0 && secretsRole(c, ns, "client") != nil
	})
	if _, ok := secretsRole(c, ns, "db")["jk-secret-rel-1"]; !ok {
		t.Error("the owner keeps its secret")
	}
}

func TestApplicationWithoutContainers(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "client", digestClient, 1)
	var sts appsv1.StatefulSet
	eventually(t, "statefulset", func() bool { return exists(c, ns, "client", &sts) })
	pod := sts.Spec.Template.Spec
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "charm" || len(pod.InitContainers) != 1 {
		t.Fatalf("pod containers %v init %v", pod.Containers, pod.InitContainers)
	}
	eventually(t, "waiting for units, not invalid", func() bool { return readyReason(c, ns, "client") == "UnitsNotReady" })
	sts.Status.Replicas, sts.Status.ReadyReplicas = 1, 1
	if err := c.Status().Update(ctx, &sts); err != nil {
		t.Fatal(err)
	}
	touch(t, c, ns, "client")
	eventually(t, "ready", func() bool { return readyReason(c, ns, "client") == "Ready" })
}
