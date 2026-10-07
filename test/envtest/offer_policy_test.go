package envtest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/config"
)

// crossRelation is a Relation in namespace ns from its application "client" to the application "db" of namespace far.
func crossRelation(name, ns, far string) *v1alpha1.Relation {
	return &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.RelationSpec{
			Endpoints: []v1alpha1.EndpointRef{{Namespace: ns, Application: "client", Endpoint: "db"}, {Namespace: far, Application: "db", Endpoint: "database"}},
			Offer:     "pg",
		},
	}
}

// TestRelationAccessPolicy runs the admission policy of relations across namespaces on a real API server (its own, with
// RBAC, since the policy asks it who may create relations where), as users who are impersonated.
func TestRelationAccessPolicy(t *testing.T) {
	ctx := context.Background()
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	env.ControlPlane.GetAPIServer().Configure().Set("authorization-mode", "RBAC")
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (set KUBEBUILDER_ASSETS; see `make test-envtest`): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	as := func(user string) client.Client {
		c := rest.CopyConfig(cfg)
		c.Impersonate = rest.ImpersonationConfig{UserName: user}
		cl, err := client.New(c, client.Options{Scheme: scheme})
		if err != nil {
			t.Fatal(err)
		}
		return cl
	}

	for _, ns := range []string{"jk-system", "consumer", "offering", "other", "third"} {
		if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatal(err)
		}
	}
	// What `jk install` applies: the access ConfigMap, the policy and its binding.
	data, err := fs.ReadFile(config.FS, "base/50-offers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := dec.Decode(&obj.Object); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatalf("%s %s: %v", obj.GetKind(), obj.GetName(), err)
		}
	}
	// alice and bob may create relations in "consumer"; bob may also in "third" (the administrator of both models).
	for _, b := range []struct{ ns, user string }{{"consumer", "alice"}, {"consumer", "bob"}, {"third", "bob"}, {"offering", "carol"}} {
		role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: b.ns, Name: "relations-" + b.user},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{v1alpha1.Group}, Resources: []string{"relations"}, Verbs: []string{"create", "update", "get", "list", "delete"}}}}
		rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: b.ns, Name: "relations-" + b.user},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name},
			Subjects: []rbacv1.Subject{{Kind: "User", Name: b.user, APIGroup: "rbac.authorization.k8s.io"}}}
		if err := admin.Create(ctx, role); err != nil {
			t.Fatal(err)
		}
		if err := admin.Create(ctx, rb); err != nil {
			t.Fatal(err)
		}
	}
	setAccess := func(keys ...string) {
		t.Helper()
		var cm corev1.ConfigMap
		if err := admin.Get(ctx, client.ObjectKey{Namespace: "jk-system", Name: v1alpha1.OfferAccessConfigMap}, &cm); err != nil {
			t.Fatal(err)
		}
		cm.Data = map[string]string{}
		for _, k := range keys {
			cm.Data[k] = ""
		}
		if err := admin.Update(ctx, &cm); err != nil {
			t.Fatal(err)
		}
	}

	// The policy takes a moment to become active: until it denies a relation nobody allows, wait.
	alice, bob, carol := as("alice"), as("bob"), as("carol")
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; ; i++ {
		rel := crossRelation(fmt.Sprintf("probe-%d", i), "consumer", "other")
		if err := alice.Create(ctx, rel); err != nil && apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
			break
		}
		_ = admin.Delete(ctx, rel)
		if time.Now().After(deadline) {
			t.Fatal("the policy never took effect")
		}
		time.Sleep(300 * time.Millisecond)
	}

	denied := func(err error) bool { return err != nil && (apierrors.IsForbidden(err) || apierrors.IsInvalid(err)) }
	t.Run("same namespace needs no offer", func(t *testing.T) {
		rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "consumer", Name: "local"},
			Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: "consumer", Application: "client", Endpoint: "db"}, {Namespace: "consumer", Application: "db", Endpoint: "database"}}}}
		if err := alice.Create(ctx, rel); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a model the offer does not allow is refused", func(t *testing.T) {
		err := alice.Create(ctx, crossRelation("refused", "consumer", "offering"))
		if !denied(err) || !strings.Contains(err.Error(), "needs an offer there that allows this namespace") {
			t.Fatalf("%v", err)
		}
	})
	t.Run("a model the offer allows is let in", func(t *testing.T) {
		setAccess("offering.consumer")
		settle(t, func() error { return alice.Create(ctx, crossRelation("allowed", "consumer", "offering")) })
		// An offer for another model does not help.
		err := alice.Create(ctx, crossRelation("elsewhere", "consumer", "other"))
		if !denied(err) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("an offer for everyone", func(t *testing.T) {
		setAccess("other._all")
		settle(t, func() error { return alice.Create(ctx, crossRelation("everyone", "consumer", "other")) })
	})
	t.Run("the administrator of both models needs no offer", func(t *testing.T) {
		setAccess()
		err := alice.Create(ctx, crossRelation("alice-third", "consumer", "third"))
		if !denied(err) {
			t.Fatalf("alice: %v", err)
		}
		settle(t, func() error { return bob.Create(ctx, crossRelation("bob-third", "consumer", "third")) })
	})
	t.Run("a relation must have an endpoint in its own namespace", func(t *testing.T) {
		rel := crossRelation("foreign", "consumer", "offering")
		rel.Spec.Endpoints[0].Namespace = "third"
		err := bob.Create(ctx, rel)
		if !denied(err) || !strings.Contains(err.Error(), "own namespace") {
			t.Fatalf("%v", err)
		}
	})
	t.Run("changing a relation cannot reach a namespace that does not allow it", func(t *testing.T) {
		setAccess("offering.consumer")
		var rel v1alpha1.Relation
		if err := alice.Get(ctx, client.ObjectKey{Namespace: "consumer", Name: "allowed"}, &rel); err != nil {
			t.Fatal(err)
		}
		rel.Spec.Endpoints[1].Namespace = "other"
		if err := alice.Update(ctx, &rel); !denied(err) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("the offering side's administrator may create the mirror in the consumer's namespace only with rights there", func(t *testing.T) {
		// The operator (a cluster administrator) is allowed anywhere: its mirror of a relation in "offering".
		mirror := crossRelation("remote.consumer.allowed", "offering", "offering")
		mirror.Spec.Endpoints[0] = v1alpha1.EndpointRef{Namespace: "consumer", Application: "client", Endpoint: "db"}
		if err := admin.Create(ctx, mirror); err != nil {
			t.Fatalf("operator: %v", err)
		}
		// carol administers "offering" only: she cannot make one that points at "consumer" without an offer there.
		setAccess()
		other := crossRelation("carol", "offering", "offering")
		other.Spec.Endpoints[0] = v1alpha1.EndpointRef{Namespace: "consumer", Application: "client", Endpoint: "db"}
		// carol may create relations in "offering" but not in "consumer"
		if err := carol.Create(ctx, other); !denied(err) {
			t.Fatalf("carol: %v", err)
		}
	})
}

// settle repeats an action that depends on a ConfigMap change the API server's policy has not seen yet.
func settle(t *testing.T, f func() error) {
	t.Helper()
	var err error
	for i := 0; i < 50; i++ {
		if err = f(); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal(err)
}
