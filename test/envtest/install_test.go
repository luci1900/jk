package envtest

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/install"
)

func newClient(t *testing.T) client.Client {
	t.Helper()
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (set KUBEBUILDER_ASSETS; see `make test-envtest`): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apiextensionsv1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestInstallIsIdempotentAndCRDsWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newClient(t)
	opts := install.Options{OperatorImage: "example.invalid/jk-operator:test"}
	// Twice: install must be idempotent.
	for i := 0; i < 2; i++ {
		if err := install.Apply(ctx, c, opts); err != nil {
			t.Fatalf("apply #%d: %v", i+1, err)
		}
	}

	// The registry credentials are created once and survive a re-install.
	var auth corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: v1alpha1.SystemNamespace, Name: install.RegistryAuthSecret}, &auth); err != nil {
		t.Fatal(err)
	}
	first := auth.Data["password"]
	if len(first) == 0 || len(auth.Data["htpasswd"]) == 0 {
		t.Fatalf("registry auth incomplete: %v", auth.Data)
	}
	if err := install.Apply(ctx, c, opts); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(&auth), &auth); err != nil || string(auth.Data["password"]) != string(first) {
		t.Fatalf("registry password rotated on re-install (err %v)", err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "m", Labels: map[string]string{v1alpha1.ModelLabel: "true"}}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	// A two-line Application must be accepted and defaulted.
	app := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "m"},
		Spec:       v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: "postgresql-k8s"}},
	}
	if err := c.Create(ctx, app); err != nil {
		t.Fatal(err)
	}
	if app.Spec.Scale == nil || *app.Spec.Scale != 1 || app.Spec.Trust != v1alpha1.TrustNone || app.Spec.Charm.Source != "charmhub" {
		t.Fatalf("defaults not applied: %+v", app.Spec)
	}
	bad := app.DeepCopy()
	bad.ObjectMeta = metav1.ObjectMeta{Name: "bad", Namespace: "m"}
	bad.Spec.Trust = "everything"
	if err := c.Create(ctx, bad); err == nil {
		t.Fatal("invalid trust accepted")
	}
}

func TestUninstall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := newClient(t)
	opts := install.Options{OperatorImage: "example.invalid/jk-operator:test"}
	if err := install.Apply(ctx, c, opts); err != nil {
		t.Fatal(err)
	}
	if err := install.Uninstall(ctx, c); err != nil {
		t.Fatal(err)
	}
	// Uninstalling twice is fine.
	if err := install.Uninstall(ctx, c); err != nil {
		t.Fatalf("second uninstall: %v", err)
	}
	// The CRDs are deleted (asynchronously). envtest has no namespace controller, so jk-system itself stays
	// Terminating; the real cluster check is `make dev`.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var crds apiextensionsv1.CustomResourceDefinitionList
		if err := c.List(ctx, &crds); err != nil {
			t.Fatal(err)
		}
		left := 0
		for _, crd := range crds.Items {
			if crd.Spec.Group == v1alpha1.Group {
				left++
			}
		}
		if left == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jk CRDs left after uninstall", left)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
