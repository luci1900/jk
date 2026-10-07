package install

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWaitUninstalled(t *testing.T) {
	old := pollInterval
	pollInterval = time.Millisecond
	defer func() { pollInterval = old }()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apiextensionsv1.AddToScheme(scheme)
	ctx := context.Background()

	// Everything gone: returns at once.
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	if err := Uninstall(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := WaitUninstalled(ctx, c, time.Second); err != nil {
		t.Fatal(err)
	}

	// A CRD held by a finalizer is still there after the delete: the wait names it and times out.
	stuck := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{
		Name: "applications.jk.luci1900.github.io", Finalizers: []string{"test/hold"}}}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "jk-system"}}
	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(stuck, ns).Build()
	if err := Uninstall(ctx, c); err != nil {
		t.Fatal(err)
	}
	err := WaitUninstalled(ctx, c, 30*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "CustomResourceDefinition applications.jk.luci1900.github.io") {
		t.Fatalf("%v", err)
	}
}
