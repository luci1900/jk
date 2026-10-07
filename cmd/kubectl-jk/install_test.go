package main

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

func TestUninstallDestroysModelsFirst(t *testing.T) {
	e := newEnv(t, app("pg", 1))
	gone := func() bool {
		err := e.kube.Get(context.Background(), client.ObjectKey{Name: "m"}, &corev1.Namespace{})
		return apierrors.IsNotFound(err)
	}
	// Declined: nothing is destroyed.
	e.in = "n\n"
	if _, errOut, err := e.run("uninstall"); err == nil || !strings.Contains(errOut, "m (1 applications)") || gone() {
		t.Fatalf("declined: %v %q", err, errOut)
	}
	e.in = "y\n"
	out, _, err := e.run("uninstall")
	if err != nil || out != "Destroying model \"m\"\njk uninstalled\n" || !gone() {
		t.Fatalf("%q %v", out, err)
	}
	var apps v1alpha1.ApplicationList
	if err := e.kube.List(context.Background(), &apps); err != nil || len(apps.Items) != 0 {
		t.Fatalf("%v %v", apps.Items, err)
	}
	// No models: no prompt.
	e.in = ""
	if out, _, err := e.run("uninstall"); err != nil || out != "jk uninstalled\n" {
		t.Fatalf("%q %v", out, err)
	}
}
