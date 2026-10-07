//go:build e2e

package registry

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// TestLive pushes the built test charm to jk-registry on kind-jk-dev (go test -tags e2e).
func TestLive(t *testing.T) {
	cfg, err := config.GetConfigWithContext("kind-jk-dev")
	if err != nil {
		t.Fatal(err)
	}
	c, closeFn, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	d, err := c.Push(context.Background(), "../../bin/jk-test.charm")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := c.Charm(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s %v %+v", d, ch.Metadata["name"], ch.Base)
}
