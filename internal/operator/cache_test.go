package operator

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/pkg/charmhub"
)

type stubHub struct{ downloads int }

func (h *stubHub) Resolve(context.Context, charmhub.Request) (*charmhub.Charm, error) {
	return nil, errors.New("unused")
}

func (h *stubHub) Download(_ context.Context, _, _ string, w io.Writer) (int64, error) {
	h.downloads++
	n, err := w.Write([]byte("charm"))
	return int64(n), err
}

type stubStore struct {
	have     bool
	digest   string
	existsN  int
	existErr error
}

func (s *stubStore) Exists(context.Context, string) (bool, error) {
	s.existsN++
	return s.have, s.existErr
}
func (s *stubStore) PushCharm(context.Context, io.ReaderAt, int64, string) (string, error) {
	s.have = true
	return s.digest, nil
}

func TestEnsureCachedRefillsLostImage(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	app := testApp(pgMetadata)
	app.Status.Charm.URL = "https://api.charmhub.io/x.charm"
	app.Status.Charm.Image = "jk-registry.jk-system.svc:5000/charms@sha256:abc"
	hub, st := &stubHub{}, &stubStore{digest: "sha256:abc"}
	r := &ApplicationReconciler{Hub: hub, Store: st, Config: testConfig(), Now: func() time.Time { return now }}

	// Missing: downloaded and pushed again; checked once, then trusted for the TTL.
	if err := r.ensureCached(ctx, app); err != nil || hub.downloads != 1 || !st.have {
		t.Fatalf("refill: %v downloads=%d", err, hub.downloads)
	}
	if err := r.ensureCached(ctx, app); err != nil || st.existsN != 1 {
		t.Fatalf("trusted: %v exists=%d", err, st.existsN)
	}
	now = now.Add(verifiedTTL + time.Second)
	if err := r.ensureCached(ctx, app); err != nil || st.existsN != 2 || hub.downloads != 1 {
		t.Fatalf("present: %v exists=%d downloads=%d", err, st.existsN, hub.downloads)
	}

	// A refill that yields another digest is refused.
	r = &ApplicationReconciler{Hub: hub, Store: &stubStore{digest: "sha256:other"}, Config: testConfig()}
	if err := r.ensureCached(ctx, app); !errors.Is(err, errCharmUnavailable) || !strings.Contains(err.Error(), "sha256:other") {
		t.Errorf("digest change: %v", err)
	}
	// Registry errors are transient.
	r = &ApplicationReconciler{Hub: hub, Store: &stubStore{existErr: errors.New("down")}, Config: testConfig()}
	if err := r.ensureCached(ctx, app); !errors.Is(err, errCharmUnavailable) {
		t.Errorf("exists error: %v", err)
	}
	// Local charms and unconfigured operators skip the check.
	local := testApp(pgMetadata)
	if err := r.ensureCached(ctx, local); err != nil {
		t.Error(err)
	}
	if err := (&ApplicationReconciler{}).ensureCached(ctx, app); err != nil {
		t.Error(err)
	}
	_ = v1alpha1.Application{}
}

func TestResolveCharmhubNeedsConfiguration(t *testing.T) {
	app := testApp(pgMetadata)
	app.Status.Charm = nil
	if err := (&ApplicationReconciler{}).resolveCharmhub(context.Background(), app); !errors.Is(err, errInvalid) {
		t.Errorf("%v", err)
	}
}
