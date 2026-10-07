//go:build charmhub_live

package charmhub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
	"time"
)

// Run with: go test -tags charmhub_live ./pkg/charmhub (needs network).
func TestLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := &Client{UserAgent: "jk-live-test"}

	ch, err := c.Resolve(ctx, Request{Name: "postgresql-k8s", Channel: "14/stable", Base: Base{Architecture: "amd64"}})
	if err != nil {
		t.Fatal(err)
	}
	if ch.Base.String() != "ubuntu@22.04" || ch.Revision == 0 || len(ch.SHA256) != 64 || ch.MetadataYAML == "" || ch.ConfigYAML == "" {
		t.Errorf("unexpected: %+v", ch)
	}
	t.Logf("postgresql-k8s 14/stable amd64: revision %d, %s, %d bytes", ch.Revision, ch.Base, ch.Size)

	// A small charm, downloaded and verified end to end.
	small, err := c.Resolve(ctx, Request{Name: "hello-kubecon", Base: Base{Architecture: "amd64"}})
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := c.Download(ctx, small.DownloadURL, small.SHA256, io.MultiWriter(h, io.Discard)); err != nil {
		t.Fatal(err)
	}
	t.Logf("hello-kubecon: revision %d sha256 %s", small.Revision, hex.EncodeToString(h.Sum(nil)))

	if _, err := c.Resolve(ctx, Request{Name: "postgresql-k8s", Channel: "nope/stable", Base: Base{Architecture: "amd64"}}); !Is(err, CodeRevisionNotFound) {
		t.Errorf("err %v", err)
	}
	info, err := c.Info(ctx, "postgresql-k8s")
	if err != nil || len(info.Channels) == 0 {
		t.Errorf("info %v %v", info, err)
	}
}
