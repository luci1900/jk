package registry

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
)

func newServer(t *testing.T) *Client {
	t.Helper()
	h := registry.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if u, p, ok := r.BasicAuth(); !ok || u != "jk" || p != "secret" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return &Client{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Username: "jk", Password: "secret", PlainHTTP: true, Arch: "arm64"}
}

func writeCharm(t *testing.T, files map[string]string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "x.charm")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for n, c := range files {
		w, err := zw.Create(n)
		if err != nil {
			t.Fatal(err)
		}
		w.Write([]byte(c))
	}
	zw.Close()
	f.Close()
	return p
}

var charmFiles = map[string]string{
	"metadata.yaml": "name: x\ncontainers:\n  web:\n    resource: img\n",
	"config.yaml":   "options:\n  greeting:\n    type: string\n    default: hello\n",
	"actions.yaml":  "ping:\n  description: p\n",
	"manifest.yaml": "bases:\n- name: ubuntu\n  channel: '22.04'\n  architectures: [arm64]\n",
	"src/charm.py":  "print('hi')\n",
	"dispatch":      "#!/bin/sh\n",
}

func TestRoundTrip(t *testing.T) {
	c := newServer(t)
	ctx := context.Background()
	d, err := c.Push(ctx, writeCharm(t, charmFiles))
	if err != nil {
		t.Fatal(err)
	}
	// reading is anonymous
	anon := *c
	anon.Username, anon.Password = "", ""
	ch, err := anon.Charm(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if ch.Digest != d || ch.Metadata["name"] != "x" {
		t.Fatalf("metadata: %+v", ch)
	}
	opts := ch.Config["options"].(map[string]any)["greeting"].(map[string]any)
	if opts["default"] != "hello" {
		t.Fatalf("config: %+v", ch.Config)
	}
	if _, ok := ch.Actions["ping"]; !ok {
		t.Fatalf("actions: %+v", ch.Actions)
	}
	if ch.Base.Name != "ubuntu" || ch.Base.Channel != "22.04" || ch.Base.Architectures[0] != "arm64" {
		t.Fatalf("base: %+v", ch.Base)
	}
}

func TestDigestStable(t *testing.T) {
	c := newServer(t)
	ctx := context.Background()
	d1, err := c.Push(ctx, writeCharm(t, charmFiles))
	if err != nil {
		t.Fatal(err)
	}
	d2, err := c.Push(ctx, writeCharm(t, charmFiles))
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatalf("digest changed: %s %s", d1, d2)
	}
	other := map[string]string{}
	for k, v := range charmFiles {
		other[k] = v
	}
	other["src/charm.py"] = "print('other')\n"
	d3, _ := c.Push(ctx, writeCharm(t, other))
	if d3 == d1 {
		t.Fatal("different charm, same digest")
	}
}

func TestAnonymousPushRejected(t *testing.T) {
	c := newServer(t)
	for _, creds := range [][2]string{{"", ""}, {"jk", "wrong"}} {
		c.Username, c.Password = creds[0], creds[1]
		if _, err := c.Push(context.Background(), writeCharm(t, charmFiles)); err == nil {
			t.Fatalf("push with %v succeeded", creds)
		}
	}
}

func TestDefaultsWithoutOptionalFiles(t *testing.T) {
	c := newServer(t)
	d, err := c.Push(context.Background(), writeCharm(t, map[string]string{"metadata.yaml": "name: y\n"}))
	if err != nil {
		t.Fatal(err)
	}
	ch, err := c.Charm(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if ch.Config != nil || ch.Actions != nil || ch.Base.Channel != "22.04" {
		t.Fatalf("%+v", ch)
	}
}

func TestErrors(t *testing.T) {
	c := newServer(t)
	ctx := context.Background()
	if _, err := c.Push(ctx, "/nonexistent.charm"); err == nil {
		t.Fatal("expected error for missing file")
	}
	d, err := c.Push(ctx, writeCharm(t, map[string]string{"dispatch": "x"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Charm(ctx, d); err == nil || !strings.Contains(err.Error(), "metadata.yaml") {
		t.Fatalf("want missing metadata error, got %v", err)
	}
	d, _ = c.Push(ctx, writeCharm(t, map[string]string{"metadata.yaml": "name: [\n"}))
	if _, err := c.Charm(ctx, d); err == nil {
		t.Fatal("expected parse error")
	}
	d, _ = c.Push(ctx, writeCharm(t, map[string]string{"metadata.yaml": "name: a\n", "manifest.yaml": "bases: x\n"}))
	if _, err := c.Charm(ctx, d); err == nil {
		t.Fatal("expected manifest parse error")
	}
	if _, err := c.Charm(ctx, "sha256:0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("expected not found")
	}
	if _, err := c.Charm(ctx, "bogus"); err == nil {
		t.Fatal("expected bad digest")
	}
}

func TestDirsAndSymlinks(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.charm")
	f, _ := os.Create(p)
	zw := zip.NewWriter(f)
	zw.Create("lib/")
	w, _ := zw.Create("metadata.yaml")
	w.Write([]byte("name: z\n"))
	h := &zip.FileHeader{Name: "link"}
	h.SetMode(os.ModeSymlink | 0o777)
	w, _ = zw.CreateHeader(h)
	w.Write([]byte("metadata.yaml"))
	zw.Close()
	f.Close()
	c := newServer(t)
	d, err := c.Push(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Charm(context.Background(), d); err != nil {
		t.Fatal(err)
	}
}

func TestPushReaderMatchesPushAndExists(t *testing.T) {
	c := newServer(t)
	ctx := context.Background()
	p := writeCharm(t, charmFiles)
	want, err := c.Push(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.PushReader(ctx, bytes.NewReader(b), int64(len(b)))
	if err != nil || got != want {
		t.Fatalf("PushReader = %q, %v; want %q", got, err, want)
	}
	other := *c
	other.Arch = "amd64"
	if d, err := other.PushReader(ctx, bytes.NewReader(b), int64(len(b))); err != nil || d == want {
		t.Fatalf("a different architecture must give a different image: %q %v", d, err)
	}
	if ok, err := c.Exists(ctx, want); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
	if ok, err := c.Exists(ctx, "sha256:0000000000000000000000000000000000000000000000000000000000000000"); err != nil || ok {
		t.Fatalf("Exists(missing) = %v, %v", ok, err)
	}
	if _, err := c.Exists(ctx, "bogus"); err == nil {
		t.Fatal("bad digest")
	}
	if _, err := c.PushReader(ctx, strings.NewReader("not a zip"), 9); err == nil {
		t.Fatal("expected error for a non-zip")
	}
	dead := &Client{Endpoint: "127.0.0.1:1", PlainHTTP: true}
	if _, err := dead.Exists(ctx, want); err == nil {
		t.Fatal("unreachable registry must be an error, not 'missing'")
	}
}
