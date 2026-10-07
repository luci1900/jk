package charmhub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeStore serves recorded responses: each refresh request is answered by pick(request).
type fakeStore struct {
	t        *testing.T
	pick     func(a map[string]any) (int, []byte)
	requests []map[string]any
}

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v2/charms/refresh":
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			f.t.Errorf("bad request body: %v", err)
		}
		f.requests = append(f.requests, req)
		code, body := f.pick(req["actions"].([]any)[0].(map[string]any))
		w.WriteHeader(code)
		_, _ = w.Write(body)
	case r.Method == http.MethodGet && r.URL.Path == "/v2/charms/info/postgresql-k8s":
		_, _ = w.Write(fixture(f.t, "info-postgresql-k8s.json"))
	default:
		http.NotFound(w, r)
	}
}

func newStore(t *testing.T, pick func(a map[string]any) (int, []byte)) (*Client, *fakeStore) {
	t.Helper()
	f := &fakeStore{t: t, pick: pick}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return &Client{BaseURL: srv.URL, HTTPClient: srv.Client(), UserAgent: "jk-test"}, f
}

func respond(t *testing.T, name string) func(map[string]any) (int, []byte) {
	return func(map[string]any) (int, []byte) { return 200, fixture(t, name) }
}

func TestResolveFull(t *testing.T) {
	c, f := newStore(t, respond(t, "refresh-postgresql-k8s-arm64.json"))
	ch, err := c.Resolve(context.Background(), Request{Name: "postgresql-k8s", Channel: "14/stable", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "arm64"}})
	if err != nil {
		t.Fatal(err)
	}
	if ch.Revision != 959 || ch.Name != "postgresql-k8s" || ch.ID != "y2vuZHGLElMmdfcL4q1zJayL9utaheIa" || ch.Channel != "14/stable" {
		t.Errorf("charm %+v", ch)
	}
	if ch.SHA256 != "c2a5b12207688c053eb9597d51131541782fccfce1a2af064a25ee8e9b0ce2b1" || !strings.HasSuffix(ch.DownloadURL, "_959.charm") || ch.Size != 28892186 {
		t.Errorf("download %q %q %d", ch.DownloadURL, ch.SHA256, ch.Size)
	}
	if ch.Base != (Base{Name: "ubuntu", Channel: "22.04", Architecture: "arm64"}) || ch.Base.String() != "ubuntu@22.04" {
		t.Errorf("base %+v", ch.Base)
	}
	for file, s := range map[string]string{"metadata": ch.MetadataYAML, "config": ch.ConfigYAML, "actions": ch.ActionsYAML} {
		if len(s) < 100 {
			t.Errorf("%s yaml is %q", file, s)
		}
	}
	if !strings.Contains(ch.MetadataYAML, "postgresql-image") || !strings.HasPrefix(ch.ConfigYAML, "options:") {
		t.Errorf("yaml content: %.80q", ch.MetadataYAML)
	}
	if len(ch.Resources) != 1 || ch.Resources[0].Name != "postgresql-image" || ch.Resources[0].Revision != 208 || ch.Resources[0].Type != "oci-image" {
		t.Errorf("resources %+v", ch.Resources)
	}
	if ch.ReleasedAt.IsZero() || ch.Summary == "" {
		t.Errorf("released %v summary %q", ch.ReleasedAt, ch.Summary)
	}
	if len(f.requests) != 1 {
		t.Fatalf("%d requests", len(f.requests))
	}
	a := f.requests[0]["actions"].([]any)[0].(map[string]any)
	if a["action"] != "install" || a["name"] != "postgresql-k8s" || a["channel"] != "14/stable" {
		t.Errorf("action %v", a)
	}
	if b := a["base"].(map[string]any); b["architecture"] != "arm64" || b["channel"] != "22.04" || b["name"] != "ubuntu" {
		t.Errorf("base %v", b)
	}
	if _, ok := f.requests[0]["context"].([]any); !ok {
		t.Errorf("context must be present: %v", f.requests[0])
	}
}

func TestResolveDiscoversBase(t *testing.T) {
	var calls []map[string]any
	c, _ := newStore(t, func(a map[string]any) (int, []byte) {
		calls = append(calls, a)
		if a["base"].(map[string]any)["name"] == "NA" {
			return 200, fixture(t, "refresh-invalid-base.json")
		}
		return 200, fixture(t, "refresh-postgresql-k8s-arm64.json")
	})
	// The recorded invalid-base answer lists amd64 20.04 only, so an arm64 request finds nothing.
	_, err := c.Resolve(context.Background(), Request{Name: "postgresql-k8s", Base: Base{Architecture: "arm64"}})
	if err == nil || !strings.Contains(err.Error(), "no base") {
		t.Fatalf("err %v", err)
	}
	if !IsPermanent(&Error{Code: CodeInvalidBase}) {
		t.Error("invalid base is permanent")
	}
	calls = nil
	ch, err := c.Resolve(context.Background(), Request{Name: "postgresql-k8s", Base: Base{Architecture: "amd64"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 {
		t.Fatalf("%d calls", len(calls))
	}
	if b := calls[0]["base"].(map[string]any); b["name"] != "NA" || b["channel"] != "NA" || b["architecture"] != "amd64" {
		t.Errorf("first base %v", b)
	}
	if calls[0]["channel"] != "latest/stable" {
		t.Errorf("default channel %v", calls[0]["channel"])
	}
	if b := calls[1]["base"].(map[string]any); b["name"] != "ubuntu" || b["channel"] != "20.04" || b["architecture"] != "amd64" {
		t.Errorf("second base %v", b)
	}
	if ch.Revision != 959 {
		t.Errorf("rev %d", ch.Revision)
	}
}

func TestResolveRevisionPin(t *testing.T) {
	c, f := newStore(t, respond(t, "refresh-postgresql-k8s-arm64.json"))
	rev := 959
	ch, err := c.Resolve(context.Background(), Request{Name: "postgresql-k8s", Channel: "14/stable", Revision: &rev, Base: Base{Architecture: "arm64"}})
	if err != nil {
		t.Fatal(err)
	}
	a := f.requests[0]["actions"].([]any)[0].(map[string]any)
	if _, ok := a["channel"]; ok || a["revision"] != float64(959) {
		t.Errorf("pin must send revision and no channel: %v", a)
	}
	if len(f.requests) != 1 {
		t.Errorf("a revision pin needs no base discovery, got %d requests", len(f.requests))
	}
	if ch.Base != (Base{Name: "ubuntu", Channel: "22.04", Architecture: "arm64"}) {
		t.Errorf("base %+v", ch.Base)
	}
	// A revision for another architecture: the base falls back to the architecture only.
	ch, err = c.Resolve(context.Background(), Request{Name: "postgresql-k8s", Revision: &rev, Base: Base{Architecture: "s390x"}})
	if err != nil || ch.Base != (Base{Architecture: "s390x"}) {
		t.Errorf("%+v %v", ch, err)
	}
}

func TestResolveErrors(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		req      Request
		code     string
		contains string
		releases int
		perm     bool
	}{
		{"revision not found", "refresh-revision-not-found.json", Request{Name: "postgresql-k8s", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}}, CodeRevisionNotFound, "Revision 900", 0, true},
		{"channel not found lists releases", "refresh-channel-not-found.json", Request{Name: "postgresql-k8s", Channel: "nope/stable", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}}, CodeRevisionNotFound, "No revision", 21, true},
		{"name not found", "refresh-name-not-found.json", Request{Name: "nosuchcharmxyz", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}}, CodeNameNotFound, "not found in the Store", 0, true},
		{"missing base", "refresh-missing-base.json", Request{Name: "postgresql-k8s", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}}, CodeMissingBase, "base", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newStore(t, respond(t, tt.fixture))
			_, err := c.Resolve(context.Background(), tt.req)
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("err %v", err)
			}
			if e.Code != tt.code || !strings.Contains(e.Message, tt.contains) || len(e.Releases) != tt.releases {
				t.Errorf("error %+v", e)
			}
			if !Is(err, tt.code) || IsPermanent(err) != tt.perm {
				t.Errorf("Is/IsPermanent wrong for %v", err)
			}
			if !strings.Contains(err.Error(), tt.code) {
				t.Errorf("message %q", err)
			}
		})
	}
	c, _ := newStore(t, respond(t, "refresh-channel-not-found.json"))
	_, err := c.Resolve(context.Background(), Request{Name: "postgresql-k8s", Channel: "x", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}})
	var e *Error
	if !errors.As(err, &e) || e.Releases[0].Channel == "" || e.Releases[0].Base.Name != "ubuntu" {
		t.Errorf("releases %+v", e)
	}
}

func TestResolveTransportAndMalformed(t *testing.T) {
	ctx := context.Background()
	base := Request{Name: "x", Base: Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}}
	tests := []struct {
		name string
		code int
		body string
		want string
		perm bool
	}{
		{"server error", 500, "boom", "HTTP 500", false},
		{"error list on 400", 400, `{"error-list":[{"code":"bad-argument","message":"nope"}]}`, "bad-argument", true},
		{"not found", 404, "", CodeNameNotFound, true},
		{"not json", 200, "<html>", "decoding", false},
		{"error list in 200", 200, `{"error-list":[{"code":"api-error","message":"x"}],"results":[]}`, "api-error", false},
		{"no results", 200, `{"results":[]}`, "0 results", false},
		{"no charm", 200, `{"results":[{"name":"x"}]}`, "no charm", false},
		{"no download", 200, `{"results":[{"charm":{"revision":1}}]}`, "no download", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newStore(t, func(map[string]any) (int, []byte) { return tt.code, []byte(tt.body) })
			_, err := c.Resolve(ctx, base)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err %v, want %q", err, tt.want)
			}
			if IsPermanent(err) != tt.perm {
				t.Errorf("IsPermanent = %v", !tt.perm)
			}
		})
	}
	c := &Client{BaseURL: "http://127.0.0.1:1"}
	if _, err := c.Resolve(ctx, base); err == nil || IsPermanent(err) {
		t.Errorf("unreachable store: %v", err)
	}
	if _, err := c.Resolve(ctx, Request{Base: base.Base}); err == nil {
		t.Error("name required")
	}
	if _, err := c.Resolve(ctx, Request{Name: "x"}); err == nil {
		t.Error("architecture required")
	}
}

func TestInfo(t *testing.T) {
	c, _ := newStore(t, nil)
	info, err := c.Info(context.Background(), "postgresql-k8s")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "postgresql-k8s" || info.ID == "" || len(info.Channels) < 10 {
		t.Fatalf("info %+v", info)
	}
	var found bool
	for _, r := range info.Channels {
		if r.Channel == "14/stable" && r.Base.Architecture == "amd64" && r.Base.Channel == "22.04" {
			found = r.Revision > 0 && r.Track == "14" && r.Risk == "stable" && !r.ReleasedAt.IsZero()
		}
	}
	if !found {
		t.Error("14/stable amd64 22.04 not found")
	}
	if info.Default == nil || info.Default.Revision == 0 {
		t.Errorf("default %+v", info.Default)
	}
	if _, err := c.Info(context.Background(), "nosuch"); !Is(err, CodeNameNotFound) {
		t.Errorf("err %v", err)
	}
	if _, err := c.Info(context.Background(), ""); err == nil {
		t.Error("name required")
	}
}

func TestDownload(t *testing.T) {
	data := []byte("PK charm bytes")
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != "jk-test" {
			t.Errorf("user agent %q", r.Header.Get("User-Agent"))
		}
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/gone", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", 403) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := &Client{BaseURL: srv.URL, HTTPClient: srv.Client(), UserAgent: "jk-test"}
	ctx := context.Background()

	var sb strings.Builder
	n, err := c.Download(ctx, srv.URL+"/ok", strings.ToUpper(want), &sb)
	if err != nil || n != int64(len(data)) || sb.String() != string(data) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, err := c.Download(ctx, srv.URL+"/ok", "", &sb); err != nil {
		t.Errorf("no checksum given: %v", err)
	}
	if _, err := c.Download(ctx, srv.URL+"/ok", strings.Repeat("0", 64), &sb); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("mismatch: %v", err)
	}
	if _, err := c.Download(ctx, srv.URL+"/gone", "", &sb); err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("403: %v", err)
	}
	for _, u := range []string{"http://evil.example.com/x.charm", "file:///etc/passwd", "::bad", "https://notcharmhub.io/x", ""} {
		if _, err := c.Download(ctx, u, "", &sb); err == nil {
			t.Errorf("%q must be refused", u)
		}
	}
	if !c.allowedHost(mustURL(t, "https://api.charmhub.io/api/v1/charms/download/x_1.charm")) || !c.allowedHost(mustURL(t, "https://charmhub.io/x")) {
		t.Error("charmhub hosts must be allowed")
	}
	dead := &Client{BaseURL: "http://127.0.0.1:1"}
	if _, err := dead.Download(ctx, "http://127.0.0.1:1/x", "", &sb); err == nil {
		t.Error("connection error expected")
	}
}

func TestHelpers(t *testing.T) {
	for in, want := range map[string]string{"": "latest/stable", "stable": "latest/stable", "edge": "latest/edge", "14/stable": "14/stable", "14": "14", "latest/beta": "latest/beta"} {
		if got := NormalizeChannel(in); got != want {
			t.Errorf("NormalizeChannel(%q) = %q, want %q", in, got, want)
		}
	}
	for _, tt := range []struct {
		in      string
		want    Base
		wantErr bool
	}{{"", Base{}, false}, {"ubuntu@22.04", Base{Name: "ubuntu", Channel: "22.04"}, false}, {"ubuntu", Base{}, true}, {"@22.04", Base{}, true}, {"ubuntu@", Base{}, true}} {
		got, err := ParseBase(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseBase(%q) = %+v, %v", tt.in, got, err)
		}
	}
	if (Base{Name: "ubuntu"}).String() != "" {
		t.Error("incomplete base must print empty")
	}
	if got, ok := newestBase([]Base{{Name: "ubuntu", Channel: "20.04", Architecture: "amd64"}, {Name: "ubuntu", Channel: "24.04", Architecture: "amd64"}, {Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}, {Name: "ubuntu", Channel: "26.04", Architecture: "arm64"}}, "amd64"); !ok || got.Channel != "24.04" || got.Architecture != "amd64" {
		t.Errorf("newest %+v %v", got, ok)
	}
	if _, ok := newestBase(nil, "amd64"); ok {
		t.Error("no bases")
	}
	for _, tt := range []struct {
		a, b string
		less bool
	}{{"20.04", "22.04", true}, {"22.04", "20.04", false}, {"9.04", "10.04", true}, {"22.04", "22.04", false}, {"22", "22.04", true}, {"a", "b", true}} {
		if versionLess(tt.a, tt.b) != tt.less {
			t.Errorf("versionLess(%q,%q)", tt.a, tt.b)
		}
	}
	if (&Error{Message: "m"}).Error() != "charmhub: m" || IsPermanent(errors.New("x")) || Is(errors.New("x"), "y") {
		t.Error("error helpers")
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestFind(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/charms/find" {
			http.NotFound(w, r)
			return
		}
		gotQuery = r.URL.Query().Get("q")
		_, _ = w.Write([]byte(`{"results":[
			{"name":"traefik-k8s","type":"charm","result":{"summary":"Ingress","publisher":{"display-name":"Canonical"}},"default-release":{"revision":{"version":"2.11"}}},
			{"name":"some-bundle","type":"bundle","result":{"summary":"x","publisher":{"display-name":"Y"}}}]}`))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	got, err := c.Find(context.Background(), "ingress")
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != "ingress" || len(got) != 1 || got[0].Name != "traefik-k8s" || got[0].Publisher != "Canonical" || got[0].Version != "2.11" || got[0].Summary != "Ingress" {
		t.Fatalf("query=%q got=%+v", gotQuery, got)
	}
}
