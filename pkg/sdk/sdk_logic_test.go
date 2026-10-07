package sdk

import (
	"reflect"
	"testing"

	"github.com/luci1900/jk/api/v1alpha1"
)

func TestParseEndpoint(t *testing.T) {
	for in, want := range map[string]EndpointSpec{"app": {"app", ""}, "app:db": {"app", "db"}} {
		got, err := ParseEndpoint(in)
		if err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	for _, bad := range []string{"", ":db", "a:b:c"} {
		if _, err := ParseEndpoint(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestApplicationNames(t *testing.T) {
	for _, ok := range []string{"db", "postgresql-k8s", "a1", "my-app-b", "app2-b"} {
		if err := ValidateApplicationName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "1db", "DB", "db_x", "db-", "db-2", "my-app-2", "-db", "a--b"} {
		if err := ValidateApplicationName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestParseParams(t *testing.T) {
	got, err := ParseParams([]string{"name=x", "n=3", "on=true", "a.b.c=1", "a.b.d=[1, 2]", "empty="})
	noErr(t, err)
	want := map[string]any{
		"name": "x", "n": float64(3), "on": true, "empty": "",
		"a": map[string]any{"b": map[string]any{"c": float64(1), "d": []any{float64(1), float64(2)}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
	if _, err := ParseParams([]string{"nokey"}); err == nil {
		t.Fatal("missing = accepted")
	}
}

func TestParseStorageAndConstraints(t *testing.T) {
	got, err := ParseStorage([]string{"data=10G", "logs=fast,1G", "cache=standard"})
	noErr(t, err)
	if got["data"].Size != "10G" || got["data"].StorageClass != nil || got["logs"].Size != "1G" || *got["logs"].StorageClass != "fast" || *got["cache"].StorageClass != "standard" || got["cache"].Size != "" {
		t.Fatalf("%+v", got)
	}
	if _, err := ParseStorage([]string{"bad"}); err == nil {
		t.Fatal("bad storage accepted")
	}
	cons, err := ParseConstraints("mem=2G cpu-power=500 arch=arm64")
	noErr(t, err)
	if cons.Mem != "2G" || *cons.CPUPower != 500 || cons.Arch != "arm64" {
		t.Fatalf("%+v", cons)
	}
	if cons, err := ParseConstraints(""); cons != nil || err != nil {
		t.Fatal("empty constraints")
	}
	for _, bad := range []string{"tags=x", "mem", "cpu-power=x"} {
		if _, err := ParseConstraints(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestCharmParseAndValidateConfig(t *testing.T) {
	ch, err := ParseCharm([]byte(dbMetadata), []byte(dbConfig), []byte(dbActions))
	noErr(t, err)
	if ch.Name != "db" || ch.Provides["metrics"].Interface != "prometheus_scrape" || ch.Peers["cluster"].Interface != "db_peers" || ch.Actions["get-password"].Description != "Get a password" {
		t.Fatalf("%+v", ch)
	}
	noErr(t, ch.ValidateConfig(map[string]string{"profile": "testing", "connections": "5", "fsync": "false"}))
	wantErr(t, ch.ValidateConfig(map[string]string{"nope": "1"}), `unknown option "nope"`)
	wantErr(t, ch.ValidateConfig(map[string]string{"connections": "many"}), "connections")
	wantErr(t, ch.ValidateConfig(map[string]string{"fsync": "maybe"}), "fsync")
}

func TestEndpointShorthandAndLimit(t *testing.T) {
	ch, err := ParseCharm([]byte(webMetadata), nil, nil)
	noErr(t, err)
	if ch.Requires["db"].Limit != 1 || ch.Requires["scrape"].Interface != "prometheus_scrape" || ch.Requires["scrape"].Limit != 0 {
		t.Fatalf("%+v", ch.Requires)
	}
}

func TestCandidatePairs(t *testing.T) {
	db, _ := ParseCharm([]byte(dbMetadata), nil, nil)
	web, _ := ParseCharm([]byte(webMetadata), nil, nil)
	other, _ := ParseCharm([]byte(otherMetadata), nil, nil)
	a, b := EndpointSpec{App: "db"}, EndpointSpec{App: "web"}
	got := candidatePairs(a, b, db, web)
	// database/pgsql pairs with web:db; metrics/prometheus_scrape pairs with web:scrape.
	if len(got) != 2 {
		t.Fatalf("%v", got)
	}
	// Naming the endpoint of one side narrows it down, on either side and in either order.
	got = candidatePairs(EndpointSpec{App: "db", Endpoint: "metrics"}, b, db, web)
	if len(got) != 1 || got[0][0].String() != "db:metrics" || got[0][1].String() != "web:scrape" {
		t.Fatalf("%v", got)
	}
	got = candidatePairs(EndpointSpec{App: "web", Endpoint: "db"}, EndpointSpec{App: "db"}, web, db)
	if len(got) != 1 || got[0][0].String() != "web:db" || got[0][1].String() != "db:database" {
		t.Fatalf("%v", got)
	}
	if got := candidatePairs(EndpointSpec{App: "web"}, EndpointSpec{App: "other"}, web, other); len(got) != 0 {
		t.Fatalf("two requirers: %v", got)
	}
	if got := candidatePairs(EndpointSpec{App: "db", Endpoint: "cluster"}, b, db, web); len(got) != 0 {
		t.Fatalf("peer endpoint: %v", got)
	}
}

func TestFormatPorts(t *testing.T) {
	got := FormatPorts([]v1alpha1.PortRange{{Protocol: "tcp", From: 8065, To: 8065}, {Protocol: "udp", From: 8000, To: 8010}})
	if !reflect.DeepEqual(got, []string{"8065/TCP", "8000-8010/UDP"}) {
		t.Fatalf("%v", got)
	}
}

func TestImageTag(t *testing.T) {
	for image, want := range map[string]string{
		"ghcr.io/luci1900/jk-operator:v1":            "v1",
		"ghcr.io/luci1900/jk-operator:v1@sha256:abc": "v1",
		"kind.local/jk-operator:719f4b2":             "719f4b2",
		"localhost:32000/jk-operator":                "",
		"localhost:32000/jk-operator@sha256:abc":     "",
		"jk-operator":                                "",
	} {
		if got := imageTag(image); got != want {
			t.Errorf("imageTag(%q) = %q, want %q", image, got, want)
		}
	}
}
