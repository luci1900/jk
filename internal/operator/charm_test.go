package operator

import (
	"reflect"
	"testing"

	"github.com/luci1900/jk/internal/registry"
)

func TestNormalizeDigest(t *testing.T) {
	for in, want := range map[string]string{"abc": "sha256:abc", "sha256:abc": "sha256:abc", "": ""} {
		if got := NormalizeDigest(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestCharmBaseImage(t *testing.T) {
	for _, tt := range []struct {
		b    registry.Base
		want string
	}{
		{registry.Base{Name: "ubuntu", Channel: "22.04"}, "r:ubuntu-22.04"},
		{registry.Base{Name: "ubuntu", Channel: "24.04"}, "r:ubuntu-24.04"},
		{registry.Base{}, "r:ubuntu-22.04"},
	} {
		if got := CharmBaseImage("r", tt.b); got != tt.want {
			t.Errorf("%+v -> %q, want %q", tt.b, got, tt.want)
		}
	}
}

func TestJujuSize(t *testing.T) {
	for in, want := range map[string]string{"100": "100Mi", "512M": "512Mi", "2G": "2Gi", "1T": "1Ti", "3Gi": "3Gi"} {
		q, err := jujuSize(in)
		if err != nil || q.String() != want {
			t.Errorf("%q -> %s (%v), want %s", in, q.String(), err, want)
		}
	}
	for _, bad := range []string{"", "lots"} {
		if _, err := jujuSize(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestResourceImages(t *testing.T) {
	md := mustMD(t, pgMetadata)
	got := ResourceImages(md, map[string]string{"exporter-image": "mine:1", "unrelated": "x"})
	want := map[string]string{"postgresql-image": "ghcr.io/canonical/charmed-postgresql@sha256:11db", "exporter-image": "mine:1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if ResourceImages(mustMD(t, `{"resources":{"r":{"type":"oci-image"}}}`), nil) != nil {
		t.Error("resource without any image should be omitted")
	}
}

func TestResolveCharm(t *testing.T) {
	ch := &registry.Charm{
		Digest:   "sha256:abc",
		Metadata: map[string]any{"name": "x"},
		Config:   map[string]any{"options": map[string]any{}},
		Base:     registry.Base{Name: "ubuntu", Channel: "24.04"},
	}
	rc, err := ResolveCharm(ch, "reg:5000")
	if err != nil {
		t.Fatal(err)
	}
	if rc.Image != "reg:5000/charms@sha256:abc" || rc.Base != "ubuntu@24.04" || rc.Sha256 != "sha256:abc" {
		t.Errorf("%+v", rc)
	}
	if string(rc.Metadata.Raw) != `{"name":"x"}` || rc.Actions != nil || rc.ConfigSchema == nil {
		t.Errorf("metadata/config/actions: %+v", rc)
	}
}
