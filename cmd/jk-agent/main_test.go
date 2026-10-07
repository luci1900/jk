package main

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnitNames(t *testing.T) {
	tests := []struct {
		pod, app, unit, tag string
		err                 bool
	}{
		{"postgresql-k8s-0", "postgresql-k8s", "postgresql-k8s/0", "unit-postgresql-k8s-0", false},
		{"app-12", "app", "app/12", "unit-app-12", false},
		{"nodash", "", "", "", true},
		{"", "", "", "", true},
		{"-1", "", "", "", true},
	}
	for _, tt := range tests {
		app, unit, tag, err := unitNames(tt.pod)
		if (err != nil) != tt.err || app != tt.app || unit != tt.unit || tag != tt.tag {
			t.Errorf("unitNames(%q) = %q %q %q %v", tt.pod, app, unit, tag, err)
		}
	}
}

func TestPebbleLayer(t *testing.T) {
	got := strings.TrimSpace(strings.NewReplacer("%[1]s", "/charm/bin", "%[2]s", "/var/lib/juju").Replace(pebbleLayer))
	for _, want := range []string{"command: /charm/bin/jk-agent unit --data-dir /var/lib/juju", "startup: enabled", "on-failure: shutdown"} {
		if !strings.Contains(got, want) {
			t.Errorf("layer lacks %q:\n%s", want, got)
		}
	}
}

func TestUntarAndCopy(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	add := func(h *tar.Header, body string) {
		h.Size = int64(len(body))
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		tw.Write([]byte(body))
	}
	add(&tar.Header{Name: "dir/", Typeflag: tar.TypeDir, Mode: 0o755}, "")
	add(&tar.Header{Name: "dir/dispatch", Typeflag: tar.TypeReg, Mode: 0o755}, "#!/bin/sh\n")
	add(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "dir/dispatch"}, "")
	add(&tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644}, "x") // stays inside the target
	tw.Close()
	dir := t.TempDir()
	if err := untar(io.NopCloser(&buf), dir); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "link")); err != nil || string(b) != "#!/bin/sh\n" {
		t.Fatalf("%q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "escape")); err != nil {
		t.Fatal("path traversal not contained")
	}
	if fi, _ := os.Stat(filepath.Join(dir, "dir", "dispatch")); fi.Mode()&0o100 == 0 {
		t.Fatal("not executable")
	}
	dst := filepath.Join(t.TempDir(), "copy")
	if err := copyFile(filepath.Join(dir, "dir", "dispatch"), dst); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "#!/bin/sh\n" {
		t.Fatal(string(b))
	}
	if err := copyFile(filepath.Join(dir, "missing"), dst); err == nil {
		t.Fatal("copy of a missing file succeeded")
	}
}
