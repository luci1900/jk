package main

import (
	"archive/tar"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// pebbleLayer is the layer that makes Pebble (PID 1 of the charm container) run the agent, as juju's init writes.
const pebbleLayer = `summary: jk unit agent
services:
  container-agent:
    summary: jk unit agent
    override: replace
    command: %[1]s/jk-agent unit --data-dir %[2]s
    kill-delay: 30m0s
    startup: enabled
    on-success: ignore
    on-failure: shutdown
`

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	binDir := fs.String("bin-dir", "/charm/bin", "")
	dataDir := fs.String("data-dir", "/var/lib/juju", "")
	pebbleDir := fs.String("pebble-dir", "/containeragent/pebble", "")
	charmImage := fs.String("charm-image", "", "charm image reference (by digest)")
	insecure := fs.Bool("insecure-registry", true, "plain HTTP (in-cluster jk-registry)")
	fs.Parse(args)

	pod := os.Getenv("JK_POD_NAME")
	_, _, tag, err := unitNames(pod)
	if err != nil {
		return err
	}
	// 1. binaries: agent, hook-tool symlinks are made by `unit`, pebble.
	if err := os.MkdirAll(*binDir, 0o775); err != nil {
		return err
	}
	for _, f := range [][2]string{{"/jk-agent", "jk-agent"}, {"/pebble", "pebble"}} {
		if err := copyFile(f[0], filepath.Join(*binDir, f[1])); err != nil {
			return err
		}
	}
	// 2. Pebble layer for the charm container.
	layers := filepath.Join(*pebbleDir, "layers")
	if err := os.MkdirAll(layers, 0o775); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(layers, "001-container-agent.yaml"), []byte(fmt.Sprintf(pebbleLayer, *binDir, *dataDir)), 0o664); err != nil {
		return err
	}
	// 3. per-container Pebble socket dirs: /charm/containers/<name> (mounted into the workload containers).
	for _, c := range strings.Split(os.Getenv("JUJU_CONTAINER_NAMES"), ",") {
		if c != "" {
			if err := os.MkdirAll(filepath.Join(filepath.Dir(*binDir), "containers", c), 0o775); err != nil {
				return err
			}
		}
	}
	// 4. charm: fetch the single-layer image into <data-dir>/agents/<unit-tag>/charm (writable; ops writes .unit-state.db, dispatch writes venv/bin/python).
	charmDir := filepath.Join(*dataDir, "agents", tag, "charm")
	if _, err := os.Stat(filepath.Join(charmDir, "dispatch")); err == nil {
		fmt.Println("charm already present, skipping fetch")
		return nil
	}
	var opts []name.Option
	if *insecure {
		opts = append(opts, name.Insecure)
	}
	ref, err := name.ParseReference(*charmImage, opts...)
	if err != nil {
		return err
	}
	img, err := remote.Image(ref, remote.WithContext(context.Background()))
	if err != nil {
		return fmt.Errorf("pulling charm %s: %w", ref, err)
	}
	if err := os.MkdirAll(charmDir, 0o775); err != nil {
		return err
	}
	// Extract flattens layers (whiteouts handled); the spike image has a single layer.
	if err := untar(mutate.Extract(img), charmDir); err != nil {
		return err
	}
	fmt.Println("charm fetched to", charmDir)
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o775)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func untar(r io.ReadCloser, dir string) error {
	defer r.Close()
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		p := filepath.Join(dir, filepath.Clean("/"+h.Name))
		switch h.Typeflag {
		case tar.TypeDir:
			err = os.MkdirAll(p, 0o775)
		case tar.TypeSymlink:
			_ = os.MkdirAll(filepath.Dir(p), 0o775)
			_ = os.Remove(p)
			err = os.Symlink(h.Linkname, p)
		case tar.TypeReg:
			_ = os.MkdirAll(filepath.Dir(p), 0o775)
			var f *os.File
			f, err = os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(h.Mode)|0o200)
			if err == nil {
				_, err = io.Copy(f, tr)
				f.Close()
			}
		}
		if err != nil {
			return fmt.Errorf("extracting %s: %w", h.Name, err)
		}
	}
}
