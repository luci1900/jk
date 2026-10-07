// Command push turns a .charm (zip) into a single-layer OCI image and pushes it to a registry (layout spike).
// Usage: push <file.charm> <registry/repo:tag>
package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

func main() {
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(charm, target string) error {
	zr, err := zip.OpenReader(charm)
	if err != nil {
		return err
	}
	defer zr.Close()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range zr.File {
		fi := f.FileInfo()
		h := &tar.Header{Name: f.Name, Mode: int64(fi.Mode().Perm()), ModTime: f.Modified}
		switch {
		case fi.IsDir():
			h.Typeflag = tar.TypeDir
			h.Mode = 0o755
		case fi.Mode()&os.ModeSymlink != 0:
			rc, _ := f.Open()
			b, _ := io.ReadAll(rc)
			rc.Close()
			h.Typeflag, h.Linkname = tar.TypeSymlink, string(b)
		default:
			h.Typeflag, h.Size = tar.TypeReg, int64(f.UncompressedSize64)
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if h.Typeflag == tar.TypeReg {
			rc, _ := f.Open()
			_, err = io.Copy(tw, rc)
			rc.Close()
			if err != nil {
				return err
			}
		}
	}
	tw.Close()
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf.Bytes())), nil })
	if err != nil {
		return err
	}
	img, err := mutate.AppendLayers(mutate.MediaType(empty.Image, "application/vnd.oci.image.manifest.v1+json"), layer)
	if err != nil {
		return err
	}
	cf, _ := img.ConfigFile()
	cf.OS, cf.Architecture = "linux", "arm64"
	if img, err = mutate.ConfigFile(img, cf); err != nil {
		return err
	}
	ref, err := name.ParseReference(target, name.Insecure)
	if err != nil {
		return err
	}
	if err := remote.Write(ref, img); err != nil {
		return err
	}
	d, _ := img.Digest()
	fmt.Println(d)
	return nil
}
