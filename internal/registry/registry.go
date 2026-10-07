// Package registry pushes charms to and reads them from jk-registry as single-layer OCI images.
package registry

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"sigs.k8s.io/yaml"
)

const (
	// Repository is the single repository all charm images live in; they are addressed by digest only.
	Repository = "charms"
	// InClusterEndpoint is the address of jk-registry inside the cluster.
	InClusterEndpoint = "jk-registry.jk-system.svc:5000"
	// Username is the push user of jk-registry.
	Username = "jk"

	maxFileSize = 4 << 20
)

// Client talks to a registry. Pushing needs Username and Password; reading is anonymous.
type Client struct {
	// Endpoint is host:port of the registry.
	Endpoint string
	// Username and Password are used for pushes when set.
	Username, Password string
	// PlainHTTP uses http instead of https (jk-registry speaks plain HTTP).
	PlainHTTP bool
	// HTTPClient overrides the transport, e.g. for tests.
	HTTPClient *http.Client
	// Arch is the image architecture, default runtime.GOARCH.
	Arch string
}

// Base is the charm's base, from manifest.yaml (charmcraft output), e.g. ubuntu 22.04.
type Base struct {
	Name          string   `json:"name"`
	Channel       string   `json:"channel"`
	Architectures []string `json:"architectures,omitempty"`
}

// Charm holds what the operator needs from a charm image. The maps are JSON-compatible.
type Charm struct {
	Digest   string         `json:"digest"`
	Metadata map[string]any `json:"metadata"`
	Config   map[string]any `json:"config,omitempty"`
	Actions  map[string]any `json:"actions,omitempty"`
	// Base is the first base listed in manifest.yaml, falling back to the first of metadata.yaml's
	// "bases" or ubuntu@22.04 (what charm-base provides) when absent.
	Base Base `json:"base"`
}

func (c *Client) ref(digest string) (name.Digest, error) {
	opts := []name.Option{}
	if c.PlainHTTP {
		opts = append(opts, name.Insecure)
	}
	return name.NewDigest(fmt.Sprintf("%s/%s@%s", c.Endpoint, Repository, digest), opts...)
}

func (c *Client) remoteOpts(ctx context.Context, auth bool) []remote.Option {
	opts := []remote.Option{remote.WithContext(ctx)}
	if c.HTTPClient != nil {
		opts = append(opts, remote.WithTransport(c.HTTPClient.Transport))
	}
	if auth && c.Username != "" {
		opts = append(opts, remote.WithAuth(&authn.Basic{Username: c.Username, Password: c.Password}))
	} else {
		opts = append(opts, remote.WithAuth(authn.Anonymous))
	}
	return opts
}

// Push turns the .charm zip into a single-layer linux/<arch> image with the contents at the root and
// pushes it by digest. The same charm file always gives the same digest.
func (c *Client) Push(ctx context.Context, charmPath string) (string, error) {
	f, err := os.Open(charmPath)
	if err != nil {
		return "", fmt.Errorf("opening charm: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}
	return c.PushReader(ctx, f, fi.Size())
}

// PushReader is Push for a charm file that is not on disk (e.g. downloaded from Charmhub): r holds the
// .charm zip of the given size. The image architecture is c.Arch.
func (c *Client) PushReader(ctx context.Context, r io.ReaderAt, size int64) (string, error) {
	img, err := c.image(r, size)
	if err != nil {
		return "", err
	}
	d, err := img.Digest()
	if err != nil {
		return "", err
	}
	ref, err := c.ref(d.String())
	if err != nil {
		return "", err
	}
	if err := remote.Write(ref, img, c.remoteOpts(ctx, true)...); err != nil {
		return "", fmt.Errorf("pushing charm: %w", err)
	}
	return d.String(), nil
}

func (c *Client) image(r io.ReaderAt, size int64) (v1.Image, error) {
	buf, err := zipToTar(r, size)
	if err != nil {
		return nil, err
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf)), nil })
	if err != nil {
		return nil, err
	}
	img, err := mutate.AppendLayers(mutate.MediaType(empty.Image, "application/vnd.oci.image.manifest.v1+json"), layer)
	if err != nil {
		return nil, err
	}
	cf, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	cf = cf.DeepCopy()
	cf.OS, cf.Architecture = "linux", c.Arch
	if cf.Architecture == "" {
		cf.Architecture = runtime.GOARCH
	}
	return mutate.ConfigFile(img, cf)
}

// zipToTar converts the zip to a deterministic tar: sorted, no timestamps, no owners.
func zipToTar(r io.ReaderAt, size int64) ([]byte, error) {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening charm: %w", err)
	}
	files := append([]*zip.File(nil), zr.File...)
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		fi := f.FileInfo()
		h := &tar.Header{Name: path.Clean(f.Name), Mode: int64(fi.Mode().Perm()), Format: tar.FormatPAX}
		switch {
		case fi.IsDir():
			h.Typeflag, h.Name, h.Mode = tar.TypeDir, h.Name+"/", 0o755
		case fi.Mode()&os.ModeSymlink != 0:
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return nil, err
			}
			h.Typeflag, h.Linkname = tar.TypeSymlink, string(b)
		default:
			h.Typeflag, h.Size = tar.TypeReg, int64(f.UncompressedSize64)
		}
		if err := tw.WriteHeader(h); err != nil {
			return nil, err
		}
		if h.Typeflag == tar.TypeReg {
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			_, err = io.Copy(tw, rc)
			rc.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Exists reports whether the registry holds an image with the given digest (a HEAD request).
func (c *Client) Exists(ctx context.Context, digest string) (bool, error) {
	ref, err := c.ref(digest)
	if err != nil {
		return false, err
	}
	_, err = remote.Head(ref, c.remoteOpts(ctx, false)...)
	if err == nil {
		return true, nil
	}
	var terr *transport.Error
	if errors.As(err, &terr) && terr.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, err
}

// Charm reads the charm metadata from the image with the given digest.
func (c *Client) Charm(ctx context.Context, digest string) (*Charm, error) {
	ref, err := c.ref(digest)
	if err != nil {
		return nil, err
	}
	img, err := remote.Image(ref, c.remoteOpts(ctx, false)...)
	if err != nil {
		return nil, fmt.Errorf("fetching charm %s: %w", digest, err)
	}
	layers, err := img.Layers()
	if err != nil {
		return nil, err
	}
	want := map[string][]byte{}
	for _, l := range layers {
		rc, err := l.Uncompressed()
		if err != nil {
			return nil, err
		}
		err = readTop(rc, want)
		rc.Close()
		if err != nil {
			return nil, err
		}
	}
	return parseCharm(digest, want)
}

var wanted = []string{"metadata.yaml", "config.yaml", "actions.yaml", "manifest.yaml"}

func readTop(r io.Reader, out map[string][]byte) error {
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		n := strings.TrimPrefix(path.Clean(h.Name), "./")
		for _, w := range wanted {
			if n == w && h.Typeflag == tar.TypeReg {
				b, err := io.ReadAll(io.LimitReader(tr, maxFileSize))
				if err != nil {
					return err
				}
				out[w] = b
			}
		}
	}
}

func parseCharm(digest string, files map[string][]byte) (*Charm, error) {
	ch := &Charm{Digest: digest, Base: Base{Name: "ubuntu", Channel: "22.04"}}
	parse := func(file string, dst *map[string]any, required bool) error {
		b, ok := files[file]
		if !ok {
			if required {
				return fmt.Errorf("charm has no %s", file)
			}
			return nil
		}
		if err := yaml.Unmarshal(b, dst); err != nil {
			return fmt.Errorf("parsing %s: %w", file, err)
		}
		return nil
	}
	if err := parse("metadata.yaml", &ch.Metadata, true); err != nil {
		return nil, err
	}
	if err := parse("config.yaml", &ch.Config, false); err != nil {
		return nil, err
	}
	if err := parse("actions.yaml", &ch.Actions, false); err != nil {
		return nil, err
	}
	if b, ok := files["manifest.yaml"]; ok {
		var m struct {
			Bases []Base `json:"bases"`
		}
		if err := yaml.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("parsing manifest.yaml: %w", err)
		}
		if len(m.Bases) > 0 {
			ch.Base = m.Bases[0]
		}
	}
	return ch, nil
}
