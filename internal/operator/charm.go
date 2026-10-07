// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/cloudconfig/podcfg/image.go (the charm-base
// image names).

package operator

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/registry"
)

// charmMetadata is the subset of a charm's metadata.yaml the operator needs to lay out pods.
type charmMetadata struct {
	Containers map[string]struct {
		Resource string `json:"resource"`
		Mounts   []struct {
			Storage  string `json:"storage"`
			Location string `json:"location"`
		} `json:"mounts"`
	} `json:"containers"`
	Resources map[string]struct {
		Type           string `json:"type"`
		UpstreamSource string `json:"upstream-source"`
	} `json:"resources"`
	Peers    map[string]relationEndpoint `json:"peers"`
	Provides map[string]relationEndpoint `json:"provides"`
	Requires map[string]relationEndpoint `json:"requires"`
	Storage  map[string]struct {
		Type        string `json:"type"`
		Location    string `json:"location"`
		MinimumSize string `json:"minimum-size"`
	} `json:"storage"`
}

// relationEndpoint is one provides, requires or peers entry of metadata.yaml.
type relationEndpoint struct {
	Interface string `json:"interface"`
	// Limit is the maximum number of relations on the endpoint; zero means unlimited.
	Limit    limitValue `json:"limit"`
	Optional bool       `json:"optional"`
}

// UnmarshalJSON also accepts the shorthand `endpoint: interface-name`.
func (e *relationEndpoint) UnmarshalJSON(b []byte) error {
	var iface string
	if json.Unmarshal(b, &iface) == nil {
		*e = relationEndpoint{Interface: iface}
		return nil
	}
	type plain relationEndpoint
	var p plain
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	*e = relationEndpoint(p)
	return nil
}

// limitValue reads a relation limit, ignoring values that are not positive integers (juju reads them as unlimited).
type limitValue int

func (l *limitValue) UnmarshalJSON(b []byte) error {
	var n float64
	if err := json.Unmarshal(b, &n); err == nil && n >= 1 && n == float64(int(n)) {
		*l = limitValue(n)
	}
	return nil
}

func parseMetadata(j *v1alpha1.JSON) (*charmMetadata, error) {
	if j == nil {
		return nil, fmt.Errorf("%w: charm has no metadata", errInvalid)
	}
	var m charmMetadata
	if err := json.Unmarshal(j.Raw, &m); err != nil {
		return nil, fmt.Errorf("%w: parsing charm metadata: %v", errInvalid, err)
	}
	return &m, nil
}

func toJSON(v map[string]any) (*v1alpha1.JSON, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &v1alpha1.JSON{Raw: b}, nil
}

// NormalizeDigest accepts "sha256:<hex>" or bare hex.
func NormalizeDigest(s string) string {
	if s != "" && !strings.Contains(s, ":") {
		return "sha256:" + s
	}
	return s
}

// CharmImageRef is the digest-pinned reference of a charm in jk-registry, as the init container pulls it.
func CharmImageRef(endpoint, digest string) string {
	return fmt.Sprintf("%s/%s@%s", endpoint, registry.Repository, digest)
}

// CharmBaseImage maps a charm base (ubuntu@22.04) to juju's charm-base image.
func CharmBaseImage(repo string, b registry.Base) string {
	n, c := b.Name, b.Channel
	if n == "" {
		n = "ubuntu"
	}
	if c == "" {
		c = "22.04"
	}
	return fmt.Sprintf("%s:%s-%s", repo, n, c)
}

// ResolveCharm turns what the registry knows into status.charm. Resource images are filled by ResourceImages.
func ResolveCharm(ch *registry.Charm, endpoint string) (*v1alpha1.ResolvedCharm, error) {
	md, err := toJSON(ch.Metadata)
	if err != nil {
		return nil, err
	}
	cfg, err := toJSON(ch.Config)
	if err != nil {
		return nil, err
	}
	act, err := toJSON(ch.Actions)
	if err != nil {
		return nil, err
	}
	return &v1alpha1.ResolvedCharm{
		Base:         ch.Base.Name + "@" + ch.Base.Channel,
		Sha256:       ch.Digest,
		Image:        CharmImageRef(endpoint, ch.Digest),
		Metadata:     md,
		ConfigSchema: cfg,
		Actions:      act,
	}, nil
}

// ResourceImages maps each oci-image resource of the charm to the image pods use: spec.resources over the
// charm's upstream-source. Resources with neither are left out (Build reports them if a container needs one).
func ResourceImages(md *charmMetadata, specResources map[string]string) map[string]string {
	out := map[string]string{}
	for name, r := range md.Resources {
		if r.Type != "" && r.Type != "oci-image" {
			continue
		}
		if img := specResources[name]; img != "" {
			out[name] = img
		} else if r.UpstreamSource != "" {
			out[name] = r.UpstreamSource
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// jujuSize parses juju sizes (plain number = MiB; M, G, T, P suffixes are binary) into a quantity.
func jujuSize(s string) (resource.Quantity, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return resource.Quantity{}, fmt.Errorf("empty size")
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		s += "Mi"
	} else if n := len(s); n > 1 && strings.ContainsRune("MGTP", rune(s[n-1])) {
		s += "i"
	}
	return resource.ParseQuantity(s)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
