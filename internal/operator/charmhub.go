package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/registry"
	"github.com/luci1900/jk/pkg/charmhub"
)

var (
	// errCharmNotFound marks a Charmhub answer that the charm, channel, revision or base does not exist. It is
	// retried slowly, since releases happen.
	errCharmNotFound = errors.New("charm not found")
	// errMixedArch marks a cluster whose nodes have several architectures while the spec names none.
	errMixedArch = errors.New("mixed architectures")
	// errNoNodes marks a cluster with no nodes to read an architecture from.
	errNoNodes = errors.New("no nodes")
)

// CharmHub is what the operator needs from Charmhub; *charmhub.Client implements it.
type CharmHub interface {
	Resolve(ctx context.Context, r charmhub.Request) (*charmhub.Charm, error)
	Download(ctx context.Context, url, sha256 string, w io.Writer) (int64, error)
}

// CharmStore is the writable side of jk-registry; see NewRegistryStore.
type CharmStore interface {
	// Exists reports whether the registry holds the image digest.
	Exists(ctx context.Context, digest string) (bool, error)
	// PushCharm stores the .charm zip as an image for the architecture and returns its digest.
	PushCharm(ctx context.Context, r io.ReaderAt, size int64, arch string) (string, error)
}

// RegistryStore adapts *registry.Client (which needs push credentials) to CharmStore and CharmSource.
type RegistryStore struct{ Client *registry.Client }

// Charm reads a charm image.
func (s RegistryStore) Charm(ctx context.Context, digest string) (*registry.Charm, error) {
	return s.Client.Charm(ctx, digest)
}

// Exists reports whether the image is in the registry.
func (s RegistryStore) Exists(ctx context.Context, digest string) (bool, error) {
	return s.Client.Exists(ctx, digest)
}

// PushCharm pushes the charm for the given architecture.
func (s RegistryStore) PushCharm(ctx context.Context, r io.ReaderAt, size int64, arch string) (string, error) {
	c := *s.Client
	if arch != "" {
		c.Arch = arch
	}
	return c.PushReader(ctx, r, size)
}

// verifiedTTL is how long a charm image found in the registry is trusted to still be there.
const verifiedTTL = 10 * time.Minute

type verifiedCache struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (v *verifiedCache) fresh(digest string, now time.Time) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return now.Sub(v.at[digest]) < verifiedTTL
}

func (v *verifiedCache) mark(digest string, now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.at == nil {
		v.at = map[string]time.Time{}
	}
	v.at[digest] = now
}

// ClusterArchitectures lists the CPU architectures of the nodes pods can run on. Nodes that are cordoned or have a
// NoSchedule/NoExecute taint are left out unless that leaves none (e.g. a single-node cluster).
func ClusterArchitectures(nodes []corev1.Node) []string {
	collect := func(usable bool) []string {
		set := map[string]bool{}
		for i := range nodes {
			n := &nodes[i]
			if usable && !schedulable(n) {
				continue
			}
			a := n.Status.NodeInfo.Architecture
			if a == "" {
				a = n.Labels[archLabel]
			}
			if a != "" {
				set[a] = true
			}
		}
		out := make([]string, 0, len(set))
		for a := range set {
			out = append(out, a)
		}
		sort.Strings(out)
		return out
	}
	if out := collect(true); len(out) > 0 {
		return out
	}
	return collect(false)
}

func schedulable(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return false
	}
	for _, t := range n.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	return true
}

// ChooseArchitecture picks the architecture charms are resolved for: the arch constraint if set, else the one
// architecture of the cluster's nodes. Mixed clusters need the constraint (docs/design.md, Charm delivery).
func ChooseArchitecture(constraint string, nodeArchs []string) (string, error) {
	if constraint != "" {
		return constraint, nil
	}
	switch len(nodeArchs) {
	case 0:
		return "", fmt.Errorf("%w: cannot tell the cluster's architecture (set spec.constraints.arch)", errNoNodes)
	case 1:
		return nodeArchs[0], nil
	}
	return "", fmt.Errorf("%w: the cluster has nodes of %s; set spec.constraints.arch", errMixedArch, strings.Join(nodeArchs, " and "))
}

func (r *ApplicationReconciler) architecture(ctx context.Context, app *v1alpha1.Application) (string, error) {
	if c := app.Spec.Constraints; c != nil && c.Arch != "" {
		return c.Arch, nil
	}
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes); err != nil {
		return "", err
	}
	arch, err := ChooseArchitecture("", ClusterArchitectures(nodes.Items))
	if err != nil {
		// Keep the cause too: the Application is retried soon, since nodes come and go.
		return "", fmt.Errorf("%w: %w", errInvalid, err)
	}
	return arch, nil
}

// PinKey summarises the spec fields that decide which charm is wanted. status.charm.pin holds the key a status was
// resolved from; the charm is resolved again only when the two differ (a refresh), never on its own.
func PinKey(app *v1alpha1.Application) string {
	c := app.Spec.Charm
	arch := ""
	if app.Spec.Constraints != nil {
		arch = app.Spec.Constraints.Arch
	}
	if c.Source == "local" {
		return "source=local;sha256=" + NormalizeDigest(c.Sha256)
	}
	rev := ""
	if c.Revision != nil {
		rev = strconv.Itoa(*c.Revision)
	}
	return fmt.Sprintf("source=charmhub;channel=%s;revision=%s;base=%s;url=%s;sha256=%s;arch=%s",
		charmhub.NormalizeChannel(c.Channel), rev, c.Base, c.URL, strings.ToLower(strings.TrimPrefix(c.Sha256, "sha256:")), arch)
}

// resolveCharmhub fills status.charm for a Charmhub charm: resolution, download and push to jk-registry. Progress is
// kept in app.Status.Charm even when a later step fails, so a retry does not resolve again. When the pins in spec
// changed after that, it refreshes instead.
func (r *ApplicationReconciler) resolveCharmhub(ctx context.Context, app *v1alpha1.Application) error {
	cur := app.Status.Charm
	pin := PinKey(app)
	if cur != nil && cur.Image != "" && cur.Metadata != nil {
		if cur.Pin == "" {
			cur.Pin = pin // resolved before pins were recorded: whatever spec says now is what it was resolved for
		}
		if cur.Pin != pin {
			return r.refreshCharmhub(ctx, app, pin)
		}
		return r.ensureCached(ctx, app)
	}
	if cur != nil && cur.Pin != "" && cur.Pin != pin {
		cur = nil // the pins changed while the first resolution was still in progress: start over
	}
	if r.Hub == nil || r.Store == nil {
		return fmt.Errorf("%w: this operator cannot install Charmhub charms", errInvalid)
	}
	c := app.Spec.Charm
	if (c.URL == "") != (c.Sha256 == "") {
		return fmt.Errorf("%w: spec.charm.url and spec.charm.sha256 must be set together", errInvalid)
	}
	if cur == nil || cur.URL == "" {
		arch, err := r.architecture(ctx, app)
		if err != nil {
			return err
		}
		rc, err := r.resolveFromHub(ctx, app, arch)
		if err != nil {
			return err
		}
		rc.Pin = pin
		app.Status.Charm = rc
		cur = rc
	}
	return r.completeCharm(ctx, cur)
}

// completeCharm finishes a resolved charm in place: it downloads and pushes the charm when it has no image yet, and
// reads the metadata from the pushed image when the pins gave none.
func (r *ApplicationReconciler) completeCharm(ctx context.Context, cur *v1alpha1.ResolvedCharm) error {
	if cur.Image == "" {
		digest, err := r.fetchAndPush(ctx, cur)
		if err != nil {
			return err
		}
		cur.Image = CharmImageRef(r.Config.RegistryEndpoint, digest)
		r.verified.mark(digest, r.now())
	}
	if cur.Metadata == nil {
		// Pinned by url: the metadata comes from the pushed image.
		digest := cur.Image[strings.LastIndex(cur.Image, "@")+1:]
		ch, err := r.Charms.Charm(ctx, digest)
		if err != nil {
			return fmt.Errorf("%w: reading %s from jk-registry: %v", errCharmUnavailable, digest, err)
		}
		rc, err := ResolveCharm(ch, r.Config.RegistryEndpoint)
		if err != nil {
			return fmt.Errorf("%w: %v", errInvalid, err)
		}
		cur.Metadata, cur.ConfigSchema, cur.Actions = rc.Metadata, rc.ConfigSchema, rc.Actions
		if cur.Base == "" {
			cur.Base = rc.Base
		}
	}
	return nil
}

// refreshCharmhub moves status.charm to the charm the changed pins ask for. The running charm stays in status until
// the new one is downloaded, pushed and complete, so a failure leaves the application as it was; the resolution
// itself is remembered in memory so retries do not resolve again.
func (r *ApplicationReconciler) refreshCharmhub(ctx context.Context, app *v1alpha1.Application, pin string) error {
	if r.Hub == nil || r.Store == nil {
		return fmt.Errorf("%w: this operator cannot install Charmhub charms", errInvalid)
	}
	cur := app.Status.Charm
	c := app.Spec.Charm
	if (c.URL == "") != (c.Sha256 == "") {
		return fmt.Errorf("%w: spec.charm.url and spec.charm.sha256 must be set together", errInvalid)
	}
	next := r.refreshes.get(app.UID, pin)
	if next == nil {
		arch := cur.Architecture
		if app.Spec.Constraints != nil && app.Spec.Constraints.Arch != "" {
			arch = app.Spec.Constraints.Arch
		}
		if arch == "" {
			var err error
			if arch, err = r.architecture(ctx, app); err != nil {
				return err
			}
		}
		rc, err := r.resolveFromHub(ctx, app, arch)
		if err != nil {
			return err
		}
		rc.Pin = pin
		if rc.Sha256 != "" && rc.Sha256 == cur.Sha256 && rc.Architecture == cur.Architecture {
			// The same file under a new pin (e.g. the revision is now pinned to the one the channel gave): keep what
			// is downloaded and only record the new bookkeeping.
			rc.Image, rc.Metadata, rc.ConfigSchema, rc.Actions = cur.Image, cur.Metadata, cur.ConfigSchema, cur.Actions
			if rc.Base == "" {
				rc.Base = cur.Base
			}
		}
		next = rc
		r.refreshes.put(app.UID, next)
	}
	if err := r.completeCharm(ctx, next); err != nil {
		return err
	}
	app.Status.Charm = next
	r.refreshes.drop(app.UID)
	return nil
}

// refreshCache remembers, per Application, the charm a refresh resolved to until it is complete.
type refreshCache struct {
	mu sync.Mutex
	m  map[types.UID]*v1alpha1.ResolvedCharm
}

func (c *refreshCache) get(uid types.UID, pin string) *v1alpha1.ResolvedCharm {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rc := c.m[uid]; rc != nil && rc.Pin == pin {
		return rc
	}
	return nil
}

func (c *refreshCache) put(uid types.UID, rc *v1alpha1.ResolvedCharm) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[types.UID]*v1alpha1.ResolvedCharm{}
	}
	c.m[uid] = rc
}

func (c *refreshCache) drop(uid types.UID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, uid)
}

// resolveFromHub turns the spec's pins into a ResolvedCharm without an image yet.
func (r *ApplicationReconciler) resolveFromHub(ctx context.Context, app *v1alpha1.Application, arch string) (*v1alpha1.ResolvedCharm, error) {
	c := app.Spec.Charm
	base, err := charmhub.ParseBase(c.Base)
	if err != nil {
		return nil, fmt.Errorf("%w: spec.charm.base: %v", errInvalid, err)
	}
	if c.URL != "" {
		// Everything pinned: no store lookup. Metadata is read from the image after the download.
		rc := &v1alpha1.ResolvedCharm{Channel: c.Channel, Base: c.Base, Architecture: arch, URL: c.URL, Sha256: strings.ToLower(strings.TrimPrefix(c.Sha256, "sha256:"))}
		if c.Revision != nil {
			rc.Revision = *c.Revision
		}
		return rc, nil
	}
	base.Architecture = arch
	req := charmhub.Request{Name: c.Name, Channel: c.Channel, Revision: c.Revision, Base: base}
	ch, err := r.Hub.Resolve(ctx, req)
	if err != nil {
		return nil, hubError(c.Name, req, err)
	}
	md, cfg, act, err := parseCharmFiles(ch)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalid, err)
	}
	return &v1alpha1.ResolvedCharm{
		Revision: ch.Revision, Channel: ch.Channel, Base: ch.Base.String(), Architecture: arch,
		URL: ch.DownloadURL, Sha256: ch.SHA256, Metadata: md, ConfigSchema: cfg, Actions: act,
	}, nil
}

func parseCharmFiles(ch *charmhub.Charm) (md, cfg, act *v1alpha1.JSON, err error) {
	conv := func(name, text string, required bool) (*v1alpha1.JSON, error) {
		if strings.TrimSpace(text) == "" {
			if required {
				return nil, fmt.Errorf("Charmhub returned no %s", name)
			}
			return nil, nil
		}
		var m map[string]any
		if err := yaml.Unmarshal([]byte(text), &m); err != nil {
			return nil, fmt.Errorf("parsing %s: %v", name, err)
		}
		return toJSON(m)
	}
	if md, err = conv("metadata.yaml", ch.MetadataYAML, true); err != nil {
		return
	}
	if cfg, err = conv("config.yaml", ch.ConfigYAML, false); err != nil {
		return
	}
	act, err = conv("actions.yaml", ch.ActionsYAML, false)
	return
}

// hubError classifies a Charmhub failure: not-found style answers get a message listing what exists.
func hubError(name string, req charmhub.Request, err error) error {
	if !charmhub.IsPermanent(err) {
		return fmt.Errorf("%w: resolving %s on Charmhub: %v", errCharmUnavailable, name, err)
	}
	msg := fmt.Sprintf("resolving %s (channel %s, %s) on Charmhub: %v", name, charmhub.NormalizeChannel(req.Channel), req.Base.Architecture, err)
	var e *charmhub.Error
	if errors.As(err, &e) && len(e.Releases) > 0 {
		seen := map[string]bool{}
		var avail []string
		for _, rel := range e.Releases {
			s := rel.Channel + " (" + rel.Base.String() + " " + rel.Base.Architecture + ")"
			if !seen[s] {
				seen[s] = true
				avail = append(avail, s)
			}
		}
		msg += "; available: " + strings.Join(avail, ", ")
	}
	return fmt.Errorf("%w: %s", errCharmNotFound, msg)
}

// fetchAndPush downloads the charm file, verifies its sha256 and pushes it to jk-registry.
func (r *ApplicationReconciler) fetchAndPush(ctx context.Context, rc *v1alpha1.ResolvedCharm) (string, error) {
	// A zip needs random access, so spool to a temporary file (a memory buffer would hold hundreds of MiB).
	f, err := os.CreateTemp("", "charm-*.charm")
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	n, err := r.Hub.Download(ctx, rc.URL, rc.Sha256, f)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errCharmUnavailable, err)
	}
	digest, err := r.Store.PushCharm(ctx, f, n, rc.Architecture)
	if err != nil {
		return "", fmt.Errorf("%w: pushing to jk-registry: %v", errCharmUnavailable, err)
	}
	return digest, nil
}

// ensureCached refills jk-registry when the charm image of a resolved Charmhub charm is gone (docs/design.md, Charm delivery),
// checking at most every verifiedTTL. The refilled image must have the digest status already pins.
func (r *ApplicationReconciler) ensureCached(ctx context.Context, app *v1alpha1.Application) error {
	cur := app.Status.Charm
	if r.Store == nil || r.Hub == nil || cur.URL == "" || !strings.Contains(cur.Image, "@") {
		return nil
	}
	digest := cur.Image[strings.LastIndex(cur.Image, "@")+1:]
	if r.verified.fresh(digest, r.now()) {
		return nil
	}
	ok, err := r.Store.Exists(ctx, digest)
	if err != nil {
		return fmt.Errorf("%w: checking jk-registry: %v", errCharmUnavailable, err)
	}
	if !ok {
		got, err := r.fetchAndPush(ctx, cur)
		if err != nil {
			return err
		}
		if got != digest {
			return fmt.Errorf("%w: refilling the charm cache produced %s, but status pins %s", errCharmUnavailable, got, digest)
		}
	}
	r.verified.mark(digest, r.now())
	return nil
}
