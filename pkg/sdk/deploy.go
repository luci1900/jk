package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
	"github.com/luci1900/jk/pkg/charmhub"
)

// DeployOptions describe an application to deploy.
type DeployOptions struct {
	// Charm is a Charmhub charm name or the path of a .charm file.
	Charm string
	// Name is the application name (default: the charm's name).
	Name string
	// Channel, Revision and Base pin a Charmhub charm; the channel defaults to latest/stable.
	Channel  string
	Revision *int
	Base     string
	Config   map[string]string
	// Scale is the number of units (default 1).
	Scale int
	// Trust is the access granted to the charm: none (default), namespace or cluster.
	Trust       v1alpha1.Trust
	Resources   map[string]string
	Storage     map[string]v1alpha1.StorageSpec
	Constraints *v1alpha1.Constraints
	// Timeout bounds the wait for the operator to resolve the charm (default 2 minutes).
	Timeout time.Duration
}

// DeployResult says what was deployed.
type DeployResult struct {
	Name     string
	Charm    string
	Revision int
	Channel  string
	Base     string
	Local    bool
}

// String is juju's deploy message.
func (r DeployResult) String() string {
	if r.Local {
		return fmt.Sprintf("Deployed %q from local charm %q", r.Name, r.Charm)
	}
	return fmt.Sprintf("Deployed %q from charm-hub charm %q, revision %d in channel %s on %s", r.Name, r.Charm, r.Revision, r.Channel, r.Base)
}

var appNameRE = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]*[a-z][a-z0-9]*)*$`)

// ValidateApplicationName applies juju's rule for application names (which keeps them valid k8s names).
func ValidateApplicationName(name string) error {
	if !appNameRE.MatchString(name) || len(name) > 50 {
		return fmt.Errorf("invalid application name %q: lowercase letters, digits and hyphens, starting with a letter, no part of it all digits", name)
	}
	return nil
}

// IsLocalCharm says whether a deploy argument is a charm file rather than a Charmhub name.
func IsLocalCharm(arg string) bool {
	if strings.HasSuffix(arg, ".charm") || strings.ContainsAny(arg, "/\\") {
		return true
	}
	_, err := os.Stat(arg)
	return err == nil
}

// architecture picks the architecture charms run on: the constraint, else the one architecture of the nodes. It
// returns "" when it cannot tell (e.g. the user may not list nodes).
func (c *Client) architecture(ctx context.Context, cons *v1alpha1.Constraints) string {
	if cons != nil && cons.Arch != "" {
		return cons.Arch
	}
	var nodes corev1.NodeList
	if err := c.Kube.List(ctx, &nodes); err != nil {
		return ""
	}
	arch, err := operator.ChooseArchitecture("", operator.ClusterArchitectures(nodes.Items))
	if err != nil {
		return ""
	}
	return arch
}

// Deploy creates an Application and waits until the operator has resolved its charm.
func (c *Client) Deploy(ctx context.Context, o DeployOptions) (*DeployResult, error) {
	if err := c.requireModel(ctx); err != nil {
		return nil, err
	}
	if o.Charm == "" {
		return nil, fmt.Errorf("no charm specified")
	}
	if o.Scale < 0 {
		return nil, fmt.Errorf("invalid number of units %d", o.Scale)
	}
	if o.Scale == 0 {
		o.Scale = 1
	}
	if o.Trust == "" {
		o.Trust = v1alpha1.TrustNone
	}
	local := IsLocalCharm(o.Charm)
	if local && (o.Channel != "" || o.Revision != nil || o.Base != "") {
		return nil, fmt.Errorf("--channel, --revision and --base apply to Charmhub charms, not to a local charm file")
	}

	spec := v1alpha1.ApplicationSpec{}
	var ch *Charm
	var err error
	channel := ""
	if local {
		spec.Charm, ch, err = c.pushLocal(ctx, o.Charm, o.Constraints)
	} else {
		spec.Charm, ch, channel, err = c.charmhubCharm(ctx, o)
	}
	if err != nil {
		return nil, err
	}
	name := o.Name
	if name == "" {
		name = spec.Charm.Name
	}
	if err := ValidateApplicationName(name); err != nil {
		return nil, err
	}
	if ch != nil {
		if err := ch.validateDeploy(o); err != nil {
			return nil, err
		}
	}
	scale := int32(o.Scale)
	spec.Scale = &scale
	spec.Config, spec.Trust, spec.Resources, spec.Storage, spec.Constraints = o.Config, o.Trust, o.Resources, o.Storage, o.Constraints
	app := &v1alpha1.Application{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace}, Spec: spec}
	if err := c.Kube.Create(ctx, app); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("application %q already exists in model %q", name, c.Namespace)
		}
		return nil, err
	}
	timeout := o.Timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	var got *v1alpha1.Application
	err = c.wait(ctx, timeout, fmt.Sprintf("the charm of %q to be resolved", name), func(ctx context.Context) (bool, string, error) {
		a, err := c.getApplication(ctx, name)
		if err != nil {
			return false, "", err
		}
		got = a
		if a.Status.Charm != nil && a.Status.Charm.Image != "" && a.Status.Charm.Metadata != nil {
			return true, "", nil
		}
		for _, cond := range a.Status.Conditions {
			if cond.Type == operator.CharmUpToDateCondition && cond.Status == metav1.ConditionFalse && cond.Reason == "CharmNotFound" {
				return false, "", fmt.Errorf("%s", cond.Message)
			}
		}
		return false, conditionMessage(a), nil
	})
	if err != nil {
		return nil, err
	}
	rc := got.Status.Charm
	if rc.Channel != "" {
		channel = rc.Channel
	} else if channel == "" {
		channel = charmhub.NormalizeChannel(o.Channel)
	}
	return &DeployResult{Name: name, Charm: spec.Charm.Name, Revision: rc.Revision, Channel: channel, Base: rc.Base, Local: local}, nil
}

func conditionMessage(a *v1alpha1.Application) string {
	for _, c := range a.Status.Conditions {
		if c.Type == operator.ReadyCondition || c.Type == operator.CharmUpToDateCondition {
			if c.Status != metav1.ConditionTrue && c.Message != "" {
				return c.Message
			}
		}
	}
	return ""
}

func (ch *Charm) validateDeploy(o DeployOptions) error {
	if err := ch.ValidateConfig(o.Config); err != nil {
		return err
	}
	for name := range o.Resources {
		if _, ok := ch.Resources[name]; !ok {
			return fmt.Errorf("unknown resource %q", name)
		}
	}
	for name := range o.Storage {
		if _, ok := ch.Storage[name]; !ok {
			return fmt.Errorf("charm %q has no storage %q", ch.Name, name)
		}
	}
	return nil
}

// pushLocal pushes a .charm file to jk-registry and returns the charm spec that refers to it.
func (c *Client) pushLocal(ctx context.Context, path string, cons *v1alpha1.Constraints) (v1alpha1.CharmSpec, *Charm, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return v1alpha1.CharmSpec{}, nil, fmt.Errorf("cannot use charm %q: %w", path, err)
	}
	if fi.IsDir() {
		return v1alpha1.CharmSpec{}, nil, fmt.Errorf("%q is a directory: pack the charm first (charmcraft pack) and deploy the .charm file", path)
	}
	if c.Registry == nil {
		return v1alpha1.CharmSpec{}, nil, fmt.Errorf("no registry connection: cannot deploy a local charm")
	}
	arch := c.architecture(ctx, cons)
	if arch == "" {
		return v1alpha1.CharmSpec{}, nil, fmt.Errorf("cannot tell the cluster's architecture: pass --constraints arch=<arch>")
	}
	reg, closeFn, err := c.Registry(ctx)
	if err != nil {
		return v1alpha1.CharmSpec{}, nil, fmt.Errorf("connecting to jk-registry (is jk installed?): %w", err)
	}
	defer closeFn()
	reg.Arch = arch
	digest, err := reg.Push(ctx, path)
	if err != nil {
		return v1alpha1.CharmSpec{}, nil, err
	}
	rc, err := reg.Charm(ctx, digest)
	if err != nil {
		return v1alpha1.CharmSpec{}, nil, err
	}
	ch, err := charmFromRegistry(rc)
	if err != nil {
		return v1alpha1.CharmSpec{}, nil, err
	}
	return v1alpha1.CharmSpec{Name: ch.Name, Source: "local", Sha256: strings.TrimPrefix(digest, "sha256:")}, ch, nil
}

func charmFromRegistry(rc *registry.Charm) (*Charm, error) {
	enc := func(m map[string]any) []byte {
		if m == nil {
			return nil
		}
		b, _ := json.Marshal(m)
		return b
	}
	return ParseCharm(enc(rc.Metadata), enc(rc.Config), enc(rc.Actions))
}

// charmhubCharm resolves the charm in Charmhub: to validate the request against it, and to pin the revision and base
// in the spec so the deploy is reproducible. It also returns the channel the revision was found in. When the cluster's
// architecture is unknown or Charmhub cannot be asked, nothing is pinned or validated here and the operator resolves
// the charm and reports problems in status.
func (c *Client) charmhubCharm(ctx context.Context, o DeployOptions) (v1alpha1.CharmSpec, *Charm, string, error) {
	spec := v1alpha1.CharmSpec{Name: o.Charm, Source: "charmhub", Channel: o.Channel, Revision: o.Revision, Base: o.Base}
	arch := c.architecture(ctx, o.Constraints)
	if arch == "" || c.Hub == nil {
		return spec, nil, "", nil
	}
	ch, err := c.resolveCharmhub(ctx, o.Charm, o.Channel, o.Revision, o.Base, arch)
	if err != nil {
		return spec, nil, "", err
	}
	parsed, err := ParseCharm([]byte(ch.MetadataYAML), []byte(ch.ConfigYAML), []byte(ch.ActionsYAML))
	if err != nil {
		return spec, nil, "", err
	}
	if spec.Revision == nil {
		rev := ch.Revision
		spec.Revision = &rev
	}
	if spec.Base == "" {
		spec.Base = ch.Base.String()
	}
	channel := ch.Channel
	if channel == "" {
		channel = charmhub.NormalizeChannel(o.Channel)
	}
	return spec, parsed, channel, nil
}

func (c *Client) resolveCharmhub(ctx context.Context, name, channel string, rev *int, base, arch string) (*charmhub.Charm, error) {
	b, err := charmhub.ParseBase(base)
	if err != nil {
		return nil, err
	}
	b.Architecture = arch
	ch, err := c.Hub.Resolve(ctx, charmhub.Request{Name: name, Channel: channel, Revision: rev, Base: b})
	if err != nil {
		return nil, charmhubError(name, channel, rev, err)
	}
	return ch, nil
}

// charmhubError words Charmhub's errors the way juju does.
func charmhubError(name, channel string, rev *int, err error) error {
	switch {
	case charmhub.Is(err, charmhub.CodeNameNotFound):
		return fmt.Errorf("charm %q not found in Charmhub", name)
	case charmhub.Is(err, charmhub.CodeRevisionNotFound):
		if rev != nil {
			return fmt.Errorf("revision %d of charm %q not found in Charmhub", *rev, name)
		}
		return fmt.Errorf("charm %q has no release in channel %q: %w", name, charmhub.NormalizeChannel(channel), err)
	}
	return err
}
