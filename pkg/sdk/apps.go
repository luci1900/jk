package sdk

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/pkg/charmhub"
)

// Setting is one config option as `config` shows it.
type Setting struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Default     any    `json:"default,omitempty"`
	Value       any    `json:"value,omitempty"`
	// Source is "default" or "user".
	Source string `json:"source"`
}

// ConfigView is an application's config.
type ConfigView struct {
	Application string             `json:"application"`
	Charm       string             `json:"charm"`
	Settings    map[string]Setting `json:"settings"`
}

// Config returns the application's settings with the charm's defaults filled in.
func (c *Client) Config(ctx context.Context, app string) (*ConfigView, error) {
	a, err := c.getApplication(ctx, app)
	if err != nil {
		return nil, err
	}
	ch, err := CharmOf(a)
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, fmt.Errorf("the charm of %q is not resolved yet", app)
	}
	opts := map[string]agent.ConfigOption{}
	for k, o := range ch.Options {
		opts[k] = agent.ConfigOption{Type: o.Type, Default: o.Default}
	}
	values, _ := agent.EffectiveConfig(opts, a.Spec.Config)
	v := &ConfigView{Application: app, Charm: a.Spec.Charm.Name, Settings: map[string]Setting{}}
	for k, o := range ch.Options {
		s := Setting{Type: o.Type, Description: o.Description, Default: o.Default, Value: values[k], Source: "default"}
		if _, set := a.Spec.Config[k]; set {
			s.Source = "user"
		}
		v.Settings[k] = s
	}
	return v, nil
}

// SetConfig sets options after checking them against the charm.
func (c *Client) SetConfig(ctx context.Context, app string, kv map[string]string) error {
	return c.changeConfig(ctx, app, kv, nil)
}

// ResetConfig removes user settings, returning the options to the charm's defaults.
func (c *Client) ResetConfig(ctx context.Context, app string, keys []string) error {
	return c.changeConfig(ctx, app, nil, keys)
}

func (c *Client) changeConfig(ctx context.Context, app string, set map[string]string, reset []string) error {
	_, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error {
		ch, err := CharmOf(a)
		if err != nil {
			return err
		}
		if ch != nil {
			if err := ch.ValidateConfig(set); err != nil {
				return err
			}
			for _, k := range reset {
				if _, ok := ch.Options[k]; !ok {
					return fmt.Errorf("unknown option %q", k)
				}
			}
		}
		cfg := map[string]string{}
		for k, v := range a.Spec.Config {
			cfg[k] = v
		}
		for k, v := range set {
			cfg[k] = v
		}
		for _, k := range reset {
			delete(cfg, k)
		}
		if len(cfg) == 0 {
			cfg = nil
		}
		a.Spec.Config = cfg
		return nil
	})
	return err
}

// SetTrust sets the application's trust level.
func (c *Client) SetTrust(ctx context.Context, app string, trust v1alpha1.Trust) error {
	switch trust {
	case v1alpha1.TrustNone, v1alpha1.TrustNamespace, v1alpha1.TrustCluster:
	default:
		return fmt.Errorf("invalid trust %q: want none, namespace or cluster", trust)
	}
	_, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error { a.Spec.Trust = trust; return nil })
	return err
}

// Scale sets the number of units.
func (c *Client) Scale(ctx context.Context, app string, n int) error {
	if n < 0 {
		return fmt.Errorf("invalid number of units %d", n)
	}
	_, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error {
		s := int32(n)
		a.Spec.Scale = &s
		return nil
	})
	return err
}

// AddUnits adds n units and returns the new scale.
func (c *Client) AddUnits(ctx context.Context, app string, n int) (int, error) {
	if n < 1 {
		return 0, fmt.Errorf("invalid number of units %d", n)
	}
	return c.addUnits(ctx, app, n)
}

// RemoveUnits removes n units (the highest ordinals, as a StatefulSet does) and returns the new scale.
func (c *Client) RemoveUnits(ctx context.Context, app string, n int) (int, error) {
	if n < 1 {
		return 0, fmt.Errorf("invalid number of units %d", n)
	}
	return c.addUnits(ctx, app, -n)
}

func (c *Client) addUnits(ctx context.Context, app string, delta int) (int, error) {
	scale := 0
	_, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error {
		cur := 1
		if a.Spec.Scale != nil {
			cur = int(*a.Spec.Scale)
		}
		scale = cur + delta
		if scale < 0 {
			return fmt.Errorf("cannot remove %d units: application %q has %d", -delta, app, cur)
		}
		s := int32(scale)
		a.Spec.Scale = &s
		return nil
	})
	return scale, err
}

// RemoveApplication deletes the application: its relations are removed in order and its units run their teardown
// hooks. Volumes are kept unless destroyStorage is set.
func (c *Client) RemoveApplication(ctx context.Context, app string, destroyStorage bool) error {
	a, err := c.getApplication(ctx, app)
	if err != nil {
		return err
	}
	if destroyStorage {
		if err := c.annotateDestroyStorage(ctx, app); err != nil {
			return err
		}
	}
	return c.Kube.Delete(ctx, a)
}

// WaitRemoved waits until the application is gone.
func (c *Client) WaitRemoved(ctx context.Context, app string, timeout time.Duration) error {
	return c.wait(ctx, timeout, fmt.Sprintf("application %q to be removed", app), func(ctx context.Context) (bool, string, error) {
		a, err := c.getApplication(ctx, app)
		if err != nil {
			return true, "", nil
		}
		return false, "removing " + a.Name, nil
	})
}

// RefreshOptions say which charm an application should move to. With none set, it moves to the newest revision of
// its channel.
type RefreshOptions struct {
	Channel  string
	Revision *int
	// Path is a local .charm file.
	Path    string
	Timeout time.Duration
}

// RefreshResult says what the application now runs.
type RefreshResult struct {
	Charm    string
	Revision int
	Channel  string
	Local    bool
}

// String is juju's refresh message.
func (r RefreshResult) String() string {
	if r.Local {
		return fmt.Sprintf("Added local charm %q to the model", r.Charm)
	}
	return fmt.Sprintf("Added charm-hub charm %q, revision %d in channel %s, to the model", r.Charm, r.Revision, r.Channel)
}

// Refresh moves an application to another charm revision and waits until the operator has it in status.
func (c *Client) Refresh(ctx context.Context, app string, o RefreshOptions) (*RefreshResult, error) {
	a, err := c.getApplication(ctx, app)
	if err != nil {
		return nil, err
	}
	var charm v1alpha1.CharmSpec
	switch {
	case o.Path != "":
		if o.Channel != "" || o.Revision != nil {
			return nil, fmt.Errorf("--path cannot be combined with --channel or --revision")
		}
		spec, ch, err := c.pushLocal(ctx, o.Path, a.Spec.Constraints)
		if err != nil {
			return nil, err
		}
		if ch.Name != a.Spec.Charm.Name {
			return nil, fmt.Errorf("cannot refresh %q (charm %q) with charm %q", app, a.Spec.Charm.Name, ch.Name)
		}
		charm = spec
	default:
		if a.Spec.Charm.Source == "local" {
			return nil, fmt.Errorf("%q was deployed from a local charm: refresh it with --path", app)
		}
		charm = a.Spec.Charm
		if o.Channel != "" {
			charm.Channel = o.Channel
			charm.Revision = nil
		}
		if o.Revision != nil {
			charm.Revision = o.Revision
		}
		if o.Channel == "" && o.Revision == nil {
			// Newest revision of the current channel.
			rev, err := c.newestRevision(ctx, a, charm)
			if err != nil {
				return nil, err
			}
			charm.Revision = &rev
		}
	}
	if _, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error {
		a.Spec.Charm = charm
		return nil
	}); err != nil {
		return nil, err
	}
	timeout := o.Timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	var res *RefreshResult
	err = c.wait(ctx, timeout, fmt.Sprintf("the new charm of %q to be resolved", app), func(ctx context.Context) (bool, string, error) {
		cur, err := c.getApplication(ctx, app)
		if err != nil {
			return false, "", err
		}
		if cur.Status.Charm != nil && cur.Status.Charm.Pin == operator.PinKey(cur) {
			channel := cur.Status.Charm.Channel
			if channel == "" { // a revision pin leaves no channel in status
				channel = charmhub.NormalizeChannel(cur.Spec.Charm.Channel)
			}
			res = &RefreshResult{Charm: cur.Spec.Charm.Name, Revision: cur.Status.Charm.Revision, Channel: channel, Local: cur.Spec.Charm.Source == "local"}
			return true, "", nil
		}
		for _, cond := range cur.Status.Conditions {
			if cond.Type == operator.CharmUpToDateCondition && cond.Status == "False" && cond.ObservedGeneration == cur.Generation {
				return false, "", fmt.Errorf("refreshing %q: %s", app, cond.Message)
			}
		}
		return false, conditionMessage(cur), nil
	})
	return res, err
}

// newestRevision asks Charmhub for the newest revision of the application's channel.
func (c *Client) newestRevision(ctx context.Context, a *v1alpha1.Application, charm v1alpha1.CharmSpec) (int, error) {
	arch := ""
	if a.Status.Charm != nil {
		arch = a.Status.Charm.Architecture
	}
	if arch == "" {
		arch = c.architecture(ctx, a.Spec.Constraints)
	}
	if arch == "" || c.Hub == nil {
		return 0, fmt.Errorf("cannot look up the newest revision: pass --revision or --channel")
	}
	ch, err := c.resolveCharmhub(ctx, charm.Name, charm.Channel, nil, charm.Base, arch)
	if err != nil {
		return 0, err
	}
	return ch.Revision, nil
}

// Actions lists the actions of the application's charm with their descriptions.
func (c *Client) Actions(ctx context.Context, app string) (map[string]string, error) {
	a, err := c.getApplication(ctx, app)
	if err != nil {
		return nil, err
	}
	ch, err := CharmOf(a)
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, fmt.Errorf("the charm of %q is not resolved yet", app)
	}
	out := map[string]string{}
	names := make([]string, 0, len(ch.Actions))
	for n := range ch.Actions {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		out[n] = ch.Actions[n].Description
	}
	return out, nil
}
