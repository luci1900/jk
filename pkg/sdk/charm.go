package sdk

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
)

// Charm is what the SDK needs to know about a charm to check requests against it.
type Charm struct {
	Name        string
	Summary     string
	Options     map[string]Option
	Provides    map[string]Endpoint
	Requires    map[string]Endpoint
	Peers       map[string]Endpoint
	Resources   map[string]struct{ Type string }
	Storage     map[string]struct{ Type string }
	Actions     map[string]ActionInfo
	hasMetadata bool
}

// Option is one option of config.yaml.
type Option struct {
	Type        string `json:"type"`
	Default     any    `json:"default"`
	Description string `json:"description"`
}

// Endpoint is one relation endpoint of metadata.yaml.
type Endpoint struct {
	Interface string
	Limit     int
	Optional  bool
}

// UnmarshalJSON also accepts the shorthand `endpoint: interface-name`.
func (e *Endpoint) UnmarshalJSON(b []byte) error {
	var iface string
	if json.Unmarshal(b, &iface) == nil {
		*e = Endpoint{Interface: iface}
		return nil
	}
	var p struct {
		Interface string  `json:"interface"`
		Limit     float64 `json:"limit"`
		Optional  bool    `json:"optional"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	*e = Endpoint{Interface: p.Interface, Optional: p.Optional}
	if p.Limit >= 1 && p.Limit == float64(int(p.Limit)) {
		e.Limit = int(p.Limit)
	}
	return nil
}

// ActionInfo describes an action of actions.yaml.
type ActionInfo struct {
	Description string `json:"description"`
}

// ParseCharm reads the charm's files given as JSON or YAML text (config and actions may be empty).
func ParseCharm(metadata, config, actions []byte) (*Charm, error) {
	ch := &Charm{}
	var md struct {
		Name      string              `json:"name"`
		Summary   string              `json:"summary"`
		Provides  map[string]Endpoint `json:"provides"`
		Requires  map[string]Endpoint `json:"requires"`
		Peers     map[string]Endpoint `json:"peers"`
		Resources map[string]struct {
			Type string `json:"type"`
		} `json:"resources"`
		Storage map[string]struct {
			Type string `json:"type"`
		} `json:"storage"`
	}
	if err := yaml.Unmarshal(metadata, &md); err != nil {
		return nil, fmt.Errorf("reading charm metadata: %w", err)
	}
	ch.Name, ch.Summary, ch.Provides, ch.Requires, ch.Peers = md.Name, md.Summary, md.Provides, md.Requires, md.Peers
	ch.hasMetadata = len(metadata) > 0
	ch.Resources = map[string]struct{ Type string }{}
	for k, v := range md.Resources {
		ch.Resources[k] = struct{ Type string }{v.Type}
	}
	ch.Storage = map[string]struct{ Type string }{}
	for k, v := range md.Storage {
		ch.Storage[k] = struct{ Type string }{v.Type}
	}
	var cf struct {
		Options map[string]Option `json:"options"`
	}
	if len(config) > 0 {
		if err := yaml.Unmarshal(config, &cf); err != nil {
			return nil, fmt.Errorf("reading charm config: %w", err)
		}
	}
	ch.Options = cf.Options
	ch.Actions = map[string]ActionInfo{}
	if len(actions) > 0 {
		var raw map[string]ActionInfo
		if err := yaml.Unmarshal(actions, &raw); err != nil {
			return nil, fmt.Errorf("reading charm actions: %w", err)
		}
		for k, v := range raw {
			ch.Actions[k] = v
		}
	}
	return ch, nil
}

func jsonRaw(j *v1alpha1.JSON) []byte {
	if j == nil {
		return nil
	}
	return j.Raw
}

// CharmOf returns the charm the operator resolved for the Application, nil while it is unresolved.
func CharmOf(app *v1alpha1.Application) (*Charm, error) {
	rc := app.Status.Charm
	if rc == nil || rc.Metadata == nil {
		return nil, nil
	}
	return ParseCharm(jsonRaw(rc.Metadata), jsonRaw(rc.ConfigSchema), jsonRaw(rc.Actions))
}

func (c *Charm) endpoint(name string) (Endpoint, string, bool) {
	if e, ok := c.Provides[name]; ok {
		return e, "provides", true
	}
	if e, ok := c.Requires[name]; ok {
		return e, "requires", true
	}
	if e, ok := c.Peers[name]; ok {
		return e, "peers", true
	}
	return Endpoint{}, "", false
}

// ValidateConfig checks values against the charm's options as juju's `deploy --config` and `config` do: options
// must exist and values must parse as the option's type.
func (c *Charm) ValidateConfig(kv map[string]string) error {
	if !c.hasMetadata {
		return nil
	}
	opts := map[string]agent.ConfigOption{}
	for k, o := range c.Options {
		opts[k] = agent.ConfigOption{Type: o.Type, Default: o.Default}
	}
	names := make([]string, 0, len(kv))
	for k := range kv {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if _, ok := c.Options[k]; !ok {
			return fmt.Errorf("unknown option %q", k)
		}
	}
	if _, bad := agent.EffectiveConfig(opts, kv); len(bad) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(bad, "; "))
	}
	return nil
}

// ParseStorage reads `--storage name=size` or `name=pool,size` values (pool is the storage class).
func ParseStorage(values []string) (map[string]v1alpha1.StorageSpec, error) {
	out := map[string]v1alpha1.StorageSpec{}
	for _, v := range values {
		name, rest, ok := strings.Cut(v, "=")
		if !ok || name == "" || rest == "" {
			return nil, fmt.Errorf("invalid storage %q: want name=size or name=pool,size", v)
		}
		var spec v1alpha1.StorageSpec
		for _, part := range strings.Split(rest, ",") {
			part = strings.TrimSpace(part)
			if isSize(part) {
				spec.Size = part
			} else if part != "" {
				class := part
				spec.StorageClass = &class
			}
		}
		out[name] = spec
	}
	return out, nil
}

func isSize(s string) bool {
	s = strings.TrimRight(s, "MGTPmgtpiB")
	_, err := strconv.ParseFloat(s, 64)
	return s != "" && err == nil
}

// ParseConstraints reads juju constraints such as `mem=2G cpu-power=500 arch=arm64`; other keys are rejected.
func ParseConstraints(s string) (*v1alpha1.Constraints, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out v1alpha1.Constraints
	for _, f := range strings.Fields(s) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			return nil, fmt.Errorf("invalid constraint %q: want key=value", f)
		}
		switch k {
		case "mem":
			out.Mem = v
		case "cpu-power":
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid cpu-power %q", v)
			}
			out.CPUPower = &n
		case "arch":
			out.Arch = v
		default:
			return nil, fmt.Errorf("constraint %q is not supported in jk (supported: mem, cpu-power, arch)", k)
		}
	}
	return &out, nil
}

func yamlBytes(s string) ([]byte, error) { return yaml.YAMLToJSON([]byte(s)) }
