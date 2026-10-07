package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
)

// Juju versions reported to charms in JUJU_VERSION. JujuVersion3 is the latest juju 3.6 release (charms gate behaviour
// on it, e.g. postgresql-k8s only removes obsolete secret revisions from 3.6.11); bump it when juju 3.6 does.
const (
	JujuVersion3 = "3.6.29"
	JujuVersion4 = "4.0.0"
)

// ConfigOption is one option of the charm's config.yaml.
type ConfigOption struct {
	Type    string `json:"type"`
	Default any    `json:"default"`
}

// Charm is what the agent needs to know about the charm: its config schema, containers and `assumes`.
type Charm struct {
	Options    map[string]ConfigOption
	Containers []string
	Assumes    any
	// Peers, Provides and Requires map endpoint names to their role; ExtraBindings are the charm's extra bindings.
	Peers         []string
	Provides      []string
	Requires      []string
	ExtraBindings []string
	// Storage maps storage names to their definitions.
	Storage map[string]StorageDef
}

// StorageDef is a storage entry of metadata.yaml.
type StorageDef struct {
	Type     string `json:"type"`
	Location string `json:"location"`
}

// Endpoints lists all endpoint names.
func (c *Charm) Endpoints() []string {
	var out []string
	out = append(out, c.Peers...)
	out = append(out, c.Provides...)
	out = append(out, c.Requires...)
	return out
}

// HasEndpoint reports whether name is a peer, provides or requires endpoint.
func (c *Charm) HasEndpoint(name string) bool {
	for _, e := range c.Endpoints() {
		if e == name {
			return true
		}
	}
	return false
}

// LoadCharm reads metadata.yaml and config.yaml from the charm directory. When a file is missing it falls
// back to the metadata or config schema the operator recorded in the Application status (may be nil).
func LoadCharm(dir string, resolved *v1alpha1.ResolvedCharm) (*Charm, error) {
	c := &Charm{Options: map[string]ConfigOption{}}

	var md struct {
		Containers    map[string]any        `json:"containers"`
		Assumes       any                   `json:"assumes"`
		Peers         map[string]any        `json:"peers"`
		Provides      map[string]any        `json:"provides"`
		Requires      map[string]any        `json:"requires"`
		ExtraBindings map[string]any        `json:"extra-bindings"`
		Storage       map[string]StorageDef `json:"storage"`
	}
	if err := readYAMLOrJSON(filepath.Join(dir, "metadata.yaml"), jsonOf(resolvedMetadata(resolved)), &md); err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	for n := range md.Containers {
		c.Containers = append(c.Containers, n)
	}
	sort.Strings(c.Containers)
	c.Assumes = md.Assumes
	keys := func(m map[string]any) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	c.Peers, c.Provides, c.Requires, c.ExtraBindings = keys(md.Peers), keys(md.Provides), keys(md.Requires), keys(md.ExtraBindings)
	c.Storage = md.Storage

	var cfg struct {
		Options map[string]ConfigOption `json:"options"`
	}
	if err := readYAMLOrJSON(filepath.Join(dir, "config.yaml"), jsonOf(resolvedConfig(resolved)), &cfg); err != nil {
		return nil, fmt.Errorf("config.yaml: %w", err)
	}
	if cfg.Options != nil {
		c.Options = cfg.Options
	}
	return c, nil
}

func resolvedMetadata(r *v1alpha1.ResolvedCharm) *v1alpha1.JSON {
	if r == nil {
		return nil
	}
	return r.Metadata
}

func resolvedConfig(r *v1alpha1.ResolvedCharm) *v1alpha1.JSON {
	if r == nil {
		return nil
	}
	return r.ConfigSchema
}

func jsonOf(j *v1alpha1.JSON) []byte {
	if j == nil {
		return nil
	}
	return j.Raw
}

func readYAMLOrJSON(path string, fallback []byte, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) || len(fallback) == 0 {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		b = fallback
	}
	return yaml.Unmarshal(b, out)
}

// EffectiveConfig is the charm's config as config-get shows it: spec.config over the defaults of config.yaml,
// coerced to the option's type. Options without a default that are not set are omitted. Values that do not
// parse as their option's type are returned in bad and left out.
func EffectiveConfig(opts map[string]ConfigOption, spec map[string]string) (cfg map[string]any, bad []string) {
	cfg = map[string]any{}
	for name, o := range opts {
		if o.Default != nil {
			if v, err := coerceValue(o.Type, o.Default); err == nil {
				cfg[name] = v
			}
		}
	}
	for name, raw := range spec {
		o, ok := opts[name]
		if !ok {
			continue // unknown options are validated when set (CLI/webhook), not here
		}
		v, err := coerceValue(o.Type, raw)
		if err != nil {
			bad = append(bad, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		cfg[name] = v
	}
	sort.Strings(bad)
	return cfg, bad
}

// coerceValue converts a default (from YAML) or a spec string to the Go type juju gives the option.
func coerceValue(typ string, v any) (any, error) {
	if s, ok := v.(string); ok {
		switch typ {
		case "int":
			n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%q is not an int", s)
			}
			return n, nil
		case "float":
			f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err != nil {
				return nil, fmt.Errorf("%q is not a float", s)
			}
			return f, nil
		case "boolean":
			b, err := strconv.ParseBool(strings.TrimSpace(s))
			if err != nil {
				return nil, fmt.Errorf("%q is not a boolean", s)
			}
			return b, nil
		}
		return s, nil
	}
	switch typ {
	case "int":
		switch n := v.(type) {
		case float64:
			return int64(n), nil
		case int64:
			return n, nil
		case int:
			return int64(n), nil
		}
	case "float":
		switch n := v.(type) {
		case float64:
			return n, nil
		case int64:
			return float64(n), nil
		case int:
			return float64(n), nil
		}
	case "boolean":
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case "string", "secret", "":
		return fmt.Sprint(v), nil
	}
	return nil, fmt.Errorf("%v (%T) is not a valid %s", v, v, typ)
}

// ConfigHash is a stable hash of the effective config.
func ConfigHash(cfg map[string]any) string {
	b, _ := json.Marshal(cfg) // map keys are sorted
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// JujuVersion picks JUJU_VERSION: 3.6.x unless the charm's `assumes` excludes it, then 4.0.x.
func JujuVersion(assumes any) string {
	if assumesSatisfied(assumes, JujuVersion3) {
		return JujuVersion3
	}
	return JujuVersion4
}

// assumesSatisfied evaluates a charm's `assumes` (a list whose items are features or any-of/all-of groups)
// against a juju version. Only `juju` constraints are evaluated; other features (k8s-api, ...) are assumed present.
func assumesSatisfied(a any, jujuVersion string) bool {
	switch t := a.(type) {
	case nil:
		return true
	case string:
		return featureSatisfied(t, jujuVersion)
	case []any:
		for _, item := range t {
			if !assumesSatisfied(item, jujuVersion) {
				return false
			}
		}
		return true
	case map[string]any:
		if all, ok := t["all-of"]; ok {
			if !assumesSatisfied(all, jujuVersion) {
				return false
			}
		}
		if anyOf, ok := t["any-of"].([]any); ok {
			for _, item := range anyOf {
				if assumesSatisfied(item, jujuVersion) {
					return true
				}
			}
			return false
		}
		return true
	}
	return true
}

func featureSatisfied(s, jujuVersion string) bool {
	s = strings.TrimSpace(s)
	rest, ok := strings.CutPrefix(s, "juju")
	if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '<' && rest[0] != '>' && rest[0] != '=' && rest[0] != '!') {
		return true
	}
	for _, c := range strings.Split(rest, ",") {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		op := ""
		for _, candidate := range []string{">=", "<=", "==", "!=", ">", "<", "="} {
			if strings.HasPrefix(c, candidate) {
				op = candidate
				break
			}
		}
		if op == "" {
			return true
		}
		cmp := compareVersions(jujuVersion, strings.TrimSpace(strings.TrimPrefix(c, op)))
		var ok bool
		switch op {
		case ">=":
			ok = cmp >= 0
		case "<=":
			ok = cmp <= 0
		case ">":
			ok = cmp > 0
		case "<":
			ok = cmp < 0
		case "==", "=":
			ok = cmp == 0
		case "!=":
			ok = cmp != 0
		}
		if !ok {
			return false
		}
	}
	return true
}

// compareVersions compares dotted numeric versions, missing parts being zero. The bound "4" means 4.0.0.
func compareVersions(a, b string) int {
	pa, pb := versionParts(a), versionParts(b)
	for i := 0; i < 3; i++ {
		switch {
		case pa[i] < pb[i]:
			return -1
		case pa[i] > pb[i]:
			return 1
		}
	}
	return 0
}

func versionParts(v string) [3]int {
	var out [3]int
	for i, p := range strings.SplitN(v, ".", 3) {
		n, _ := strconv.Atoi(digits(p))
		out[i] = n
	}
	return out
}

func digits(s string) string {
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	return s[:end]
}
