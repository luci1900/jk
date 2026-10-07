package sdk

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/version"
)

// EndpointInfo is an endpoint of an application's charm.
type EndpointInfo struct {
	// Role is "provides", "requires" or "peers".
	Role      string `json:"role"`
	Interface string `json:"interface,omitempty"`
}

// ApplicationInfo is what `show-application` prints for one application.
type ApplicationInfo struct {
	Charm        string                  `json:"charm"`
	CharmOrigin  string                  `json:"charm-origin"`
	CharmChannel string                  `json:"charm-channel,omitempty"`
	CharmRev     int                     `json:"charm-rev,omitempty"`
	Base         string                  `json:"base,omitempty"`
	Scale        int                     `json:"scale"`
	Trust        string                  `json:"trust"`
	Constraints  string                  `json:"constraints,omitempty"`
	Config       map[string]string       `json:"config,omitempty"`
	Storage      map[string]StorageInfo  `json:"storage,omitempty"`
	Endpoints    map[string]EndpointInfo `json:"endpoints,omitempty"`
	Status       StatusInfo              `json:"application-status"`
	Address      string                  `json:"address,omitempty"`
	Units        []string                `json:"units,omitempty"`
}

// StorageInfo is the size and class an application's storage was deployed with.
type StorageInfo struct {
	Size  string `json:"size,omitempty"`
	Class string `json:"class,omitempty"`
}

// UnitInfo is what `show-unit` prints for one unit.
type UnitInfo struct {
	Application string `json:"application"`
	Pod         string `json:"pod"`
	UnitStatus
}

// ModelInfo is what `show-model` prints.
type ModelInfo struct {
	Name         string            `json:"name"`
	Version      string            `json:"version"`
	Applications int               `json:"applications"`
	Offers       int               `json:"offers,omitempty"`
	Config       map[string]string `json:"config,omitempty"`
}

// ShowApplication describes an application: its spec, charm endpoints and status.
func (c *Client) ShowApplication(ctx context.Context, name string) (*ApplicationInfo, error) {
	app, err := c.getApplication(ctx, name)
	if err != nil {
		return nil, err
	}
	st, err := c.Status(ctx)
	if err != nil {
		return nil, err
	}
	as := st.Applications[name]
	out := &ApplicationInfo{
		Charm: as.Charm, CharmOrigin: as.CharmOrigin, CharmChannel: as.CharmChannel, CharmRev: as.CharmRev, Base: as.Base,
		Scale: as.Scale, Trust: string(app.Spec.Trust), Constraints: FormatConstraints(app.Spec.Constraints),
		Config: app.Spec.Config, Status: as.Status, Address: as.Address,
	}
	if out.Trust == "" {
		out.Trust = string(v1alpha1.TrustNone)
	}
	for unit := range as.Units {
		out.Units = append(out.Units, unit)
	}
	sortUnits(out.Units)
	for k, s := range app.Spec.Storage {
		if out.Storage == nil {
			out.Storage = map[string]StorageInfo{}
		}
		info := StorageInfo{Size: s.Size}
		if s.StorageClass != nil {
			info.Class = *s.StorageClass
		}
		out.Storage[k] = info
	}
	if ch, _ := CharmOf(app); ch != nil {
		out.Endpoints = map[string]EndpointInfo{}
		for role, eps := range map[string]map[string]Endpoint{"provides": ch.Provides, "requires": ch.Requires, "peers": ch.Peers} {
			for n, e := range eps {
				out.Endpoints[n] = EndpointInfo{Role: role, Interface: e.Interface}
			}
		}
	}
	return out, nil
}

// ShowUnit describes one unit, e.g. "pg/0".
func (c *Client) ShowUnit(ctx context.Context, unit string) (*UnitInfo, error) {
	app, _, ok := splitUnit(unit)
	if !ok {
		return nil, fmt.Errorf("%q is not a valid unit name", unit)
	}
	if _, err := c.getApplication(ctx, app); err != nil {
		return nil, err
	}
	st, err := c.Status(ctx)
	if err != nil {
		return nil, err
	}
	us, ok := st.Applications[app].Units[unit]
	if !ok {
		return nil, fmt.Errorf("unit %q not found", unit)
	}
	return &UnitInfo{Application: app, Pod: podName(unit), UnitStatus: us}, nil
}

// ShowModel describes the model.
func (c *Client) ShowModel(ctx context.Context) (*ModelInfo, error) {
	cfg, err := c.ModelConfig(ctx)
	if err != nil {
		return nil, err
	}
	var apps v1alpha1.ApplicationList
	var offers v1alpha1.OfferList
	if err := c.Kube.List(ctx, &apps, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	if err := c.Kube.List(ctx, &offers, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	return &ModelInfo{Name: c.Namespace, Version: version.Version, Applications: len(apps.Items), Offers: len(offers.Items), Config: cfg}, nil
}

// SetConstraints replaces an application's constraints (nil clears them), as juju's set-constraints does.
func (c *Client) SetConstraints(ctx context.Context, app string, cons *v1alpha1.Constraints) error {
	_, err := c.updateApplication(ctx, app, func(a *v1alpha1.Application) error {
		a.Spec.Constraints = cons
		return nil
	})
	return err
}

// Constraints returns an application's constraints.
func (c *Client) Constraints(ctx context.Context, app string) (*v1alpha1.Constraints, error) {
	a, err := c.getApplication(ctx, app)
	if err != nil {
		return nil, err
	}
	return a.Spec.Constraints, nil
}

// FormatConstraints renders constraints as juju does: "arch=arm64 cpu-power=500 mem=2G".
func FormatConstraints(c *v1alpha1.Constraints) string {
	if c == nil {
		return ""
	}
	var parts []string
	if c.Arch != "" {
		parts = append(parts, "arch="+c.Arch)
	}
	if c.CPUPower != nil {
		parts = append(parts, "cpu-power="+strconv.Itoa(*c.CPUPower))
	}
	if c.Mem != "" {
		parts = append(parts, "mem="+c.Mem)
	}
	return strings.Join(parts, " ")
}

// sortUnits orders unit names by application, then ordinal.
func sortUnits(units []string) {
	sort.Slice(units, func(i, j int) bool {
		ai, ni, _ := splitUnit(units[i])
		aj, nj, _ := splitUnit(units[j])
		if ai != aj {
			return ai < aj
		}
		return ni < nj
	})
}
