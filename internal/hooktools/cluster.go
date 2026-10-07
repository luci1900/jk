// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: network-get and goal-state of
// internal/worker/uniter/runner/jujuc.

package hooktools

import (
	"sort"
	"time"
)

func (r *Real) networkGet(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"primary-address": false, "bind-address": false, "ingress-address": false,
		"egress-subnets": false, "relation": true}, relationFlag), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("no arguments specified")
	}
	binding := c.pos[0]
	if binding == "" {
		return failf("no binding name specified")
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	if names := r.B.BindingNames(); len(names) > 0 {
		known := false
		for _, n := range names {
			if n == binding {
				known = true
			}
		}
		if !known {
			return failf("binding name %q not defined by the unit's charm", binding)
		}
	}
	if c.has("relation") {
		if _, err := r.relationID(c); err != nil {
			return failf("%v", err)
		}
	}
	ni := r.B.NetworkInfo()
	if ni.Address == "" {
		return failf("no network config found for binding %q", binding)
	}
	var keys []string
	if c.boolean("bind-address") {
		keys = append(keys, "bind-address")
	}
	if c.boolean("ingress-address") {
		keys = append(keys, "ingress-address")
	}
	if c.boolean("egress-subnets") {
		keys = append(keys, "egress-subnets")
	}
	if c.boolean("primary-address") {
		if len(keys) > 0 {
			return failf("--primary-address must be the only flag specified")
		}
		return write(c, "smart", ni.Address)
	}
	if len(keys) == 0 {
		return write(c, "smart", map[string]any{
			"bind-addresses": []any{map[string]any{"mac-address": ni.MAC, "interface-name": ni.Interface,
				"addresses": []any{map[string]any{"hostname": "", "value": ni.Address, "cidr": ni.CIDR}}}},
			"egress-subnets":    []string{ni.Address + "/32"},
			"ingress-addresses": []string{ni.Address},
		})
	}
	vals := map[string]any{"bind-address": ni.Address, "ingress-address": ni.Address, "egress-subnets": []string{ni.Address + "/32"}}
	if len(keys) == 1 {
		return write(c, "smart", vals[keys[0]])
	}
	out := map[string]any{}
	for _, k := range keys {
		out[k] = vals[k]
	}
	return write(c, "smart", out)
}

type goalStatusDisplay struct {
	Status string `json:"status"`
	Since  string `json:"since,omitempty"`
}

func goalSince(gs GoalStatus) string {
	t := gs.Since
	if t.IsZero() {
		t = time.Unix(0, 0)
	}
	return t.UTC().Format("2006-01-02 15:04:05Z")
}

func (r *Real) goalState(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"format": true}}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	if f := c.str("format"); f != "" && f != "yaml" && f != "json" {
		return failf("invalid value %q for flag --format: unknown format %q", f, f)
	}
	gs, err := r.B.GoalState()
	if err != nil {
		return failf("%v", err)
	}
	units := map[string]goalStatusDisplay{}
	for name, s := range gs.Units {
		units[name] = goalStatusDisplay{Status: s.Status, Since: goalSince(s)}
	}
	relations := map[string]map[string]goalStatusDisplay{}
	names := make([]string, 0, len(gs.Relations))
	for n := range gs.Relations {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		m := map[string]goalStatusDisplay{}
		for name, s := range gs.Relations[n] {
			m[name] = goalStatusDisplay{Status: s.Status, Since: goalSince(s)}
		}
		relations[n] = m
	}
	return write(c, "yaml", map[string]any{"units": units, "relations": relations})
}
