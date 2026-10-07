// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: relation-ids, relation-list, relation-get,
// relation-set and relation-model-get of internal/worker/uniter/runner/jujuc.

package hooktools

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
)

var relationFlag = map[string]string{"r": "relation"}

// relationID resolves the -r/--relation flag against the hook context: the default is the hook's relation, and the id
// must name a relation the unit is in. It returns -1 when neither is available (callers say "no relation id specified").
func (r *Real) relationID(c cli) (int, error) {
	id := -1
	if hr, ok := r.B.HookRelation(); ok {
		id = hr.ID
	}
	if !c.has("relation") {
		return id, nil
	}
	v := c.str("relation")
	trim := v
	if i := strings.LastIndex(trim, ":"); i != -1 {
		trim = trim[i+1:]
	}
	n, err := strconv.Atoi(trim)
	if err != nil {
		return -1, fmt.Errorf("invalid value %q for flag -r: invalid relation id", v)
	}
	if _, err := r.B.Relation(n); err != nil {
		return -1, fmt.Errorf("invalid value %q for flag -r: %v", v, err)
	}
	return n, nil
}

// relationFakeID is how charms see a relation id: <endpoint>:<id>.
func relationFakeID(ri RelationInfo) string { return fmt.Sprintf("%s:%d", ri.Endpoint, ri.ID) }

func (r *Real) relationIDs(req Request) Response {
	c, bad := parseReq(withFormat(nil, nil), req)
	if bad != nil {
		return *bad
	}
	name := ""
	if hr, ok := r.B.HookRelation(); ok {
		name = hr.Endpoint
	}
	if len(c.pos) > 0 {
		name = c.pos[0]
		if len(c.pos) > 1 {
			return failf("unrecognized args: %q", c.pos[1:])
		}
	} else if name == "" {
		return failf("no endpoint name specified")
	}
	result := []string{}
	for _, id := range r.B.RelationIDs() {
		ri, err := r.B.Relation(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return failf("%v", err)
		}
		if ri.Endpoint == name {
			result = append(result, relationFakeID(ri))
		}
	}
	sort.Strings(result)
	return write(c, "smart", result)
}

func (r *Real) relationList(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"relation": true, "app": false}, relationFlag), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	id, err := r.relationID(c)
	if err != nil {
		return failf("%v", err)
	}
	if id == -1 {
		return failf("no relation id specified")
	}
	ri, err := r.B.Relation(id)
	if err != nil {
		return failf("%v", err)
	}
	if c.boolean("app") {
		return write(c, "smart", ri.RemoteApp)
	}
	units := ri.Units
	if units == nil {
		units = []string{}
	}
	return write(c, "smart", units)
}

func isUnitName(s string) bool {
	app, n, ok := strings.Cut(s, "/")
	if !ok || app == "" || n == "" {
		return false
	}
	_, err := strconv.Atoi(n)
	return err == nil
}

func isAppName(s string) bool { return s != "" && !strings.Contains(s, "/") }

func (r *Real) relationGet(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"relation": true, "app": false}, relationFlag), req)
	if bad != nil {
		return *bad
	}
	id, err := r.relationID(c)
	if err != nil {
		return failf("%v", err)
	}
	if id == -1 {
		return failf("no relation id specified")
	}
	args := c.pos
	key := ""
	if len(args) > 0 {
		if key = args[0]; key == "-" {
			key = ""
		}
		args = args[1:]
	}
	app := c.boolean("app")
	hr, inHook := r.B.HookRelation()

	// Which unit or application: juju's determineUnitOrAppName.
	var name string
	switch {
	case len(args) > 0:
		given := args[0]
		args = args[1:]
		if app {
			switch {
			case isAppName(given):
				name = given
			case isUnitName(given):
				name = given[:strings.Index(given, "/")]
			}
		} else {
			if !isUnitName(given) {
				if isAppName(given) {
					return failf("expected unit name, got application name %q", given)
				}
				return failf("invalid unit name %q", given)
			}
			name = given
		}
	case app:
		if !inHook || hr.RemoteApp == "" {
			return failf("no unit or application specified")
		}
		name = hr.RemoteApp
	case inHook && hr.RemoteUnit != "":
		name = hr.RemoteUnit
	case inHook && hr.RemoteApp != "":
		name, app = hr.RemoteApp, true
	default:
		return failf("no unit or application specified")
	}
	if len(args) > 0 {
		return failf("unrecognized args: %q", args)
	}

	settings, err := r.B.ReadRelationSettings(id, name, app)
	if err != nil {
		return failf("%v", err)
	}
	if settings == nil {
		settings = map[string]string{}
	}
	if key == "" {
		return write(c, "smart", settings)
	}
	if v, ok := settings[key]; ok {
		return write(c, "smart", v)
	}
	return write(c, "smart", nil)
}

func (r *Real) relationSet(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"relation": true, "file": true, "app": false, "format": true}, aliases: relationFlag}, req)
	if bad != nil {
		return *bad
	}
	id, err := r.relationID(c)
	if err != nil {
		return failf("%v", err)
	}
	if id == -1 {
		return failf("no relation id specified")
	}
	overrides, err := parseKeyValues(c.pos)
	if err != nil {
		return failf("%v", err)
	}
	settings := map[string]string{}
	if path := c.str("file"); path != "" {
		b, err := r.readFile(path, req.Stdin)
		if err != nil {
			return failf("%v", err)
		}
		if settings, err = readSettings(b); err != nil {
			return failf("%v", err)
		}
	}
	maps.Copy(settings, overrides)

	var resp Response
	if c.has("format") {
		resp.Stderr = `--format flag deprecated for command "relation-set"`
	}
	if _, err := r.B.Relation(id); err != nil {
		return failf("%v", err)
	}
	app := c.boolean("app")
	if app && !r.B.IsLeader() {
		return failf("cannot write relation settings")
	}
	if err := r.B.SetRelationSettings(id, app, settings); err != nil {
		if app {
			return failf("cannot read relation application settings: %v", err)
		}
		return failf("cannot read relation settings: %v", err)
	}
	return resp
}

func (r *Real) relationModelGet(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"relation": true}, relationFlag), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	id, err := r.relationID(c)
	if err != nil {
		return failf("%v", err)
	}
	if id == -1 {
		return failf("no relation id specified")
	}
	if _, err := r.B.Relation(id); err != nil {
		return failf("%v", err)
	}
	return write(c, "smart", map[string]string{"uuid": r.B.ModelUUID()})
}
