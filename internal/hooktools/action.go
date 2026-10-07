// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: action-get, action-set, action-fail and action-log of
// internal/worker/uniter/runner/jujuc.

package hooktools

import (
	"errors"
	"regexp"
	"slices"
	"strings"
)

// ErrNotAction is what the action tools report outside an action.
var ErrNotAction = errors.New("not running an action")

// ActionBackend is the action side of a hook context. Outside an action every method returns ErrNotAction.
type ActionBackend interface {
	// ActionParams are the parameters of the running action (never nil inside an action).
	ActionParams() (map[string]any, error)
	// LogActionMessage appends a progress message to the action's log.
	LogActionMessage(message string) error
	// SetActionMessage sets the action's message, and SetActionFailed makes it fail.
	SetActionMessage(message string) error
	SetActionFailed() error
	// UpdateActionResults sets the result at the nested path keys, merging maps and overwriting other values.
	UpdateActionResults(keys []string, value string) error
}

// keyRule is juju's action key rule: lowercase alphanumeric and hyphens, starting and ending alphanumeric.
var keyRule = regexp.MustCompile("^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")

// reservedActionKeys are set by the agent from the process output.
var reservedActionKeys = []string{"stdout", "stdout-encoding", "stderr", "stderr-encoding"}

// recurseMapOnKeys follows keys through nested maps: the value and whether it exists.
func recurseMapOnKeys(keys []string, params map[string]any) (any, bool) {
	answer, ok := params[keys[0]]
	if len(keys) == 1 {
		return answer, ok
	}
	if !ok {
		return nil, false
	}
	switch typed := answer.(type) {
	case map[string]any:
		return recurseMapOnKeys(keys[1:], typed)
	}
	return nil, false
}

func (r *Real) actionGet(req Request) Response {
	c, bad := parseReq(withFormat(nil, nil), req)
	if bad != nil {
		return *bad
	}
	var keys []string
	if len(c.pos) > 0 {
		if len(c.pos) > 1 {
			return failf("unrecognized args: %q", c.pos[1:])
		}
		keys = strings.Split(c.pos[0], ".")
	}
	params, err := r.B.ActionParams()
	if err != nil {
		return failf("%v", err)
	}
	var answer any
	if len(keys) == 0 {
		// An action without parameters prints an empty object, not nothing.
		if params == nil {
			params = map[string]any{}
		}
		answer = params
	} else {
		answer, _ = recurseMapOnKeys(keys, params)
	}
	return write(c, "smart", answer)
}

func (r *Real) actionSet(req Request) Response {
	c, bad := parseReq(flagDef{}, req)
	if bad != nil {
		return *bad
	}
	type pair struct {
		keys  []string
		value string
	}
	var pairs []pair
	for _, arg := range c.pos {
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			return failf("argument %q must be of the form key...=value", arg)
		}
		keys := strings.Split(k, ".")
		for _, key := range keys {
			if !keyRule.MatchString(key) {
				return failf("key %q must start and end with lowercase alphanumeric, and contain only lowercase alphanumeric and hyphens", key)
			}
			if slices.Contains(reservedActionKeys, key) {
				return failf("cannot set reserved action key %q", key)
			}
		}
		pairs = append(pairs, pair{keys, v})
	}
	for _, p := range pairs {
		if err := r.B.UpdateActionResults(p.keys, p.value); err != nil {
			return failf("%v", err)
		}
	}
	return Response{}
}

func (r *Real) actionFail(req Request) Response {
	c, bad := parseReq(flagDef{}, req)
	if bad != nil {
		return *bad
	}
	msg := "action failed without reason given, check action for errors"
	if len(c.pos) > 0 {
		msg = c.pos[0]
		if len(c.pos) > 1 {
			return failf("unrecognized args: %q", c.pos[1:])
		}
	}
	if err := r.B.SetActionMessage(msg); err != nil {
		return failf("%v", err)
	}
	if err := r.B.SetActionFailed(); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func (r *Real) actionLog(req Request) Response {
	c, bad := parseReq(flagDef{}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) == 0 {
		return failf("no message specified")
	}
	if err := r.B.LogActionMessage(strings.Join(c.pos, " ")); err != nil {
		return failf("%v", err)
	}
	return Response{}
}
