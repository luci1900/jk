// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/runner/context/action.go and the
// action methods of context.go.

package agent

import (
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/hooktools"
)

// actionRun is the state of the action a hook context runs.
type actionRun struct {
	// obj is the Action object's name, name the action (or juju-exec) and id the task id.
	obj, name string
	id        int64
	params    map[string]any

	mu      sync.Mutex
	results map[string]any
	message string
	failed  bool
}

var _ hooktools.ActionBackend = (*hookContext)(nil)

func (h *hookContext) ActionParams() (map[string]any, error) {
	if h.act == nil {
		return nil, hooktools.ErrNotAction
	}
	if h.act.params == nil {
		return map[string]any{}, nil
	}
	return h.act.params, nil
}

// LogActionMessage appends to the Action's log at once, so that watchers see progress while the action runs.
func (h *hookContext) LogActionMessage(msg string) error {
	if h.act == nil {
		return hooktools.ErrNotAction
	}
	now := metav1.NewTime(h.a.clk.Now())
	return h.a.kube.UpdateActionStatus(h.ctx, h.act.obj, func(a *v1alpha1.Action) bool {
		a.Status.Log = append(a.Status.Log, v1alpha1.ActionLog{Time: now, Message: msg})
		return true
	})
}

func (h *hookContext) SetActionMessage(msg string) error {
	if h.act == nil {
		return hooktools.ErrNotAction
	}
	h.act.mu.Lock()
	defer h.act.mu.Unlock()
	h.act.message = msg
	return nil
}

func (h *hookContext) SetActionFailed() error {
	if h.act == nil {
		return hooktools.ErrNotAction
	}
	h.act.mu.Lock()
	defer h.act.mu.Unlock()
	h.act.failed = true
	return nil
}

func (h *hookContext) UpdateActionResults(keys []string, value string) error {
	if h.act == nil {
		return hooktools.ErrNotAction
	}
	h.act.mu.Lock()
	defer h.act.mu.Unlock()
	if h.act.results == nil {
		h.act.results = map[string]any{}
	}
	addValueToMap(keys, value, h.act.results)
	return nil
}

// addValueToMap sets the value at the nested path keys, merging maps: {foo: {bar: 1}} and {foo: {baz: 2}} give
// {foo: {bar: 1, baz: 2}}, a value in the way of a path is replaced by a map and a path's end overwrites.
func addValueToMap(keys []string, value any, target map[string]any) {
	next := target
	for i, k := range keys {
		if i == len(keys)-1 {
			next[k] = value
			return
		}
		m, ok := next[k].(map[string]any)
		if !ok {
			m = map[string]any{}
			next[k] = m
		}
		next = m
	}
}
