package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
	"github.com/luci1900/jk/internal/hooktools"
)

// MaxObjectBytes is the size limit of UnitData and AppData (etcd's request limit is 1.5 MiB).
const MaxObjectBytes = 1536 * 1024

// hookContext is the hooktools.Backend for one hook execution. Writes (state, ports, workload version) are
// buffered and committed only if the hook succeeds; statuses are applied at once, as in juju.
type hookContext struct {
	a   *Agent
	ctx context.Context

	cfg  map[string]any
	keys []string

	mu        sync.Mutex
	state     map[string]string // committed state with pending changes applied
	stateSet  map[string]string
	stateDel  map[string]bool
	portOps   []portOp
	version   *string
	statusSet bool // the hook called status-set

	info   resolver.HookInfo
	execID string
	w      *world
	local  resolver.Local
	// relUnit and relApp are the relation settings changed in this hook (empty value: removed).
	relUnit map[int]map[string]string
	relApp  map[int]map[string]string
	// sec holds the secret changes of this hook.
	sec secretRecorder
	// act is set when the context runs an action.
	act *actionRun
}

type portOp struct {
	open bool
	port hooktools.Port
}

var _ hooktools.Backend = (*hookContext)(nil)

func newHookContext(ctx context.Context, a *Agent, cfg map[string]any, info resolver.HookInfo, execID string, w *world) *hookContext {
	h := &hookContext{a: a, ctx: ctx, cfg: cfg, stateSet: map[string]string{}, stateDel: map[string]bool{},
		info: info, execID: execID, w: w, local: a.local,
		relUnit: map[int]map[string]string{}, relApp: map[int]map[string]string{}}
	h.sec = newSecretRecorder()
	for k := range a.charm.Options {
		h.keys = append(h.keys, k)
	}
	h.state = map[string]string{}
	for k, v := range a.store.Spec().State {
		h.state[k] = v
	}
	return h
}

func (h *hookContext) UnitName() string { return h.a.cfg.Unit }
func (h *hookContext) IsLeader() bool   { return h.a.lead.IsLeader() }
func (h *hookContext) Config() map[string]any {
	return h.cfg
}
func (h *hookContext) ConfigKeys() []string { return h.keys }

func toStatus(s *v1alpha1.WorkloadStatus) (hooktools.Status, bool) {
	if s == nil {
		return hooktools.Status{}, false
	}
	return hooktools.Status{State: s.State, Message: s.Message}, true
}

func (h *hookContext) UnitStatus() (hooktools.Status, bool) {
	return toStatus(h.a.store.Spec().WorkloadStatus)
}

func (h *hookContext) SetUnitStatus(s hooktools.Status) error {
	h.mu.Lock()
	h.statusSet = true
	h.mu.Unlock()
	return h.a.store.SetWorkloadStatus(h.ctx, s.State, s.Message)
}

func (h *hookContext) ApplicationStatus() (hooktools.Status, bool) {
	ad, err := h.a.kube.AppData(h.ctx)
	if err != nil || ad == nil {
		return hooktools.Status{}, false
	}
	return toStatus(ad.Spec.Status)
}

func (h *hookContext) SetApplicationStatus(s hooktools.Status) error {
	now := metav1.NewTime(h.a.clk.Now())
	return h.a.kube.UpdateAppData(h.ctx, func(sp *v1alpha1.AppDataSpec) {
		sp.Status = &v1alpha1.WorkloadStatus{State: s.State, Message: s.Message, Since: &now}
	})
}

func (h *hookContext) SetWorkloadVersion(v string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.version = &v
	return nil
}

func (h *hookContext) State() map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(h.state))
	for k, v := range h.state {
		out[k] = v
	}
	return out
}

func (h *hookContext) SetState(kv map[string]string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, v := range kv {
		h.state[k] = v
		h.stateSet[k] = v
		delete(h.stateDel, k)
	}
	return nil
}

func (h *hookContext) DeleteState(keys ...string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range keys {
		delete(h.state, k)
		delete(h.stateSet, k)
		h.stateDel[k] = true
	}
	return nil
}

func (h *hookContext) OpenedPorts() []hooktools.Port {
	var out []hooktools.Port
	for _, p := range h.a.store.Spec().OpenedPorts {
		out = append(out, hooktools.Port{Protocol: p.Protocol, From: int(p.From), To: int(p.To)})
	}
	return out
}

func (h *hookContext) OpenPort(p hooktools.Port) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.portOps = append(h.portOps, portOp{open: true, port: p})
	return nil
}

func (h *hookContext) ClosePort(p hooktools.Port) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.portOps = append(h.portOps, portOp{port: p})
	return nil
}

func (h *hookContext) Address() string { return h.a.cfg.PodIP() }

func (h *hookContext) Log(level, msg string) {
	h.a.logf("juju-log %s: %s", level, msg)
}

// flush commits the buffered writes in the order of docs/design.md (UnitData and AppData): secrets, then AppData, then UnitData. Every step is
// safe to repeat (a crash midway reruns the hook with the same execution id). It returns the UnitData mutation, which
// the caller saves together with the "operation done" marker. It fails, and the hook with it, if UnitData or AppData
// would exceed the object size limit.
func (h *hookContext) flush(ctx context.Context) (func(*v1alpha1.UnitDataSpec), error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 1. Secrets.
	idx, err := h.commitSecrets(ctx)
	if err != nil {
		return nil, err
	}
	// 2. AppData (leader only).
	if err := h.commitAppData(ctx, idx); err != nil {
		return nil, err
	}
	// 3. UnitData.
	mutate := func(sp *v1alpha1.UnitDataSpec) {
		if len(h.stateSet) > 0 || len(h.stateDel) > 0 {
			st := make(map[string]string, len(sp.State)+len(h.stateSet))
			for k, v := range sp.State {
				st[k] = v
			}
			for k := range h.stateDel {
				delete(st, k)
			}
			for k, v := range h.stateSet {
				st[k] = v
			}
			if len(st) == 0 {
				st = nil
			}
			sp.State = st
		}
		for _, op := range h.portOps {
			sp.OpenedPorts = applyPortOp(sp.OpenedPorts, op)
		}
		if h.version != nil {
			sp.WorkloadVersion = *h.version
		}
		h.applyRelationData(sp)
		h.applyUnitSecrets(sp, idx)
	}
	probe := h.a.store.Spec()
	mutate(&probe)
	if b, err := json.Marshal(probe); err != nil {
		return nil, err
	} else if len(b) > MaxObjectBytes {
		return nil, fmt.Errorf("unit data would be %d bytes, over the limit of %d: reduce state-set or relation data", len(b), MaxObjectBytes)
	}
	return mutate, nil
}

func applyPortOp(ports []v1alpha1.PortRange, op portOp) []v1alpha1.PortRange {
	pr := v1alpha1.PortRange{Protocol: op.port.Protocol, From: int32(op.port.From), To: int32(op.port.To)}
	for i, p := range ports {
		if p == pr {
			if op.open {
				return ports
			}
			return append(ports[:i:i], ports[i+1:]...)
		}
	}
	if op.open {
		return append(ports, pr)
	}
	return ports
}
