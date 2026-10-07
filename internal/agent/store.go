package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

// Agent status states, as in juju.
const (
	AgentAllocating = "allocating"
	AgentExecuting  = "executing"
	AgentIdle       = "idle"
	AgentError      = "error"
)

// UnitStore keeps this unit's UnitData spec in memory and writes every change through to the API server.
// The unit's agent is the only writer, so the in-memory copy is authoritative once loaded.
type UnitStore struct {
	kube  Kube
	clock clock.PassiveClock

	mu     sync.Mutex
	spec   v1alpha1.UnitDataSpec
	loaded bool
	exists bool
}

// NewUnitStore returns a store; call Load before use.
func NewUnitStore(kube Kube, clk clock.PassiveClock) *UnitStore {
	return &UnitStore{kube: kube, clock: clk}
}

// Load reads UnitData from the API server. A unit that has none starts from the initial state (install queued);
// the object is created on the first Update.
func (s *UnitStore) Load(ctx context.Context) (resolver.State, error) {
	ud, err := s.kube.UnitData(ctx)
	if err != nil {
		return resolver.State{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loaded = true
	if ud == nil {
		s.spec = v1alpha1.UnitDataSpec{}
		s.exists = false
		return resolver.Initial(), nil
	}
	s.exists = true
	s.spec = *ud.Spec.DeepCopy()
	return s.stateLocked()
}

func (s *UnitStore) stateLocked() (resolver.State, error) {
	if s.spec.Operation == nil || len(s.spec.Operation.Raw) == 0 {
		return resolver.Initial(), nil
	}
	var st resolver.State
	if err := json.Unmarshal(s.spec.Operation.Raw, &st); err != nil {
		return resolver.State{}, fmt.Errorf("decoding saved operation state: %w", err)
	}
	if err := st.Validate(); err != nil {
		return resolver.State{}, err
	}
	return st, nil
}

// Spec returns a copy of the current spec.
func (s *UnitStore) Spec() v1alpha1.UnitDataSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.spec.DeepCopy()
}

// Update applies mutate to a copy of the spec and saves it; the in-memory copy changes only if the save succeeds.
func (s *UnitStore) Update(ctx context.Context, mutate func(*v1alpha1.UnitDataSpec)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.loaded {
		return fmt.Errorf("unit store not loaded")
	}
	next := *s.spec.DeepCopy()
	mutate(&next)
	if err := s.kube.SaveUnitData(ctx, next); err != nil {
		return err
	}
	s.spec, s.exists = next, true
	return nil
}

// SetState records the operation state (with optional further changes).
func (s *UnitStore) SetState(ctx context.Context, st resolver.State, more ...func(*v1alpha1.UnitDataSpec)) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return s.Update(ctx, func(sp *v1alpha1.UnitDataSpec) {
		sp.Operation = &v1alpha1.JSON{Raw: raw}
		for _, f := range more {
			f(sp)
		}
	})
}

func (s *UnitStore) status(state, msg string) *v1alpha1.WorkloadStatus {
	now := metav1.NewTime(s.clock.Now())
	return &v1alpha1.WorkloadStatus{State: state, Message: msg, Since: &now}
}

// withAgentStatus sets the agent status unless it already has that state and message.
func (s *UnitStore) withAgentStatus(state, msg string) func(*v1alpha1.UnitDataSpec) {
	return func(sp *v1alpha1.UnitDataSpec) {
		if a := sp.AgentStatus; a != nil && a.State == state && a.Message == msg {
			return
		}
		sp.AgentStatus = s.status(state, msg)
	}
}

// SetAgentStatus writes the agent status if it changed.
func (s *UnitStore) SetAgentStatus(ctx context.Context, state, msg string) error {
	cur := s.Spec().AgentStatus
	if cur != nil && cur.State == state && cur.Message == msg {
		return nil
	}
	return s.Update(ctx, s.withAgentStatus(state, msg))
}

// SetWorkloadStatus writes the workload status (what status-set does). It is applied at once, not on hook success, as in juju.
func (s *UnitStore) SetWorkloadStatus(ctx context.Context, state, msg string) error {
	return s.Update(ctx, func(sp *v1alpha1.UnitDataSpec) { sp.WorkloadStatus = s.status(state, msg) })
}

// Now is the store's clock reading.
func (s *UnitStore) Now() time.Time { return s.clock.Now() }
