package agent

import (
	"context"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/utils/clock"
)

const (
	// LeaseRenewInterval is how often the leader renews its Lease.
	LeaseRenewInterval = 10 * time.Second
	// LeaderGuarantee is the time that must remain on the Lease for is-leader to be true (juju's guarantee).
	LeaderGuarantee = 30 * time.Second
	// DefaultLeaseDuration is used when the Lease does not say.
	DefaultLeaseDuration = 60 * time.Second
)

// Leadership tracks whether this unit holds the application's Lease, and renews it while it does.
// The operator assigns the holder; the agent only renews. Expiry is measured on the agent's clock from the
// last successful renewal, so clock skew with the API server does not matter.
type Leadership struct {
	kube  Kube
	unit  string
	clock clock.Clock

	mu       sync.Mutex
	holder   string
	duration time.Duration
	expires  time.Time // zero until a renewal succeeded while holding the lease
	reported bool      // is-leader as of the last Renew
}

// NewLeadership returns a tracker for unit (<app>/<n>).
func NewLeadership(kube Kube, unit string, clk clock.Clock) *Leadership {
	return &Leadership{kube: kube, unit: unit, clock: clk, duration: DefaultLeaseDuration}
}

// IsLeader reports whether the unit holds the Lease with at least LeaderGuarantee left.
func (l *Leadership) IsLeader() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isLeaderLocked()
}

func (l *Leadership) isLeaderLocked() bool {
	return l.holder == l.unit && !l.expires.IsZero() && l.expires.Sub(l.clock.Now()) >= LeaderGuarantee
}

// Decided reports whether the Lease names a holder and, if that is this unit, whether its first renewal succeeded.
// A fresh unit waits for this before its first hook so that leader-elected lands in juju's place in the sequence.
func (l *Leadership) Decided() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holder != "" && (l.holder != l.unit || !l.expires.IsZero())
}

// NeedsRenewal reports whether the Lease names this unit but no renewal has succeeded since: the first renewal
// should not wait for the next tick, as a fresh unit holds back its first hook until it has happened.
func (l *Leadership) NeedsRenewal() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holder == l.unit && l.expires.IsZero()
}

// Holder returns the holder as last observed.
func (l *Leadership) Holder() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holder
}

// Observe records a Lease read from the API server (renewals included). A change of holder away from this
// unit ends leadership at once; a Lease that names this unit does not confer it until Renew succeeds.
func (l *Leadership) Observe(lease *coordinationv1.Lease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.observeLocked(lease)
}

func (l *Leadership) observeLocked(lease *coordinationv1.Lease) {
	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	if lease.Spec.LeaseDurationSeconds != nil && *lease.Spec.LeaseDurationSeconds > 0 {
		l.duration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	}
	if holder != l.unit {
		l.expires = time.Time{}
	}
	l.holder = holder
}

// Renew renews the Lease if this unit holds it. It reports whether is-leader differs from what the previous
// call reported (including a lapse between calls), so the caller knows to reconcile.
func (l *Leadership) Renew(ctx context.Context) (changed bool, err error) {
	now := l.clock.Now()
	lease, err := l.kube.RenewLease(ctx, l.unit, now)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil {
		l.observeLocked(lease)
		if l.holder == l.unit {
			l.expires = now.Add(l.duration)
		}
	}
	// On error the last known expiry stands: leadership lapses on its own when the guarantee runs out.
	cur := l.isLeaderLocked()
	changed = cur != l.reported
	l.reported = cur
	return changed, err
}

// Run renews every LeaseRenewInterval until ctx ends. notify is called when leadership may have changed.
func (l *Leadership) Run(ctx context.Context, notify func(), logf func(string, ...any)) {
	t := l.clock.NewTimer(LeaseRenewInterval)
	defer t.Stop()
	for {
		changed, err := l.Renew(ctx)
		if err != nil && ctx.Err() == nil {
			logf("renewing lease: %v", err)
		}
		if changed {
			notify()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			t.Reset(LeaseRenewInterval)
		}
	}
}
