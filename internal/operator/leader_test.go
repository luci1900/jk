package operator

import (
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func spec(holder string, renewedAgo time.Duration) *coordinationv1.LeaseSpec {
	d := int32(LeaseDurationSeconds)
	s := &coordinationv1.LeaseSpec{LeaseDurationSeconds: &d}
	if holder != "" {
		s.HolderIdentity = &holder
		mt := metav1.NewMicroTime(t0.Add(-renewedAgo))
		s.RenewTime = &mt
	}
	return s
}

func TestPodOrdinal(t *testing.T) {
	for _, tt := range []struct {
		app, name string
		n         int
		ok        bool
	}{
		{"pg", "pg-0", 0, true}, {"pg", "pg-12", 12, true}, {"pg", "pg-", 0, false}, {"pg", "pg-x", 0, false},
		{"pg", "pg-01", 0, false}, {"pg", "pg-1-0", 0, false}, {"pg", "other-1", 0, false}, {"pg", "pg", 0, false},
		{"pg-1", "pg-1-0", 0, true}, {"pg", "pg--1", 0, false},
	} {
		n, ok := PodOrdinal(tt.app, tt.name)
		if n != tt.n || ok != tt.ok {
			t.Errorf("%s %s = %d,%v want %d,%v", tt.app, tt.name, n, ok, tt.n, tt.ok)
		}
	}
}

func TestCurrentLeaderAndExpiry(t *testing.T) {
	if CurrentLeader(nil, t0) != "" || !LeaseExpiry(nil).IsZero() {
		t.Error("nil lease")
	}
	if CurrentLeader(spec("", 0), t0) != "" {
		t.Error("empty holder")
	}
	if got := CurrentLeader(spec("pg/0", 59*time.Second), t0); got != "pg/0" {
		t.Errorf("fresh = %q", got)
	}
	if got := CurrentLeader(spec("pg/0", 61*time.Second), t0); got != "" {
		t.Errorf("expired = %q", got)
	}
	// Acquire time stands in for a missing renew time; no times at all means no valid claim.
	s := spec("pg/0", 0)
	at := metav1.NewMicroTime(t0)
	s.RenewTime, s.AcquireTime = nil, &at
	if CurrentLeader(s, t0.Add(time.Second)) != "pg/0" {
		t.Error("acquire time ignored")
	}
	s.AcquireTime = nil
	if CurrentLeader(s, t0) != "" {
		t.Error("no times")
	}
	// A missing duration defaults to 60s.
	s = spec("pg/0", 30*time.Second)
	s.LeaseDurationSeconds = nil
	if CurrentLeader(s, t0) != "pg/0" {
		t.Error("default duration")
	}
}

func TestNextLeader(t *testing.T) {
	tests := []struct {
		name   string
		scale  int32
		lease  *coordinationv1.LeaseSpec
		ready  []int
		want   string
		change bool
	}{
		{"no lease, lowest ready", 3, nil, []int{1, 2}, "pg/1", true},
		{"empty holder", 3, spec("", 0), []int{0, 1}, "pg/0", true},
		{"expired holder moves", 3, spec("pg/0", 2*time.Minute), []int{1, 2}, "pg/1", true},
		{"expired holder, itself ready, renewed", 3, spec("pg/0", 2*time.Minute), []int{0, 1}, "pg/0", true},
		{"valid holder kept even if a lower unit is ready", 3, spec("pg/2", 5*time.Second), []int{0, 2}, "pg/2", false},
		{"valid holder not ready is kept", 3, spec("pg/2", 5*time.Second), []int{0}, "pg/2", false},
		{"holder scaled away", 2, spec("pg/2", 5*time.Second), []int{0, 1}, "pg/0", true},
		{"holder of another app", 2, spec("other/0", 5*time.Second), []int{1}, "pg/1", true},
		{"garbage holder", 2, spec("nonsense", 5*time.Second), []int{0}, "pg/0", true},
		{"nobody ready", 2, nil, nil, "", false},
		{"expired and nobody ready", 2, spec("pg/0", 2*time.Minute), nil, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, change := NextLeader("pg", tt.scale, tt.lease, tt.ready, t0)
			if change != tt.change || (change && got != tt.want) || (!change && tt.want != "" && got != tt.want) {
				t.Errorf("got %q,%v want %q,%v", got, change, tt.want, tt.change)
			}
		})
	}
}

func TestEligibleOrdinals(t *testing.T) {
	pod := func(name, container string, running, terminating bool) corev1.Pod {
		p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}
		st := corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}
		if running {
			st = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "other", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}, {Name: container, State: st}}
		if terminating {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		}
		return p
	}
	pods := []corev1.Pod{
		pod("pg-2", "charm", true, false), pod("pg-0", "charm", true, false), pod("pg-1", "charm", false, false),
		pod("pg-3", "charm", true, false), pod("pg-4", "charm", true, true), pod("x-0", "charm", true, false),
		pod("pg-6", "workload", true, false), {ObjectMeta: metav1.ObjectMeta{Name: "pg-5"}},
	}
	got := EligibleOrdinals("pg", 5, pods)
	if len(got) != 3 || got[0] != 0 || got[1] != 2 || got[2] != 3 {
		t.Errorf("got %v, want [0 2 3]", got)
	}
}

func TestNewLeaseSpec(t *testing.T) {
	s := NewLeaseSpec(nil, "pg/0", t0)
	if *s.HolderIdentity != "pg/0" || *s.LeaseTransitions != 0 || *s.LeaseDurationSeconds != 60 || !s.RenewTime.Time.Equal(t0) || !s.AcquireTime.Time.Equal(t0) {
		t.Errorf("%+v", s)
	}
	old := spec("pg/0", time.Minute)
	n := int32(3)
	old.LeaseTransitions = &n
	s2 := NewLeaseSpec(old, "pg/1", t0)
	if *s2.LeaseTransitions != 4 || *s2.HolderIdentity != "pg/1" {
		t.Errorf("%+v", s2)
	}
	if *old.HolderIdentity != "pg/0" {
		t.Error("input mutated")
	}
	if UnitName("pg", 3) != "pg/3" || LeaseName("pg") != "pg-leader" {
		t.Error("names")
	}
}
