package operator

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LeaseDurationSeconds is the leader Lease's duration; the leader's agent renews every 10s.
const LeaseDurationSeconds = 60

// LeaseName is the name of an application's leader Lease.
func LeaseName(app string) string { return app + "-leader" }

// UnitName is the juju unit name for a pod ordinal: app/n.
func UnitName(app string, ordinal int) string { return fmt.Sprintf("%s/%d", app, ordinal) }

// PodOrdinal returns n for a pod or UnitData name "<app>-<n>".
func PodOrdinal(app, name string) (int, bool) {
	rest, ok := cutPrefix(name, app+"-")
	if !ok || rest == "" || (len(rest) > 1 && rest[0] == '0') {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 0 || strconv.Itoa(n) != rest {
		return 0, false
	}
	return n, true
}

func cutPrefix(s, p string) (string, bool) {
	if len(s) < len(p) || s[:len(p)] != p {
		return "", false
	}
	return s[len(p):], true
}

var unitRe = regexp.MustCompile(`^(.+)/(\d+)$`)

// holderOrdinal parses a Lease holder "app/n".
func holderOrdinal(app, holder string) (int, bool) {
	m := unitRe.FindStringSubmatch(holder)
	if m == nil || m[1] != app {
		return 0, false
	}
	n, err := strconv.Atoi(m[2])
	return n, err == nil
}

// LeaseExpiry returns when the holder's claim lapses; the zero time if there is no holder or no renewal yet.
func LeaseExpiry(l *coordinationv1.LeaseSpec) time.Time {
	if l == nil || l.HolderIdentity == nil || *l.HolderIdentity == "" {
		return time.Time{}
	}
	t := time.Time{}
	switch {
	case l.RenewTime != nil:
		t = l.RenewTime.Time
	case l.AcquireTime != nil:
		t = l.AcquireTime.Time
	default:
		return time.Time{}
	}
	d := int32(LeaseDurationSeconds)
	if l.LeaseDurationSeconds != nil {
		d = *l.LeaseDurationSeconds
	}
	return t.Add(time.Duration(d) * time.Second)
}

// CurrentLeader returns the holder of a Lease that has not expired, or "".
func CurrentLeader(l *coordinationv1.LeaseSpec, now time.Time) string {
	if exp := LeaseExpiry(l); !exp.IsZero() && now.Before(exp) {
		return *l.HolderIdentity
	}
	return ""
}

// EligibleOrdinals returns the sorted ordinals of pods of app below scale that may hold the Lease: not
// terminating, with the charm container running. Readiness is not required, so the leader exists before the
// unit's first hook (juju runs leader-elected right after install).
func EligibleOrdinals(app string, scale int32, pods []corev1.Pod) []int {
	var out []int
	for i := range pods {
		p := &pods[i]
		n, ok := PodOrdinal(app, p.Name)
		if !ok || int32(n) >= scale || p.DeletionTimestamp != nil {
			continue
		}
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Name == charmContainer && cs.State.Running != nil {
				out = append(out, n)
				break
			}
		}
	}
	sort.Ints(out)
	return out
}

// NextLeader decides who should hold the Lease. It returns the new holder and true when the Lease must change:
// it is empty or expired, or its holder is no longer a unit of the application (scaled away), and an eligible pod
// exists (lowest ordinal wins). A valid holder is never moved.
func NextLeader(app string, scale int32, l *coordinationv1.LeaseSpec, ready []int, now time.Time) (string, bool) {
	if cur := CurrentLeader(l, now); cur != "" {
		if n, ok := holderOrdinal(app, cur); ok && int32(n) < scale {
			return cur, false
		}
	}
	if len(ready) == 0 {
		return "", false
	}
	return UnitName(app, ready[0]), true
}

// NewLeaseSpec returns the Lease spec after assigning holder at now.
func NewLeaseSpec(old *coordinationv1.LeaseSpec, holder string, now time.Time) coordinationv1.LeaseSpec {
	t := metav1.NewMicroTime(now)
	s := coordinationv1.LeaseSpec{}
	if old != nil {
		s = *old.DeepCopy()
	}
	transitions := int32(0)
	if s.LeaseTransitions != nil {
		transitions = *s.LeaseTransitions
	}
	if s.HolderIdentity != nil && *s.HolderIdentity != "" {
		transitions++
	}
	s.HolderIdentity = &holder
	s.AcquireTime = &t
	s.RenewTime = &t
	s.LeaseTransitions = &transitions
	d := int32(LeaseDurationSeconds)
	s.LeaseDurationSeconds = &d
	return s
}
