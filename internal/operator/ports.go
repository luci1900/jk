// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0:
// internal/provider/kubernetes/application/application.go (port names and the placeholder port).

package operator

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/luci1900/jk/api/v1alpha1"
)

const (
	placeholderPortName = "placeholder"
	placeholderPort     = 65535
	portNamePrefix      = "juju-"
)

// UnionPorts is the sorted union of the units' opened port ranges, without duplicates.
func UnionPorts(units ...[]v1alpha1.PortRange) []v1alpha1.PortRange {
	seen := map[v1alpha1.PortRange]bool{}
	var out []v1alpha1.PortRange
	for _, ports := range units {
		for _, p := range ports {
			p.Protocol = strings.ToLower(p.Protocol)
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Protocol < b.Protocol
	})
	return out
}

// ServicePorts maps opened ports to Service ports as juju's k8s firewaller does: one per range, named
// juju-<from>[-<to>]-<protocol>, with the range's end as the target port (a single port maps to itself); only
// tcp, udp and sctp can be exposed. With nothing to expose it returns juju's placeholder port.
func ServicePorts(opened []v1alpha1.PortRange) []corev1.ServicePort {
	var out []corev1.ServicePort
	for _, p := range UnionPorts(opened) {
		proto, ok := map[string]corev1.Protocol{"tcp": corev1.ProtocolTCP, "udp": corev1.ProtocolUDP, "sctp": corev1.ProtocolSCTP}[p.Protocol]
		if !ok || p.From < 1 || p.From > 65535 || p.To < p.From || p.To > 65535 {
			continue
		}
		name := fmt.Sprintf("%s%d-%s", portNamePrefix, p.From, p.Protocol)
		if p.To != p.From {
			name = fmt.Sprintf("%s%d-%d-%s", portNamePrefix, p.From, p.To, p.Protocol)
		}
		out = append(out, corev1.ServicePort{Name: name, Port: p.From, TargetPort: intstr.FromInt32(p.To), Protocol: proto})
	}
	if len(out) == 0 {
		return []corev1.ServicePort{{Name: placeholderPortName, Port: placeholderPort, Protocol: corev1.ProtocolTCP}}
	}
	return out
}
