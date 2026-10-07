package agent

import (
	"net"
	"net/netip"
	"testing"
)

// ops parses network-get's cidr with ipaddress.ip_network(strict), so host bits must be clear.
func TestNetworkCIDRClearsHostBits(t *testing.T) {
	t.Parallel()
	ip, n, _ := net.ParseCIDR("10.244.0.48/24")
	got := networkCIDR(&net.IPNet{IP: ip, Mask: n.Mask})
	if got != "10.244.0.0/24" {
		t.Fatalf("got %s", got)
	}
	p, err := netip.ParsePrefix(got)
	if err != nil || p.Masked() != p {
		t.Fatalf("not a canonical network: %v %v", p, err)
	}
}
