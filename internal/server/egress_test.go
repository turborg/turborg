package server

import "testing"

func TestEgressResolverResolvesToLocalIPInSubnet(t *testing.T) {
	// Map a (documentation-range) public IP to the loopback subnet; the resolver
	// must discover this process's 127.0.0.1 inside it. A second entry points at a
	// subnet this host has no interface in → no source IP.
	t.Setenv("TURBORG_EGRESS_MAP", "203.0.113.7=127.0.0.0/8,198.51.100.9=10.255.255.0/24")
	r := &egressResolver{}

	if got := r.resolveSourceIP("203.0.113.7"); got != "127.0.0.1" {
		t.Fatalf("resolveSourceIP(mapped→loopback) = %q; want 127.0.0.1", got)
	}
	if got := r.resolveSourceIP("198.51.100.9"); got != "" {
		t.Fatalf("resolveSourceIP(no local iface in subnet) = %q; want empty", got)
	}
}

func TestEgressResolverGracefulMisses(t *testing.T) {
	t.Setenv("TURBORG_EGRESS_MAP", "203.0.113.7=127.0.0.0/8,bad-entry,=skip,nope=not-a-cidr")
	r := &egressResolver{}

	if got := r.resolveSourceIP(""); got != "" {
		t.Fatalf("empty public IP = %q; want empty", got)
	}
	if got := r.resolveSourceIP("9.9.9.9"); got != "" {
		t.Fatalf("unmapped public IP = %q; want empty", got)
	}
	// Malformed entries are skipped, the valid one still resolves.
	if got := r.resolveSourceIP("203.0.113.7"); got != "127.0.0.1" {
		t.Fatalf("valid entry alongside malformed = %q; want 127.0.0.1", got)
	}
}

func TestEgressResolverEmptyMap(t *testing.T) {
	t.Setenv("TURBORG_EGRESS_MAP", "")
	r := &egressResolver{}
	if got := r.resolveSourceIP("203.0.113.7"); got != "" {
		t.Fatalf("no map configured = %q; want empty", got)
	}
}
