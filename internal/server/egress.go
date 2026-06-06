package server

import (
	"net"
	"os"
	"strings"
	"sync"
)

// egressResolver maps a tenant's assigned public egress IP to the pool
// container's own local source IP on the matching egress network, so the IRC
// connector can bind it (the host SNAT then egresses on the public IP).
//
// TURBORG_EGRESS_MAP is a static `<public-ip>=<subnet-cidr>` list the sidecar
// injects (one entry per egress network the pool is attached to). The pool's
// own address inside that subnet is discovered at runtime from its interfaces —
// so the map needs only the subnets (known at create time), not the pool's
// per-network IPs (assigned after the extra networks are connected).
type egressResolver struct {
	once    sync.Once
	subnets map[string]*net.IPNet // public IP -> subnet
}

// defaultEgressResolver reads TURBORG_EGRESS_MAP once, lazily.
var defaultEgressResolver = &egressResolver{}

// resolveSourceIP returns the local source IP to bind for a tenant assigned
// publicIP, or "" to use the default route — the graceful path when egress
// mapping is unconfigured, the public IP is unknown, or the pool has no
// interface in that subnet (e.g. it wasn't attached to that egress network).
func (r *egressResolver) resolveSourceIP(publicIP string) string {
	if publicIP == "" {
		return ""
	}
	r.once.Do(r.parse)
	subnet := r.subnets[publicIP]
	if subnet == nil {
		return ""
	}
	return localIPInSubnet(subnet)
}

func (r *egressResolver) parse() {
	r.subnets = map[string]*net.IPNet{}
	for _, pair := range strings.Split(os.Getenv("TURBORG_EGRESS_MAP"), ",") {
		pub, cidr, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			continue
		}
		if _, ipnet, err := net.ParseCIDR(strings.TrimSpace(cidr)); err == nil {
			r.subnets[strings.TrimSpace(pub)] = ipnet
		}
	}
}

// localIPInSubnet returns this process's IPv4 address that falls inside subnet,
// or "" when none does.
func localIPInSubnet(subnet *net.IPNet) string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ip := ipnet.IP.To4(); ip != nil && subnet.Contains(ip) {
			return ip.String()
		}
	}
	return ""
}
