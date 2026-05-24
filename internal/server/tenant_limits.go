package server

import (
	"fmt"
	"time"

	"github.com/turborg/turborg/internal/connector/irc"
)

// applyPlanLimits enforces a tenant's plan-tier caps on one IRC connector
// (M4). In container mode the host's cgroups + the connector env handle this;
// in pooled mode N tenants share one process, so the per-tenant outbound
// throttle and channel/identity locks must be enforced in-process — the same
// mechanism the single-tenant runtime wires from env, sourced here from the
// tenant spec instead.
//
// nil caps (self-host / spec without a plan block) leaves the connector at
// its unrestricted defaults.
func applyPlanLimits(conn *irc.Connector, caps *PlanCapabilities) error {
	if caps == nil {
		return nil
	}

	conn.SetClientLimits(irc.ClientLimits{
		NickLocked:     caps.NickLocked,
		RealnameLocked: caps.RealnameLocked,
		MaxChannels:    caps.MaxChannels,
	})

	if caps.OutboundMsgsPerWindow > 0 && caps.OutboundWindowSeconds > 0 {
		throttle, err := irc.NewThrottle(
			caps.OutboundMsgsPerWindow,
			time.Duration(caps.OutboundWindowSeconds)*time.Second,
			nil,
		)
		if err != nil {
			return fmt.Errorf("outbound throttle: %w", err)
		}
		conn.SetOutboundThrottle(throttle)
	}

	return nil
}
