package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

func newBareConnector() *irc.Connector {
	return irc.New(&irc.Settings{Hostname: "h", Port: 6697, Nick: "n"}, nil, nil)
}

func TestApplyPlanLimitsSetsThrottleAndLocks(t *testing.T) {
	conn := newBareConnector()
	err := applyPlanLimits(conn, &PlanCapabilities{
		NickLocked:            true,
		RealnameLocked:        true,
		MaxChannels:           5,
		OutboundMsgsPerWindow: 5,
		OutboundWindowSeconds: 30,
	})
	require.NoError(t, err)

	limits := conn.ClientLimits()
	require.True(t, limits.NickLocked)
	require.True(t, limits.RealnameLocked)
	require.Equal(t, 5, limits.MaxChannels)
	require.NotNil(t, conn.OutboundThrottle(), "outbound throttle should be installed")
}

func TestApplyPlanLimitsNilCapsLeavesDefaults(t *testing.T) {
	conn := newBareConnector()
	require.NoError(t, applyPlanLimits(conn, nil))

	require.Equal(t, 0, conn.ClientLimits().MaxChannels)
	require.Nil(t, conn.OutboundThrottle(), "no throttle without caps")
}

func TestApplyPlanLimitsNoThrottleWhenWindowZero(t *testing.T) {
	conn := newBareConnector()
	require.NoError(t, applyPlanLimits(conn, &PlanCapabilities{
		MaxChannels:           10,
		OutboundMsgsPerWindow: 5,
		OutboundWindowSeconds: 0, // disabled
	}))

	require.Equal(t, 10, conn.ClientLimits().MaxChannels)
	require.Nil(t, conn.OutboundThrottle(), "throttle requires a positive window")
}
