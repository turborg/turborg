package irc

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBringUpAbortsWhenConnectGateContextCancelled covers bringUp's connect-gate
// admission branch: when the gate makes a connect wait and the supervisor
// context is cancelled (shutdown / terminal escalation) before a slot opens,
// bringUp returns the ctx error and classifyFallback routes the machine to a
// terminal Stopped state — it must NOT proceed to Dial. This is the path that,
// before the fix, conflated a long gate wait with a dial timeout.
func TestBringUpAbortsWhenConnectGateContextCancelled(t *testing.T) {
	t.Cleanup(func() { EnableConnectGate(0) })
	// Burst 1 + a long interval so the bucket empties after one connect and the
	// next admission must block.
	EnableConnectGateWithBurst(time.Hour, 1)

	settings := &Settings{Nick: "x", Hostname: "irc.example.test", Port: 6667}
	// Drain the single token for this (sourceIP, host) key so bringUp's wait
	// below has nothing to take and parks on the refill.
	require.NoError(t, awaitConnectSlot(context.Background(), settings.SourceIP, settings.Hostname))

	c := New(settings, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	// A pre-cancelled context: the gate wait returns immediately with ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.bringUp(ctx)
	require.Error(t, err, "bringUp must surface the cancelled gate wait")
	require.Equal(t, UpstreamStateStopped, c.machine.State(),
		"a cancelled gate wait routes through classifyFallback to Stopped")
}
