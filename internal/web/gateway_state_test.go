package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// drainUntilOp keeps reading WS frames until one matches the wanted op,
// or the test budget elapses. Returns the matching frame or nil.
func drainUntilOp(t *testing.T, conn *websocket.Conn, op string, budget time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, body, err := conn.Read(ctx)
		cancel()
		if err != nil {
			continue
		}
		var p map[string]any
		if err := json.Unmarshal(body, &p); err != nil {
			continue
		}
		if got, _ := p["op"].(string); got == op {
			return p
		}
	}
	return nil
}

// TestGatewayBroadcastsConnectorStateChanged covers the central scenario
// 3 fix: when the IRC state machine transitions, every attached WS
// client receives a connector.state_changed event with the new state,
// severity, and a human-readable body matching what the bouncer would
// fan as a channel NOTICE.
func TestGatewayBroadcastsConnectorStateChanged(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	ws := dialWS(t, g.Addr(), "p")
	defer ws.Close(0, "")

	// Drain the initial state + replay frames so the next read sees
	// only fresh state-change events.
	drainInitialFrames(t, ws)

	// Drive a transition through the bridge's state machine.
	bridge.UpstreamState().Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("Ping timeout: 240 seconds"))

	got := drainUntilOp(t, ws, "connector.state_changed", time.Second)
	require.NotNil(t, got, "transition must fan a connector.state_changed event")
	assert.Equal(t, "disconnected_transient", got["state"])
	assert.Equal(t, "registered", got["prior_state"])
	assert.Equal(t, "warning", got["severity"],
		"disconnected_transient is recoverable — severity must be warning, not error")
	assert.Equal(t, "Ping timeout: 240 seconds", got["server_reason"],
		"server-supplied reason from WithServerReason must thread into the event payload")
	assert.Contains(t, got["message"], "NOT be delivered",
		"message body must carry the user-facing 'NOT delivered' warning")
}

// TestGatewayStateChangedSeverityForTerminalStates verifies the
// severity = "error" branch for terminal states (paused / banned /
// auth-failed) — the UI uses this to decide whether to disable the
// send input.
func TestGatewayStateChangedSeverityForTerminalStates(t *testing.T) {
	cases := []struct {
		state irc.UpstreamState
		want  string
	}{
		{irc.UpstreamStateDisconnectedAuthFailed, "error"},
		{irc.UpstreamStateDisconnectedBanned, "error"},
		{irc.UpstreamStatePausedIdle, "error"},
		{irc.UpstreamStateConnecting, "warning"},
		{irc.UpstreamStateDisconnectedNickUnavailable, "warning"},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			bridge := newFakeBridge("turborg")
			g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
			defer td()

			ws := dialWS(t, g.Addr(), "p")
			defer ws.Close(0, "")
			drainInitialFrames(t, ws)

			bridge.UpstreamState().Transition(tc.state)
			got := drainUntilOp(t, ws, "connector.state_changed", time.Second)
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got["severity"])
		})
	}
}

// TestGatewaySayRejectedWhenStateNotRegistered covers the load-bearing
// silent-message-loss fix on the WS side: a `say` op while upstream is
// disconnected must produce a send_message.rejected frame (echoing the
// original target + body so the UI can mark the bubble), never flow
// to the sender.
func TestGatewaySayRejectedWhenStateNotRegistered(t *testing.T) {
	bridge := newFakeBridge("turborg")
	sender := &fakeSender{}
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, sender)
	defer td()

	ws := dialWS(t, g.Addr(), "p")
	defer ws.Close(0, "")
	drainInitialFrames(t, ws)

	// Drop upstream out from under any subsequent say op.
	bridge.UpstreamState().Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("connection reset by peer"))
	_ = drainUntilOp(t, ws, "connector.state_changed", time.Second)

	require.NoError(t, ws.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","channel":"#archlinux","text":"hello"}`)))

	got := drainUntilOp(t, ws, "send_message.rejected", time.Second)
	require.NotNil(t, got, "say while detached must produce a send_message.rejected frame")
	assert.Equal(t, "#archlinux", got["target"], "rejected frame echoes the original target")
	assert.Equal(t, "hello", got["body"], "rejected frame echoes the original body")
	assert.Equal(t, "disconnected_transient", got["reason"])
	assert.Contains(t, got["message"], "NOT be delivered")

	assert.Empty(t, sender.Outbound(),
		"rejected say must NOT reach the sender — that's the load-bearing fix")
}

// TestGatewaySayWriteErrorPathRejects covers the write-error race
// branch from Edge 2: state was registered at the gate, the sender's
// Send call failed mid-flight (upstream just went down). The user
// must see a send_message.rejected frame rather than silent loss.
func TestGatewaySayWriteErrorPathRejects(t *testing.T) {
	bridge := newFakeBridge("turborg")
	sender := &fakeSender{sendErr: errors.New("upstream write failed mid-flight")}
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, sender)
	defer td()

	ws := dialWS(t, g.Addr(), "p")
	defer ws.Close(0, "")
	drainInitialFrames(t, ws)

	require.NoError(t, ws.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","channel":"#a","text":"hi"}`)))

	got := drainUntilOp(t, ws, "send_message.rejected", time.Second)
	require.NotNil(t, got,
		"sender write failure must surface as send_message.rejected, not silent drop")
	assert.Equal(t, "#a", got["target"])
	assert.Equal(t, "hi", got["body"])
	// State stayed registered (no transition fired), so reason falls
	// back to the generic "send_failed" with the wrapped error text.
	assert.Equal(t, "send_failed", got["reason"])
}

// TestGatewaySayDuringRegisteredFlowsThrough is the happy-path
// regression cover: when state is registered, say ops flow to the
// sender exactly as before.
func TestGatewaySayDuringRegisteredFlowsThrough(t *testing.T) {
	bridge := newFakeBridge("turborg") // default registered
	sender := &fakeSender{}
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, sender)
	defer td()

	ws := dialWS(t, g.Addr(), "p")
	defer ws.Close(0, "")
	drainInitialFrames(t, ws)

	require.NoError(t, ws.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","channel":"#a","text":"hi"}`)))

	require.Eventually(t, func() bool {
		return len(sender.Outbound()) == 1
	}, time.Second, 10*time.Millisecond,
		"registered-state say must reach the sender")
}
