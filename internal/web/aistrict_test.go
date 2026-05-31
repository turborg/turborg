package web_test

import (
	"context"
	"testing"

	"encoding/json"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/connector/irc"
)

// strictGatewayBridge returns a bridge whose ClientLimits enable the AI gate
// with a recognisable notice. Channel membership is set by the caller.
func strictGatewayBridge() *fakeBridge {
	b := newFakeBridge("turborg")
	b.limits = irc.ClientLimits{
		AIStrict:        true,
		AIStrictMessage: "op consent required here",
	}
	return b
}

func TestTBOpSummarizeAIStrictDeniesNonOp(t *testing.T) {
	bridge := strictGatewayBridge()
	bridge.state.OnSelfJoin("#x")
	// Bot present but NOT opped.
	bridge.state.OnNamesReply("#x", []string{"turborg", "alice"})
	bridge.state.OnNamesEnd("#x")

	opts := newOptions(t, "p")
	opts.LLMProvider = &testLLM{response: "should not run"}
	opts.TBSummarizeMaxMessages = 200
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	// The gate returns before tb_status, so the first frame is the error.
	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Equal(t, "op consent required here", got["message"])
}

func TestTBOpSummarizeAIStrictAllowsOp(t *testing.T) {
	bridge := strictGatewayBridge()
	bridge.state.OnSelfJoin("#x")
	// Bot holds +o → consent granted, the command runs.
	bridge.state.OnNamesReply("#x", []string{"@turborg", "alice"})
	bridge.state.OnNamesEnd("#x")

	opts := newOptions(t, "p")
	opts.LLMProvider = &testLLM{response: "chat summary"}
	opts.TBSummarizeMaxMessages = 200
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	// Gate passed → first frame is tb_status, not the deny error.
	got := readJSON(t, conn)
	assert.Equal(t, "tb_status", got["op"])
}
