package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/messages"
)

// White-box tests for the helpers in runtime.go. The full Build flow
// is exercised in runtime_test.go; these tighten the unit-level
// coverage on the small pure helpers.

func TestClampReplayDepthBounds(t *testing.T) {
	assert.Equal(t, 200, clampReplayDepth(0), "zero falls back to default")
	assert.Equal(t, 200, clampReplayDepth(-1), "negative falls back to default")
	assert.Equal(t, 1, clampReplayDepth(1))
	assert.Equal(t, 1000, clampReplayDepth(1000))
	assert.Equal(t, 2000, clampReplayDepth(2000))
	assert.Equal(t, 2000, clampReplayDepth(5000), "over-max clamps to ceiling")
}

func TestIsChannelTarget(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"#chan", true}, {"&local", true}, {"+modeless", true}, {"!safe", true},
		{"alice", false}, {"", false}, {"~bad", false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, isChannelTarget(tc.in), "input=%q", tc.in)
	}
}

func TestBuildMessageStoreMemoryDefault(t *testing.T) {
	s := &config.Settings{} // No sink, no store URLs configured.
	store, sink := buildMessageStore(s, nil)
	require.NotNil(t, store)
	assert.Nil(t, sink, "no sink URL → nil *Sink")
	_, isMem := store.(*messages.MemoryStore)
	assert.True(t, isMem, "default store must be MemoryStore")
}

func TestBuildMessageStoreHTTPWhenConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	s := &config.Settings{
		MessageSinkURL:    srv.URL,
		MessageSinkToken:  "t",
		MessageStoreURL:   srv.URL,
		MessageStoreToken: "t",
	}
	store, sink := buildMessageStore(s, nil)
	require.NotNil(t, store)
	require.NotNil(t, sink)
	defer sink.Close(context.Background())
	_, isHTTP := store.(*messages.HTTPStore)
	assert.True(t, isHTTP, "with both URLs set → HTTPStore")
}

func TestMakeStoreSubmitterSkipsNonChannelTargets(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessage,
		Fields: map[string]any{
			"channel": "alice", "sender": "bob", "text": "psst",
		},
	})
	assert.Equal(t, 0, store.Len("alice"), "DM target must not be stored")
}

func TestMakeStoreSubmitterSubmitsChannelMessages(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessage,
		Fields: map[string]any{
			"channel": "#x", "sender": "alice", "text": "hi",
		},
	})
	assert.Equal(t, 1, store.Len("#x"))
}

func TestMakeStoreSubmitterUnpacksInboundEnvelope(t *testing.T) {
	// Some publishers carry the data on `envelope` instead of the
	// flat fields — the submitter must read both shapes.
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessage,
		Fields: map[string]any{
			"envelope": &agent.InboundEnvelope{
				Channel: "#x", Sender: "alice", Text: "hi",
			},
		},
	})
	assert.Equal(t, 1, store.Len("#x"))
}

func TestMakeStoreSubmitterUnpacksOutboundEnvelope(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessageSent,
		Fields: map[string]any{
			"sender":   "bot",
			"envelope": &agent.OutboundEnvelope{Channel: "#x", Text: "reply"},
		},
	})
	assert.Equal(t, 1, store.Len("#x"))
}

func TestMakeStoreSubmitterEmptyChannelNoop(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil)
	sub(context.Background(), &agent.Event{
		Type:   agent.EventMessage,
		Fields: map[string]any{"sender": "alice", "text": "no channel"},
	})
	// MemoryStore tracks per-channel; absent channel == nothing recorded.
	assert.Equal(t, 0, store.Len(""))
}
