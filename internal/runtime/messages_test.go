package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/skill"
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

func TestMakeStoreSubmitterPersistsDMs(t *testing.T) {
	// DMs (handlePrivmsg sets env.Channel = sender when IsDirect) land
	// in the store too — each conversation gets its own per-peer
	// bucket. Previously the submitter filtered these out with an
	// "isChannelTarget" check, which threw away every direct message
	// the bot received or sent.
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessage,
		Fields: map[string]any{
			"channel": "alice", "sender": "alice", "text": "psst",
		},
	})
	assert.Equal(t, 1, store.Len("alice"),
		"DM rows must be persisted under the peer-nick bucket")
}

func TestMakeStoreSubmitterSubmitsChannelMessages(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil, nil)
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
	sub := makeStoreSubmitter(store, nil, nil)
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
	sub := makeStoreSubmitter(store, nil, nil)
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
	sub := makeStoreSubmitter(store, nil, nil)
	sub(context.Background(), &agent.Event{
		Type:   agent.EventMessage,
		Fields: map[string]any{"sender": "alice", "text": "no channel"},
	})
	// MemoryStore tracks per-channel; absent channel == nothing recorded.
	assert.Equal(t, 0, store.Len(""))
}

// TestMakeStoreSubmitterOutboundUsesBotNick pins the contract that
// command replies (!ping → pong) attribute correctly to the bot.
// OutboundEnvelope has no Sender field — the bot is implicit — so the
// submitter must consult the botNick callback to populate the
// row's nick. The receiver's validator rejects an
// empty nick with 422, which silently dropped every command reply
// until this seam was added.
func TestMakeStoreSubmitterOutboundUsesBotNick(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, func() string { return "xinfolocal" }, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessageSent,
		Fields: map[string]any{
			"envelope": &agent.OutboundEnvelope{Channel: "#x", Text: "pong"},
		},
	})
	assert.Equal(t, 1, store.Len("#x"))
	got, err := store.Recent(context.Background(), "#x", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "xinfolocal", got[0].Nick, "outbound nick must come from botNick callback")
	assert.Equal(t, "pong", got[0].Text)
}

// TestMakeStoreSubmitterOutboundSkipsWhenBotNickEmpty guards against
// the half-fix where botNick returns "" (the IRC connector hasn't yet
// learned the live nick from the server — pre-001-welcome window). The
// submitter must NOT POST a row that would 422 at the receiver; it
// drops the message silently and lets the next attempt succeed.
func TestMakeStoreSubmitterOutboundSkipsWhenBotNickEmpty(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, func() string { return "" }, nil)
	sub(context.Background(), &agent.Event{
		Type: agent.EventMessageSent,
		Fields: map[string]any{
			"envelope": &agent.OutboundEnvelope{Channel: "#x", Text: "pong"},
		},
	})
	assert.Equal(t, 0, store.Len("#x"),
		"empty bot nick → row would 422; must skip rather than write a half-formed entry")
}

// TestMakeStoreSubmitterDedupesSendAndSelfEcho pins the duplicate-row fix: a
// message you send arrives on MESSAGE_SENT and again as the IRC self-echo
// MESSAGE. One submitter closure subscribed to both events must persist it
// exactly once (previously it wrote both, each with a fresh minted msg_id, so
// the receiver's msg_id dedupe couldn't collapse them).
func TestMakeStoreSubmitterDedupesSendAndSelfEcho(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, func() string { return "bot" }, nil)
	sub(context.Background(), &agent.Event{
		Type:   agent.EventMessageSent,
		Fields: map[string]any{"envelope": &agent.OutboundEnvelope{Channel: "#x", Text: "hello"}},
	})
	sub(context.Background(), &agent.Event{
		Type:   agent.EventMessage,
		Fields: map[string]any{"channel": "#x", "sender": "bot", "text": "hello"},
	})
	assert.Equal(t, 1, store.Len("#x"), "send + self-echo must collapse to one row")
}

// TestMakeStoreSubmitterKeepsDistinctMessages guards against the dedupe being
// too greedy: different text, or the same text from a different nick, are
// genuinely different messages and must all persist.
func TestMakeStoreSubmitterKeepsDistinctMessages(t *testing.T) {
	store := messages.NewMemoryStore(0)
	sub := makeStoreSubmitter(store, nil, nil)
	fields := []map[string]any{
		{"channel": "#x", "sender": "alice", "text": "one"},
		{"channel": "#x", "sender": "alice", "text": "two"},
		{"channel": "#x", "sender": "bob", "text": "one"},
	}
	for _, f := range fields {
		sub(context.Background(), &agent.Event{Type: agent.EventMessage, Fields: f})
	}
	assert.Equal(t, 3, store.Len("#x"), "distinct text/nick must not be deduped")
}

// TestStoreDedupeFirstSight exercises the dedupe helper directly: a first
// sighting persists, an immediate repeat within the window is suppressed, the
// same key after the window is a fresh sighting again, and once the map grows
// past the GC threshold a later sighting evicts the stale keys.
func TestStoreDedupeFirstSight(t *testing.T) {
	d := &storeDedupe{seen: make(map[string]time.Time)}
	base := time.Unix(1_700_000_000, 0)

	assert.True(t, d.firstSight("a", base), "first sighting persists")
	assert.False(t, d.firstSight("a", base.Add(time.Second)), "repeat within window is suppressed")
	assert.True(t, d.firstSight("a", base.Add(2*storeDedupeWindow)), "same key past the window is fresh again")

	for i := 0; i < 600; i++ {
		d.firstSight(fmt.Sprintf("k%d", i), base)
	}
	assert.True(t, d.firstSight("trigger", base.Add(10*time.Second)),
		"a new key after the window is a first sighting")
	assert.Less(t, len(d.seen), 600, "stale keys evicted once the map grew past the GC threshold")
}

func TestBuildSkillStoreNilWhenUnconfigured(t *testing.T) {
	s := &config.Settings{} // No STATE_URL configured.
	assert.Nil(t, buildSkillStore(s, nil), "no state URL → nil (engines default to in-process)")
}

func TestBuildSkillStoreHTTPWhenConfigured(t *testing.T) {
	s := &config.Settings{StateURL: "https://state.example/agent", StateToken: "t"}
	got := buildSkillStore(s, nil)
	require.NotNil(t, got)
	_, isHTTP := got.(*skill.HTTPStore)
	assert.True(t, isHTTP, "with STATE_URL + token set → HTTPStore")
}
