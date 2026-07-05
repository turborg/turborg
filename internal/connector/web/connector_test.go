package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/messages"
)

const testKey = "tenant-a-container-token"

func newTestConn(t *testing.T, s Settings) (*Connector, *agent.EventBus) {
	t.Helper()
	if s.Room == "" {
		s.Room = "console"
	}
	bus := agent.NewEventBus(nil)
	c := New(s, nil, bus, newVerifier(t, testKey))
	c.SetMessageStore(messages.NewMemoryStore(0))
	// Stop closes any server-side sockets, which lets the ServeWS read loops
	// (owned by the httptest server) return so goleak stays clean.
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, bus
}

// serveConn stands the connector up behind an httptest server that routes every
// request as tenant `tenantID`, and returns the ws:// base URL.
func serveConn(t *testing.T, c *Connector, tenantID string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.ServeWS(w, r, tenantID)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dial(t *testing.T, base, token string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(context.Background(), base+"/chat?token="+token, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	return conn
}

func readFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func validToken(t *testing.T, tenantID, room, role string) string {
	return mintToken(t, testKey, Claims{
		TenantID: tenantID, Room: room, Role: role, VisitorID: "user-1", ExpiresAt: farFuture,
	})
}

// TestInboundSayProducesEnvelope drives the core connector→agent path: a `say`
// frame becomes a normalized InboundEnvelope on Inbound(), and the sender's own
// message is echoed back as a user frame.
func TestInboundSayProducesEnvelope(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console", Public: false})
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))

	assert.Equal(t, "state", readFrame(t, conn)["op"])

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"hello there"}`)))

	echo := readFrame(t, conn)
	assert.Equal(t, "message", echo["op"])
	assert.Equal(t, "user", echo["kind"])
	assert.Equal(t, "hello there", echo["text"])

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "web", env.Connector)
		assert.Equal(t, "console", env.Channel)
		assert.Equal(t, "owner", env.Sender)
		assert.Equal(t, "hello there", env.Text)
		assert.True(t, env.IsDirect, "private console messages are direct")
		assert.Equal(t, "owner", env.Metadata["role"])
	case <-time.After(2 * time.Second):
		t.Fatal("no inbound envelope produced")
	}
}

// TestPublicRoomInboundNotDirect checks a public room does not mark inbound
// messages as direct (owner-only handlers must not treat a public message as a
// DM from the owner).
func TestPublicRoomInboundNotDirect(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "lobby", Public: true})
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "lobby", "visitor"))
	_ = readFrame(t, conn) // state

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"hi"}`)))
	_ = readFrame(t, conn) // echo

	select {
	case env := <-c.Inbound():
		assert.False(t, env.IsDirect)
	case <-time.After(2 * time.Second):
		t.Fatal("no inbound envelope produced")
	}
}

// TestActorSayBroadcastsAndPublishes verifies Actor.Say fans a bot frame to
// attached clients AND publishes MESSAGE_SENT (so the shared store submitter
// persists bot output).
func TestActorSayBroadcastsAndPublishes(t *testing.T) {
	c, bus := newTestConn(t, Settings{BotNick: "helper", Room: "console"})

	sentCh := make(chan *agent.Event, 1)
	bus.Subscribe(agent.EventMessageSent, func(_ context.Context, ev *agent.Event) {
		sentCh <- ev
	})

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	assert.Equal(t, "state", readFrame(t, conn)["op"])

	// Wait for the client to register so the broadcast reaches it.
	require.Eventually(t, func() bool { return c.clientCount() == 1 }, time.Second, 10*time.Millisecond)

	require.NoError(t, NewActor(c).Say("console", "how can I help?"))

	frame := readFrame(t, conn)
	assert.Equal(t, "message", frame["op"])
	assert.Equal(t, "bot", frame["kind"])
	assert.Equal(t, "helper", frame["sender"])
	assert.Equal(t, "how can I help?", frame["text"])

	select {
	case ev := <-sentCh:
		assert.Equal(t, "console", ev.Fields["channel"])
		assert.Equal(t, "helper", ev.Fields["sender"])
		assert.Equal(t, "how can I help?", ev.Fields["text"])
	case <-time.After(2 * time.Second):
		t.Fatal("MESSAGE_SENT not published")
	}
}

// TestSendBroadcastsBotFrame checks a command reply routed through the agent
// (Connector.Send) reaches clients as a bot frame.
func TestSendBroadcastsBotFrame(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	require.Eventually(t, func() bool { return c.clientCount() == 1 }, time.Second, 10*time.Millisecond)

	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "console", Text: "pong"}))

	frame := readFrame(t, conn)
	assert.Equal(t, "bot", frame["kind"])
	assert.Equal(t, "pong", frame["text"])
}

// TestHistoryReplayedOnAttach seeds the store and asserts a fresh client is
// replayed prior messages, oldest-first, flagged replayed.
func TestHistoryReplayedOnAttach(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	store := messages.NewMemoryStore(0)
	c.SetMessageStore(store)
	ctx := context.Background()
	require.NoError(t, store.Submit(ctx, messages.Message{Channel: "console", Nick: "owner", Text: "first", TS: time.Now().Add(-time.Minute)}))
	require.NoError(t, store.Submit(ctx, messages.Message{Channel: "console", Nick: "helper", Text: "second", TS: time.Now()}))

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	assert.Equal(t, "state", readFrame(t, conn)["op"])

	first := readFrame(t, conn)
	assert.Equal(t, "first", first["text"])
	assert.Equal(t, "user", first["kind"])
	assert.Equal(t, true, first["replayed"])

	second := readFrame(t, conn)
	assert.Equal(t, "second", second["text"])
	assert.Equal(t, "bot", second["kind"], "bot-authored history renders as bot")
}

// TestServeWSRejectsCrossTenantToken is the security-critical case: a token
// minted for tenant-a is presented to a router that resolved tenant-b. It must
// fail closed with 401 before any upgrade.
func TestServeWSRejectsCrossTenantToken(t *testing.T) {
	c, _ := newTestConn(t, Settings{Room: "console"})
	tok := validToken(t, "tenant-a", "console", "owner")

	req := httptest.NewRequest(http.MethodGet, "/chat/tenant-b?token="+tok, nil)
	rec := httptest.NewRecorder()
	c.ServeWS(rec, req, "tenant-b")

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestServeWSRejectsMissingToken(t *testing.T) {
	c, _ := newTestConn(t, Settings{Room: "console"})
	req := httptest.NewRequest(http.MethodGet, "/chat/tenant-a", nil)
	rec := httptest.NewRecorder()
	c.ServeWS(rec, req, "tenant-a")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestActorNoticeAndModerationDenies covers Notice (renders like Say) and the
// moderation actions the private console can't perform (they must error, per
// the Actor contract).
func TestActorNoticeAndModerationDenies(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	require.Eventually(t, func() bool { return c.clientCount() == 1 }, time.Second, 10*time.Millisecond)

	a := NewActor(c)
	require.NoError(t, a.Notice("console", "heads up"))
	frame := readFrame(t, conn)
	assert.Equal(t, "bot", frame["kind"])
	assert.Equal(t, "heads up", frame["text"])

	assert.Error(t, a.Kick("console", "x", "r"))
	assert.Error(t, a.Ban("console", "x"))
	assert.Error(t, a.SetMode("console", "+m"))
	assert.Error(t, a.Op("console", "x"))
	assert.Error(t, a.Voice("console", "x"))
	assert.Error(t, a.Topic("console", "t"))
	assert.Error(t, a.Invite("console", "x"))
}

// TestHistoryOp exercises the on-demand scrollback op.
func TestHistoryOp(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	store := messages.NewMemoryStore(0)
	c.SetMessageStore(store)
	require.NoError(t, store.Submit(context.Background(),
		messages.Message{Channel: "console", Nick: "owner", Text: "old one", TS: time.Now()}))

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	_ = readFrame(t, conn) // replay of "old one"

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"history","limit":5}`)))
	res := readFrame(t, conn)
	assert.Equal(t, "history_result", res["op"])
	assert.Equal(t, false, res["has_more"])
	msgs, ok := res["messages"].([]any)
	require.True(t, ok)
	require.Len(t, msgs, 1)
}

// TestHistoryOpNoStore covers the empty-result branch when no store is wired.
func TestHistoryOpNoStore(t *testing.T) {
	c := New(Settings{Room: "console"}, nil, agent.NewEventBus(nil), newVerifier(t, testKey))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state (no store → no replay frames)

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"history"}`)))
	res := readFrame(t, conn)
	assert.Equal(t, "history_result", res["op"])
	assert.Empty(t, res["messages"])
}

// TestActivityHookFiresOnInbound checks an owner message marks presence.
func TestActivityHookFiresOnInbound(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	fired := make(chan string, 1)
	c.SetActivityHook(func(reason string) { fired <- reason })

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"present"}`)))
	select {
	case reason := <-fired:
		assert.Equal(t, "web_message", reason)
	case <-time.After(2 * time.Second):
		t.Fatal("activity hook did not fire")
	}
	<-c.Inbound() // drain so goleak/teardown is clean
}

// TestEmptySayIgnored checks a blank message produces no envelope.
func TestEmptySayIgnored(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"   "}`)))
	// Follow with a real message; if the blank one had produced an envelope it
	// would be first in the inbox.
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"real"}`)))
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text)
}

// TestNewDefaults covers the room-default and nil-log fallbacks in New.
func TestNewDefaults(t *testing.T) {
	c := New(Settings{}, nil, agent.NewEventBus(nil), newVerifier(t, testKey))
	assert.Equal(t, "console", c.settings.Room)
	assert.NotNil(t, c.log)
}

// TestBearerHeaderAuth covers the Authorization: Bearer token path.
func TestBearerHeaderAuth(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	base := serveConn(t, c, "tenant-a")
	tok := validToken(t, "tenant-a", "console", "owner")

	conn, _, err := websocket.Dial(context.Background(), base+"/chat", &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + tok}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	assert.Equal(t, "state", readFrame(t, conn)["op"])
}

// TestServeWSAfterStopCloses covers the closed-connector reject branch: a client
// arriving after Stop is accepted then immediately closed, never registered.
func TestServeWSAfterStopCloses(t *testing.T) {
	c, _ := newTestConn(t, Settings{Room: "console"})
	require.NoError(t, c.Stop(context.Background()))

	base := serveConn(t, c, "tenant-a")
	tok := validToken(t, "tenant-a", "console", "owner")
	conn, _, err := websocket.Dial(context.Background(), base+"/chat?token="+tok, nil)
	if err == nil {
		t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _, readErr := conn.Read(ctx)
		assert.Error(t, readErr, "socket should be closed by a stopped connector")
	}
	assert.Zero(t, c.clientCount())
}

// TestBroadcastDropsDeadClient covers sendTo's write-error branch: once a client
// socket is gone, a broadcast prunes it.
func TestBroadcastDropsDeadClient(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	require.Eventually(t, func() bool { return c.clientCount() == 1 }, time.Second, 10*time.Millisecond)

	// Abort the client without a clean close so the server-side write fails
	// before the read loop notices the disconnect.
	_ = conn.CloseNow()

	// Keep broadcasting until the dead client is pruned by either sendTo's
	// write-error branch or the read loop's deferred cleanup.
	require.Eventually(t, func() bool {
		_ = c.Send(&agent.OutboundEnvelope{Channel: "console", Text: "ping"})
		return c.clientCount() == 0
	}, 2*time.Second, 20*time.Millisecond)
}

// TestSendToPrunesOnWriteError deterministically drives sendTo's write-error
// branch: a registered client whose socket is closed is pruned on the next
// broadcast. Manipulates the client set directly (in-package) to avoid racing
// the read loop's own cleanup.
func TestSendToPrunesOnWriteError(t *testing.T) {
	c, _ := newTestConn(t, Settings{Room: "console"})

	serverConnCh := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sc, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		serverConnCh <- sc // hand the accepted (hijacked) conn to the test
	}))
	defer srv.Close()

	base := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, _, err := websocket.Dial(context.Background(), base, nil)
	require.NoError(t, err)
	sc := <-serverConnCh
	_ = client.Close(websocket.StatusNormalClosure, "")
	_ = sc.Close(websocket.StatusNormalClosure, "") // subsequent writes now error

	cl := &wsClient{conn: sc}
	c.mu.Lock()
	c.clients[cl] = struct{}{}
	c.mu.Unlock()
	require.Equal(t, 1, c.clientCount())

	c.broadcast(map[string]any{"op": "message", "text": "x"})
	assert.Zero(t, c.clientCount(), "write error prunes the dead client")
}

// TestHistoryBeforeFilter covers the `before` timestamp parse in the history op.
func TestHistoryBeforeFilter(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	store := messages.NewMemoryStore(0)
	c.SetMessageStore(store)
	old := time.Now().Add(-time.Hour)
	require.NoError(t, store.Submit(context.Background(),
		messages.Message{Channel: "console", Nick: "owner", Text: "ancient", TS: old}))

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	_ = readFrame(t, conn) // replay

	// before = now → the hour-old message is older, so it's returned.
	before := time.Now().Format(time.RFC3339Nano)
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"history","before":"`+before+`"}`)))
	res := readFrame(t, conn)
	assert.Equal(t, "history_result", res["op"])
	msgs, _ := res["messages"].([]any)
	assert.Len(t, msgs, 1)
}

func TestClientIPFallsBackToRemoteAddr(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/chat", nil)
	r.RemoteAddr = "no-port-here" // SplitHostPort fails → raw RemoteAddr
	assert.Equal(t, "no-port-here", clientIP(r))
}

func TestConnectorLifecycle(t *testing.T) {
	c := New(Settings{Room: "console"}, nil, agent.NewEventBus(nil), newVerifier(t, testKey))
	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, "web", c.Name())
	assert.True(t, c.ClaimSupervision())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}

	require.NoError(t, c.Stop(context.Background()))
	require.NoError(t, c.Stop(context.Background()), "Stop is idempotent")
}
