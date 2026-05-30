package web_test

import (
	"context"
	"encoding/json"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"errors"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/web"
)

// --- test doubles ----------------------------------------------------------

type fakeBridge struct {
	nick     string
	state    *irc.ChannelState
	machine  *irc.UpstreamStateMachine
	sentMu   sync.Mutex
	sent     []string
	limits   irc.ClientLimits
	throttle *irc.Throttle
}

func newFakeBridge(nick string) *fakeBridge {
	m := irc.NewUpstreamStateMachine(nil)
	// Default to registered so existing tests that don't care about
	// state machine behavior still pass — they implicitly assume the
	// gateway lets `say` ops through.
	m.Transition(irc.UpstreamStateConnecting)
	m.Transition(irc.UpstreamStateRegistering)
	m.Transition(irc.UpstreamStateRegistered)
	return &fakeBridge{nick: nick, state: irc.NewChannelState(), machine: m}
}
func (f *fakeBridge) CurrentNick() string                      { return f.nick }
func (f *fakeBridge) State() *irc.ChannelState                 { return f.state }
func (f *fakeBridge) ClientLimits() irc.ClientLimits           { return f.limits }
func (f *fakeBridge) OutboundThrottle() *irc.Throttle          { return f.throttle }
func (f *fakeBridge) UpstreamState() *irc.UpstreamStateMachine { return f.machine }
func (f *fakeBridge) SendRaw(line string) error {
	f.sentMu.Lock()
	defer f.sentMu.Unlock()
	f.sent = append(f.sent, line)
	return nil
}
func (f *fakeBridge) Sent() []string {
	f.sentMu.Lock()
	defer f.sentMu.Unlock()
	out := make([]string, len(f.sent))
	copy(out, f.sent)
	return out
}

type fakeSender struct {
	mu      sync.Mutex
	sent    []*agent.OutboundEnvelope
	sendErr error // non-nil = every Send call returns this
}

func (s *fakeSender) Send(env *agent.OutboundEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, env)
	return nil
}

func (s *fakeSender) Outbound() []*agent.OutboundEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*agent.OutboundEnvelope, len(s.sent))
	copy(out, s.sent)
	return out
}

type testLLM struct {
	response string
}

func (t *testLLM) Model() string { return "test" }
func (t *testLLM) Ask(_ context.Context, _ string, _ ...llm.CallOption) (string, error) {
	return t.response, nil
}
func (t *testLLM) Stream(_ context.Context, _ string, _ ...llm.CallOption) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {}
}

type testLLMErr struct {
	err error
}

func (t *testLLMErr) Model() string { return "test" }
func (t *testLLMErr) Ask(_ context.Context, _ string, _ ...llm.CallOption) (string, error) {
	return "", t.err
}
func (t *testLLMErr) Stream(_ context.Context, _ string, _ ...llm.CallOption) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {}
}

// --- helpers ---------------------------------------------------------------

// startGateway boots a Gateway on a random port and returns it plus a
// teardown. The handler runs on a real net.Listener so the WS upgrade
// path is identical to production.
func startGateway(t *testing.T, opts web.Options, bridge *fakeBridge, sender *fakeSender) (*web.Gateway, *agent.Agent, func()) {
	t.Helper()
	a := agent.New(nil)
	g, err := web.New(bridge, sender, opts)
	require.NoError(t, err)
	g.Subscribe(a.Events)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Serve(ctx) }()

	require.Eventually(t, func() bool { return g.Addr() != "" }, time.Second, 5*time.Millisecond)

	return g, a, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("gateway did not shut down")
		}
	}
}

func dialWS(t *testing.T, addr, token string) *websocket.Conn {
	t.Helper()
	url := "ws://" + addr + "/ws?token=" + token
	conn, _, err := websocket.Dial(context.Background(), url, nil)
	require.NoError(t, err)
	return conn
}

func readJSON(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, body, err := conn.Read(ctx)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

// drainInitialFrames consumes the gateway's standard attach burst —
// currently `state` followed by `connector.state_changed` — so tests
// can land on the first event their assertions actually care about.
// Centralised here so adding/removing initial frames is a one-line
// change instead of touching every test that opens a WS.
func drainInitialFrames(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_ = readJSON(t, conn) // state
	_ = readJSON(t, conn) // connector.state_changed (sendInitialConnectorState)
}

func newOptions(t *testing.T, password string) web.Options {
	v, err := web.NewStaticPasswordVerifier(password)
	require.NoError(t, err)
	// Every test gets a fresh in-memory store so attach replay +
	// history scrollback have something to read from. Tests that
	// need to seed/inspect the store can read MessageStore back via
	// type assertion on *messages.MemoryStore.
	return web.Options{
		Host:         "127.0.0.1",
		Port:         0,
		Verifier:     v,
		MessageStore: messages.NewMemoryStore(0),
	}
}

// --- construction ----------------------------------------------------------

func TestNewRequiresEssentials(t *testing.T) {
	v, _ := web.NewStaticPasswordVerifier("p")
	_, err := web.New(nil, &fakeSender{}, web.Options{Verifier: v})
	require.Error(t, err)
	_, err = web.New(newFakeBridge("n"), nil, web.Options{Verifier: v})
	require.Error(t, err)
	_, err = web.New(newFakeBridge("n"), &fakeSender{}, web.Options{})
	require.Error(t, err)
}

// --- /health + /metrics ---------------------------------------------------

func TestHealthEndpoint(t *testing.T) {
	g, _, td := startGateway(t, newOptions(t, "p"), newFakeBridge("turborg"), &fakeSender{})
	defer td()

	resp, err := http.Get("http://" + g.Addr() + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "ok", out["status"])
}

func TestMetricsEndpoint(t *testing.T) {
	g, _, td := startGateway(t, newOptions(t, "p"), newFakeBridge("turborg"), &fakeSender{})
	defer td()

	resp, err := http.Get("http://" + g.Addr() + "/metrics")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(body)
	for _, need := range []string{
		"turborg_ws_connections_total",
		"turborg_ws_auth_failures_total",
		"turborg_messages_forwarded_total",
		"turborg_ws_clients_current",
		"turborg_uptime_seconds",
	} {
		assert.Contains(t, out, need, "metrics must expose %s", need)
	}
}

// --- static UI -----------------------------------------------------------

func TestStaticUIServedAtRoot(t *testing.T) {
	g, _, td := startGateway(t, newOptions(t, "p"), newFakeBridge("turborg"), &fakeSender{})
	defer td()

	resp, err := http.Get("http://" + g.Addr() + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "<!doctype html>",
		"bundled reference UI must serve at /")
}

// --- auth ---------------------------------------------------------------

func TestWSAuthRejectsBadToken(t *testing.T) {
	g, _, td := startGateway(t, newOptions(t, "right-secret"), newFakeBridge("turborg"), &fakeSender{})
	defer td()

	url := "ws://" + g.Addr() + "/ws?token=wrong"
	_, resp, err := websocket.Dial(context.Background(), url, nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestWSAuthAcceptsBearerHeader(t *testing.T) {
	g, _, td := startGateway(t, newOptions(t, "secret"), newFakeBridge("turborg"), &fakeSender{})
	defer td()

	url := "ws://" + g.Addr() + "/ws"
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer secret"}},
	})
	require.NoError(t, err)
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

func TestWSAttachHookFiresOnSuccessfulHandshake(t *testing.T) {
	var (
		mu      sync.Mutex
		reasons []string
	)
	opts := newOptions(t, "secret")
	opts.OnClientAttached = func(reason string) {
		mu.Lock()
		reasons = append(reasons, reason)
		mu.Unlock()
	}
	g, _, td := startGateway(t, opts, newFakeBridge("turborg"), &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "secret")
	defer conn.Close(websocket.StatusNormalClosure, "")

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reasons) == 1 && reasons[0] == "ws_attach"
	}, time.Second, 10*time.Millisecond)
}

func TestWSAttachHookSilentOnAuthFailure(t *testing.T) {
	var fired atomic.Bool
	opts := newOptions(t, "right-secret")
	opts.OnClientAttached = func(_ string) { fired.Store(true) }
	g, _, td := startGateway(t, opts, newFakeBridge("turborg"), &fakeSender{})
	defer td()

	url := "ws://" + g.Addr() + "/ws?token=wrong"
	_, resp, err := websocket.Dial(context.Background(), url, nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	// Give the auth-fail path enough time that any errant hook would fire.
	time.Sleep(50 * time.Millisecond)
	assert.False(t, fired.Load(), "auth failure must not fire the attach hook")
}

// --- state op on connect -----------------------------------------------

func TestWSSendsStateOnConnect(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#test")
	bridge.state.SetTopic("#test", "hello")
	bridge.state.OnNamesReply("#test", []string{"@turborg", "alice"})

	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")

	msg := readJSON(t, conn)
	assert.Equal(t, "state", msg["op"])
	assert.Equal(t, "turborg", msg["nick"])
	channels := msg["channels"].([]any)
	require.Len(t, channels, 1)
	ch := channels[0].(map[string]any)
	assert.Equal(t, "#test", ch["name"])
	assert.Equal(t, "hello", ch["topic"])
}

// --- broadcast on EventBus: every handler -----------------------------

func TestEveryEventBusHandlerBroadcastsAnOp(t *testing.T) {
	cases := []struct {
		name   string
		event  agent.Event
		wantOp string
		check  func(t *testing.T, got map[string]any)
	}{
		{"USER_LEAVE → part",
			agent.Event{Type: agent.EventUserLeave, Fields: map[string]any{"channel": "#x", "nick": "alice", "reason": "bye"}},
			"part",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "bye", got["reason"]) },
		},
		{"USER_KICKED → kick",
			agent.Event{Type: agent.EventUserKicked, Fields: map[string]any{"channel": "#x", "nick": "alice", "by": "op", "reason": "spam"}},
			"kick",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "op", got["kicker"]) },
		},
		{"USER_NICK_CHANGE → nick",
			agent.Event{Type: agent.EventUserNickChange, Fields: map[string]any{"old": "alice", "new": "alice2"}},
			"nick",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "alice2", got["new"]) },
		},
		{"TOPIC_CHANGED → topic",
			agent.Event{Type: agent.EventTopicChanged, Fields: map[string]any{"channel": "#x", "topic": "new", "by": "op"}},
			"topic",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "new", got["topic"]) },
		},
		{"CHANNEL_NAMES → names",
			agent.Event{Type: agent.EventChannelNames, Fields: map[string]any{"channel": "#x", "members": []map[string]string{{"nick": "alice", "mode": ""}}}},
			"names",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "#x", got["channel"]) },
		},
		{"MODE_CHANGED → mode_changed",
			agent.Event{Type: agent.EventModeChanged, Fields: map[string]any{"channel": "#x", "modes": "+o", "args": "alice", "by": "op"}},
			"mode_changed",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "+o", got["modes"]) },
		},
		{"WHOIS_RESULT → whois_result",
			agent.Event{Type: agent.EventWhoisResult, Fields: map[string]any{"nick": "alice", "host": "1.2.3.4"}},
			"whois_result",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "alice", got["nick"]) },
		},
		{"LIST_RESULT → list_result",
			agent.Event{Type: agent.EventListResult, Fields: map[string]any{"channels": []any{}}},
			"list_result", nil,
		},
		{"WHO_RESULT → who_result",
			agent.Event{Type: agent.EventWhoResult, Fields: map[string]any{"target": "#x", "users": []any{}}},
			"who_result",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "#x", got["target"]) },
		},
		{"JOIN_FAILED → channel.rejoin_failed",
			agent.Event{Type: agent.EventJoinFailed, Fields: map[string]any{"channel": "#x", "code": "474", "reason": "banned"}},
			"channel.rejoin_failed",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "banned", got["reason"]) },
		},
		{"MESSAGE_SENT → message",
			agent.Event{Type: agent.EventMessageSent, Fields: map[string]any{"channel": "#x", "sender": "turborg", "text": "echo"}},
			"message",
			func(t *testing.T, got map[string]any) { assert.Equal(t, "echo", got["text"]) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bridge := newFakeBridge("turborg")
			g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
			defer td()

			conn := dialWS(t, g.Addr(), "p")
			defer conn.Close(websocket.StatusNormalClosure, "")
			drainInitialFrames(t, conn)

			a.Events.Publish(context.Background(), &tc.event)
			got := readJSON(t, conn)
			assert.Equal(t, tc.wantOp, got["op"])
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}

// --- broadcast on EventBus -------------------------------------------

func TestEventBroadcastReachesClients(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	a.Events.Publish(context.Background(), &agent.Event{
		Type:   agent.EventUserJoin,
		Fields: map[string]any{"channel": "#test", "nick": "alice"},
	})

	got := readJSON(t, conn)
	assert.Equal(t, "join", got["op"])
	assert.Equal(t, "#test", got["channel"])
	assert.Equal(t, "alice", got["nick"])
}

func TestMessageEventCarriesText(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	a.Events.Publish(context.Background(), &agent.Event{
		Type: agent.EventMessage,
		Fields: map[string]any{
			"channel": "#test", "sender": "alice", "text": "hi",
		},
	})

	got := readJSON(t, conn)
	assert.Equal(t, "message", got["op"])
	assert.Equal(t, "hi", got["text"])
	assert.Equal(t, "alice", got["nick"])
}

// --- inbound ops dispatch -------------------------------------------

func TestInboundSayCallsSender(t *testing.T) {
	bridge := newFakeBridge("turborg")
	sender := &fakeSender{}
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, sender)
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	payload, _ := json.Marshal(map[string]any{
		"op": "say", "channel": "#test", "text": "hello via ws",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, payload))

	require.Eventually(t, func() bool { return len(sender.Outbound()) == 1 },
		time.Second, 10*time.Millisecond)

	out := sender.Outbound()[0]
	assert.Equal(t, "#test", out.Channel)
	assert.Equal(t, "hello via ws", out.Text)
}

func TestInboundSayPublishesMessageSentBackToSender(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "say", "channel": "#test", "text": "echo to self",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "message", got["op"], "say must produce a message op back to the sender")
	assert.Equal(t, "#test", got["channel"])
	assert.Equal(t, "turborg", got["nick"], "sender must be the bot's current nick")
	assert.Equal(t, "echo to self", got["text"])
}

func TestMessageSentFromAgentDispatchRendersCorrectly(t *testing.T) {
	// Regression: Agent.handle publishes EventMessageSent with the
	// envelope-only Fields shape. Earlier the gateway only looked at
	// channel/sender/text, dropping the message on the floor when a
	// command reply (!ping → pong) flowed through. This test pretends
	// to be agent.handle and verifies the WS broadcast has the
	// channel + nick + text the UI needs to render.
	bridge := newFakeBridge("bot")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	out := &agent.OutboundEnvelope{
		Connector: "irc",
		Channel:   "alice",
		Text:      "pong",
	}
	a.Events.Publish(context.Background(), &agent.Event{
		Type:   agent.EventMessageSent,
		Fields: map[string]any{"envelope": out},
	})

	got := readJSON(t, conn)
	assert.Equal(t, "message", got["op"])
	assert.Equal(t, "alice", got["channel"], "channel must come from the envelope")
	assert.Equal(t, "pong", got["text"], "text must come from the envelope")
	assert.Equal(t, "bot", got["nick"], "sender falls back to bridge.CurrentNick when the publisher didn't pass one")
}

func TestInboundSayUsesCurrentNickAfterChange(t *testing.T) {
	bridge := newFakeBridge("orig")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	// Bot renames itself. In production, irc.Connector.setCurrentNick
	// fires from the 001 welcome or an observed self-NICK; here we
	// flip the fake bridge directly to isolate the test.
	bridge.nick = "renamed"

	body, _ := json.Marshal(map[string]any{"op": "say", "channel": "#x", "text": "after rename"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "renamed", got["nick"],
		"say after nick change must echo with the new nick, not the stale one")
}

func TestInboundJoinPartNickKickWhois(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, conn)

	cases := []struct {
		payload map[string]any
		want    string
	}{
		{map[string]any{"op": "join", "channel": "#x"}, "JOIN #x"},
		{map[string]any{"op": "part", "channel": "#x"}, "PART #x"},
		{map[string]any{"op": "nick", "nick": "newnick"}, "NICK newnick"},
		{map[string]any{"op": "kick", "channel": "#x", "nick": "alice", "reason": "no"}, "KICK #x alice :no"},
		{map[string]any{"op": "whois", "nick": "alice"}, "WHOIS alice"},
		{map[string]any{"op": "mode", "channel": "#x", "modes": "+o", "target": "alice"}, "MODE #x +o alice"},
		{map[string]any{"op": "topic", "channel": "#x", "topic": "new"}, "TOPIC #x :new"},
		{map[string]any{"op": "topic", "channel": "#x"}, "TOPIC #x"},
		{map[string]any{"op": "list", "pattern": ""}, "LIST"},
		{map[string]any{"op": "list", "pattern": "#*"}, "LIST #*"},
		{map[string]any{"op": "who", "target": "#x"}, "WHO #x"},
		{map[string]any{"op": "raw", "line": "/PING server"}, "PING server"},
	}
	for _, tc := range cases {
		body, _ := json.Marshal(tc.payload)
		require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	}

	require.Eventually(t, func() bool { return len(bridge.Sent()) == len(cases) },
		time.Second, 10*time.Millisecond)
	got := bridge.Sent()
	for i, tc := range cases {
		assert.Equal(t, tc.want, got[i], "case %d (%v)", i, tc.payload)
	}
}

func TestInboundRawRejectsNewlines(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "raw", "line": "PRIVMSG #x :line1\r\nQUIT",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, bridge.Sent(), "raw lines with CR/LF must be dropped")
}

// --- replay buffer -------------------------------------------------

func TestServerNoticesReplayedOnConnect(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	// Publish a notice BEFORE any client connects.
	a.Events.Publish(context.Background(), &agent.Event{
		Type:   agent.EventServerNotice,
		Fields: map[string]any{"text": "buffered MOTD line", "kind": "info"},
	})

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	got := readJSON(t, conn)
	assert.Equal(t, "server", got["op"])
	assert.Equal(t, "buffered MOTD line", got["text"])
	assert.Equal(t, true, got["replayed"], "replayed flag must be set")
}

// --- idle shutdown -------------------------------------------------

func TestIdleShutdownFiresWhenLastClientLeaves(t *testing.T) {
	bridge := newFakeBridge("turborg")
	var fired atomic.Bool
	opts := newOptions(t, "p")
	opts.IdleShutdownSeconds = 1
	opts.OnIdleShutdown = func() { fired.Store(true) }
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	_ = readJSON(t, conn)
	_ = conn.Close(websocket.StatusNormalClosure, "")

	require.Eventually(t, fired.Load, 3*time.Second, 50*time.Millisecond,
		"idle callback should fire after the configured window")
}

func TestIdleShutdownCancelsOnReconnect(t *testing.T) {
	bridge := newFakeBridge("turborg")
	var fired atomic.Bool
	opts := newOptions(t, "p")
	opts.IdleShutdownSeconds = 1
	opts.OnIdleShutdown = func() { fired.Store(true) }
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	c1 := dialWS(t, g.Addr(), "p")
	_ = readJSON(t, c1)
	_ = c1.Close(websocket.StatusNormalClosure, "")

	// Reconnect inside the window so the timer should be cancelled.
	time.Sleep(200 * time.Millisecond)
	c2 := dialWS(t, g.Addr(), "p")
	defer c2.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, c2)

	time.Sleep(1500 * time.Millisecond)
	assert.False(t, fired.Load(), "callback must NOT fire when a client reconnects in time")
}

// --- token extraction ---------------------------------------------

func TestTokenExtractorPrefersQuery(t *testing.T) {
	v, _ := web.NewStaticPasswordVerifier("from-query")
	g, _, td := startGateway(t, web.Options{Host: "127.0.0.1", Verifier: v},
		newFakeBridge("turborg"), &fakeSender{})
	defer td()

	url := "ws://" + g.Addr() + "/ws?token=from-query"
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer wrong"}},
	})
	require.NoError(t, err, "query token should win over Bearer header")
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

// --- malformed inbound silently dropped ------------------------

func TestInboundIgnoresNonJSON(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, conn)

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, []byte("not json")))
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, bridge.Sent())
}

// --- /health responds quickly under load ---------------------

func TestHealthAvailableWithActiveClients(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	c := dialWS(t, g.Addr(), "p")
	defer c.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, c)

	resp, err := http.Get("http://" + g.Addr() + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), `"ws_clients":1`,
		"health must report current client count: %s", body)
}

// --- Stop method ---------------------------------------------

func TestStopUnblocksServe(t *testing.T) {
	g, _, td := startGateway(t, newOptions(t, "p"), newFakeBridge("turborg"), &fakeSender{})
	td() // teardown already calls cancel; double-stop should be safe
	g.Stop()
}

// --- httptest sanity (catch HTTP method-not-allowed regressions) ---

func TestHealthDoesNotRequireAuth(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	resp, err := httpGet(t, "http://"+g.Addr()+"/health", "")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func httpGet(t *testing.T, url, bearer string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	return http.DefaultClient.Do(req)
}

// --- ensure httptest harness used somewhere so import isn't dead

func TestHTTPTestHarnessAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "ok", strings.TrimSpace(string(body)))
}

// --- rate limiter wiring ---------------------------------------------

func TestRateLimiterLocksOutRepeatedBadTokens(t *testing.T) {
	bridge := newFakeBridge("turborg")
	rl, err := irc.NewRateLimiter(2, time.Minute, time.Minute, nil)
	require.NoError(t, err)
	opts := newOptions(t, "right")
	opts.RateLimiter = rl

	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	// Two failures lock the IP. The third attempt — even with the right
	// token — gets blocked by IsLocked before Verify runs.
	for i := 0; i < 2; i++ {
		url := "ws://" + g.Addr() + "/ws?token=wrong"
		_, resp, err := websocket.Dial(context.Background(), url, nil)
		require.Error(t, err)
		require.NotNil(t, resp)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	}
	url := "ws://" + g.Addr() + "/ws?token=right"
	_, resp, err := websocket.Dial(context.Background(), url, nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode,
		"locked IPs must be rejected with 429 before Verify runs")
}

func TestRateLimiterResetsOnSuccessfulAuth(t *testing.T) {
	bridge := newFakeBridge("turborg")
	rl, err := irc.NewRateLimiter(3, time.Minute, time.Minute, nil)
	require.NoError(t, err)
	opts := newOptions(t, "right")
	opts.RateLimiter = rl

	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	// Two failures + one success — the success should call RecordSuccess
	// and clear the failure tally. We can't peek inside, but we can
	// verify the next bad attempt does NOT immediately lock out.
	for i := 0; i < 2; i++ {
		_, resp, err := websocket.Dial(context.Background(),
			"ws://"+g.Addr()+"/ws?token=wrong", nil)
		require.Error(t, err)
		_ = resp.Body.Close()
	}
	conn := dialWS(t, g.Addr(), "right")
	drainInitialFrames(t, conn)
	_ = conn.Close(websocket.StatusNormalClosure, "")

	// A single bad attempt after the reset must NOT 429 — it must 401.
	_, resp, err := websocket.Dial(context.Background(),
		"ws://"+g.Addr()+"/ws?token=wrong", nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"a successful auth should reset the failure tally")
}

// --- channel-log replay ordering ------------------------------------

func TestChannelMessagesReplayedInTSOrder(t *testing.T) {
	bridge := newFakeBridge("turborg")
	// replayBuffers iterates over JoinedChannels — seed the bridge
	// state so #x is in the list, otherwise the channel-history
	// replay loop never visits it.
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	// Seed the MessageStore directly. In production, runtime's
	// makeStoreSubmitter writes here on every EventMessage / SENT
	// — the gateway is broadcast-only now. The test exercises the
	// gateway's replay path, so it bypasses the agent EventBus and
	// stages the store contents to assert ordering.
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	for i, text := range []string{"first", "second", "third"} {
		require.NoError(t, opts.MessageStore.Submit(context.Background(), messages.Message{
			Channel: "#x", Nick: "alice", Text: text,
			TS: base.Add(time.Duration(i) * time.Millisecond),
		}))
	}

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	got := []string{}
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, body, err := conn.Read(ctx)
		cancel()
		if err != nil {
			break
		}
		var msg map[string]any
		if err := json.Unmarshal(body, &msg); err != nil {
			continue
		}
		if msg["op"] == "message" && msg["replayed"] == true {
			got = append(got, msg["text"].(string))
		}
	}
	assert.Equal(t, []string{"first", "second", "third"}, got,
		"channel replay must come back in original publish order")
}

// --- history op (scrollback) ----------------------------------------

func TestHistoryOpReturnsOlderMessagesFromStore(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	// Seed 5 messages; ask for the 3 older than the most recent.
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	for i, text := range []string{"m1", "m2", "m3", "m4", "m5"} {
		require.NoError(t, store.Submit(context.Background(), messages.Message{
			Channel: "#x", Nick: "alice", Text: text,
			TS: base.Add(time.Duration(i) * time.Second),
		}))
	}

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)
	// Drain the initial replay frames (5 of them) before the history op.
	for i := 0; i < 5; i++ {
		_ = readJSON(t, conn)
	}

	// Ask for everything strictly before m5's ts.
	req := map[string]any{
		"op":      "history",
		"channel": "#x",
		"before":  base.Add(4 * time.Second).Format(time.RFC3339Nano),
		"limit":   10,
	}
	body, _ := json.Marshal(req)
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "history_result", got["op"])
	assert.Equal(t, "#x", got["channel"])
	msgs, ok := got["messages"].([]any)
	require.True(t, ok, "messages must be an array, got %T", got["messages"])
	require.Len(t, msgs, 4, "expected m1..m4 older than m5")
	first := msgs[0].(map[string]any)
	// Newest-first within the response.
	assert.Equal(t, "m4", first["text"])
	assert.Equal(t, false, got["has_more"], "5 < limit=10, no more pages")
}

func TestHistoryOpRespectsLimitAndReportsHasMore(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	for i := 0; i < 5; i++ {
		require.NoError(t, store.Submit(context.Background(), messages.Message{
			Channel: "#x", Nick: "alice", Text: "m",
			TS: time.Now().Add(-time.Duration(5-i) * time.Second),
		}))
	}

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)
	for i := 0; i < 5; i++ {
		_ = readJSON(t, conn)
	}

	body, _ := json.Marshal(map[string]any{
		"op": "history", "channel": "#x", "limit": 2,
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	got := readJSON(t, conn)
	msgs, _ := got["messages"].([]any)
	assert.Len(t, msgs, 2)
	assert.Equal(t, true, got["has_more"], "len == limit signals more available")
}

func TestHistoryOpWithNilStoreReturnsEmpty(t *testing.T) {
	// Gateway without a MessageStore: history op must still answer
	// with a valid history_result frame (empty messages, has_more
	// false) so the UI's "loading older messages" spinner clears.
	v, err := web.NewStaticPasswordVerifier("p")
	require.NoError(t, err)
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, web.Options{
		Host: "127.0.0.1", Port: 0, Verifier: v,
		// MessageStore intentionally nil.
	}, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "history", "channel": "#x", "limit": 10,
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	got := readJSON(t, conn)
	assert.Equal(t, "history_result", got["op"])
	msgs, _ := got["messages"].([]any)
	assert.Empty(t, msgs)
	assert.Equal(t, false, got["has_more"])
}

func TestHistoryOpAcceptsTimestampWithMillisFormat(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.Submit(context.Background(), messages.Message{
		Channel: "#x", Nick: "a", Text: "older", TS: base,
	}))

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)
	_ = readJSON(t, conn) // initial replay

	// `before` in the .000Z variant — the gateway's parser falls
	// through to that format when RFC3339Nano doesn't match.
	body, _ := json.Marshal(map[string]any{
		"op": "history", "channel": "#x",
		"before": "2026-05-21T12:00:10.000Z",
		"limit":  10,
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	got := readJSON(t, conn)
	msgs, _ := got["messages"].([]any)
	require.Len(t, msgs, 1)
}

// failingStore satisfies messages.Store with a Recent that errors.
// Used to exercise the gateway's error-degraded response on the
// history op (which must still emit a valid history_result frame so
// the UI's loading state clears).
type failingStore struct{}

func (failingStore) Submit(context.Context, messages.Message) error { return nil }
func (failingStore) Recent(context.Context, string, time.Time, int) ([]messages.Message, error) {
	return nil, errFake
}

var errFake = errors.New("fake store error")

func TestReplayBuffersStoreErrorIsSwallowed(t *testing.T) {
	// On attach, store.Recent failures must NOT block the gateway —
	// the client still gets `state` + initial connector state and
	// can continue normally. Verifies the error-degrade branch in
	// replayBuffers.
	v, err := web.NewStaticPasswordVerifier("p")
	require.NoError(t, err)
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	g, _, td := startGateway(t, web.Options{
		Host: "127.0.0.1", Port: 0, Verifier: v,
		MessageStore: failingStore{},
	}, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	// Must successfully read state + connector.state_changed despite
	// the store error — proves the gateway didn't hang or close.
	drainInitialFrames(t, conn)
}

func TestHistoryOpStoreErrorReturnsEmpty(t *testing.T) {
	v, err := web.NewStaticPasswordVerifier("p")
	require.NoError(t, err)
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, web.Options{
		Host: "127.0.0.1", Port: 0, Verifier: v,
		MessageStore: failingStore{},
	}, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{"op": "history", "channel": "#x", "limit": 10})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	got := readJSON(t, conn)
	assert.Equal(t, "history_result", got["op"])
	msgs, _ := got["messages"].([]any)
	assert.Empty(t, msgs, "error path must return empty messages, not panic")
	assert.Equal(t, false, got["has_more"])
}

func TestHistoryOpRejectsNonChannelTarget(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "history", "channel": "alice",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	// No response is expected for a malformed op — the gateway
	// silently drops it. Verify by polling with a short deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, err := conn.Read(ctx)
	assert.Error(t, err, "non-channel target must not produce a history_result")
}

// --- /tb ops ---------------------------------------------------------

func TestTBOpSummarizeNoProvider(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "No LLM provider")
}

func TestTBOpSummarizeSuccess(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	for i, text := range []string{"hello", "world", "test"} {
		_ = store.Submit(context.Background(), messages.Message{
			Channel: "#x", Nick: "alice", Text: text,
			TS: time.Now().Add(time.Duration(i) * time.Second),
		})
	}
	opts.LLMProvider = &testLLM{response: "  chat summary  "}
	opts.TBSummarizeMaxMessages = 200
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)
	// Drain the initial replay (3 messages).
	for range 3 {
		_ = readJSON(t, conn)
	}

	// Send with explicit n > cap to exercise both the n-parsing and n>cap branches.
	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x", "n": float64(500),
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	// First frame: tb_status.
	status := readJSON(t, conn)
	assert.Equal(t, "tb_status", status["op"])

	// Second frame: tb_result.
	result := readJSON(t, conn)
	assert.Equal(t, "tb_result", result["op"])
	assert.Equal(t, "summarize", result["sub"])
	assert.Equal(t, "#x", result["channel"])
	assert.Equal(t, "chat summary", result["summary"])
}

func TestTBOpSummarizeZeroCap(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	opts.LLMProvider = &testLLM{response: "ok"}
	opts.TBSummarizeMaxMessages = 0
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "not available on your plan")
}

func TestTBOpUnknownSubcommand(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "bogus", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "Unknown")
}

func TestTBOpSummarizeNoStore(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	opts.MessageStore = nil
	opts.LLMProvider = &testLLM{response: "ok"}
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

	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "No message history")
}

func TestTBOpSummarizeEmptyChannel(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#empty")
	opts := newOptions(t, "p")
	opts.LLMProvider = &testLLM{response: "ok"}
	opts.TBSummarizeMaxMessages = 200
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#empty",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	// Status frame first.
	status := readJSON(t, conn)
	assert.Equal(t, "tb_status", status["op"])

	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "No messages")
}

func TestTBOpSummarizePersistsToStore(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	_ = store.Submit(context.Background(), messages.Message{
		Channel: "#x", Nick: "alice", Text: "hello", TS: time.Now(),
	})
	opts.LLMProvider = &testLLM{response: "great chat"}
	opts.TBSummarizeMaxMessages = 200
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)
	_ = readJSON(t, conn) // replay

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	_ = readJSON(t, conn) // tb_status
	_ = readJSON(t, conn) // tb_result

	// The summary should be persisted in the store.
	require.Eventually(t, func() bool {
		return store.Len("#x") == 2
	}, 2*time.Second, 50*time.Millisecond, "summary should be stored")
}

func TestTBOpSummarizeLLMError(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	_ = store.Submit(context.Background(), messages.Message{
		Channel: "#x", Nick: "alice", Text: "hello", TS: time.Now(),
	})
	opts.LLMProvider = &testLLMErr{err: errors.New("api down")}
	opts.TBSummarizeMaxMessages = 200
	g, _, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)
	_ = readJSON(t, conn) // replay

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize", "channel": "#x",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	_ = readJSON(t, conn) // tb_status
	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "LLM request failed")
}

func TestTBOpSummarizeMissingChannel(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "tb", "sub": "summarize",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "tb_error", got["op"])
	assert.Contains(t, got["message"], "Channel is required")
}

// --- sendTo error path: closed conn is removed from clients ------

func TestBroadcastDropsClientOnClosedConn(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	drainInitialFrames(t, conn)

	// Close from the client side without notice; the gateway's next
	// broadcast.sendTo write will fail and remove the client.
	_ = conn.CloseNow()

	// Publish enough events to ensure at least one write attempt lands
	// after the close.
	require.Eventually(t, func() bool {
		a.Events.Publish(context.Background(), &agent.Event{
			Type:   agent.EventServerNotice,
			Fields: map[string]any{"text": "tick", "kind": "info"},
		})
		// /health reports the live client count; once sendTo's error
		// branch fires the count drops to 0.
		resp, err := http.Get("http://" + g.Addr() + "/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return strings.Contains(string(body), `"ws_clients":0`)
	}, 2*time.Second, 50*time.Millisecond,
		"broken-write client must be removed from the gateway's client set")
}

// --- inbound dispatch: no-op edges ---------------------------------

func TestInboundDispatchSkipsEmptyOrInvalidPayloads(t *testing.T) {
	bridge := newFakeBridge("turborg")
	sender := &fakeSender{}
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, sender)
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	// Every one of these is a syntactic no-op: required fields missing,
	// empty strings, etc. None should fan to bridge or sender.
	cases := []map[string]any{
		{"op": "say", "channel": "", "text": "x"},
		{"op": "say", "channel": "#x", "text": ""},
		{"op": "join", "channel": ""},
		{"op": "part", "channel": ""},
		{"op": "nick", "nick": ""},
		{"op": "mode", "channel": "#x"},
		{"op": "mode", "channel": "", "modes": "+o"},
		{"op": "kick", "channel": "#x"},
		{"op": "kick", "channel": "", "nick": "alice"},
		{"op": "whois", "nick": ""},
		{"op": "topic", "channel": ""},
		{"op": "who", "target": "   "},
		{"op": "raw", "line": ""},
		{"op": "unknown_op_name", "any": "thing"},
	}
	for _, p := range cases {
		body, _ := json.Marshal(p)
		require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	}
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, bridge.Sent(), "no-op payloads must not produce any SendRaw")
	assert.Empty(t, sender.Outbound(), "no-op say payloads must not call Sender")
}

func TestInboundKickWithoutReason(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "kick", "channel": "#x", "nick": "alice",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	require.Eventually(t, func() bool { return len(bridge.Sent()) == 1 },
		time.Second, 10*time.Millisecond)
	assert.Equal(t, "KICK #x alice", bridge.Sent()[0],
		"KICK without reason must NOT include the trailing colon")
}

func TestInboundModeWithoutTarget(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	_ = readJSON(t, conn)

	body, _ := json.Marshal(map[string]any{
		"op": "mode", "channel": "#x", "modes": "+t",
	})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	require.Eventually(t, func() bool { return len(bridge.Sent()) == 1 },
		time.Second, 10*time.Millisecond)
	assert.Equal(t, "MODE #x +t", bridge.Sent()[0])
}

// --- recordChannel skips messages without a channel ---------

// TestGatewayDoesNotWriteToMessageStore pins the contract: persistence
// is the runtime.makeStoreSubmitter subscriber's job; the gateway only
// broadcasts to WS clients. The previous mistake had BOTH the runtime
// submitter AND the gateway's own subscribers calling MessageStore.
// Submit on every EventMessage / EventMessageSent, landing every
// channel message in the DB twice with different msg_ids. Regression
// guard.
func TestGatewayDoesNotWriteToMessageStore(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.state.OnSelfJoin("#x")
	opts := newOptions(t, "p")
	store := opts.MessageStore.(*messages.MemoryStore)
	g, a, td := startGateway(t, opts, bridge, &fakeSender{})
	defer td()
	_ = g

	// Publish an inbound message + an outbound (SENT) message. If the
	// gateway re-introduced its old submitToStore path, MessageStore
	// would now contain entries; runtime's makeStoreSubmitter is the
	// only canonical writer and the test setup doesn't wire it.
	a.Events.Publish(context.Background(), &agent.Event{
		Type: agent.EventMessage,
		Fields: map[string]any{
			"channel": "#x", "sender": "alice", "text": "hi",
		},
	})
	a.Events.Publish(context.Background(), &agent.Event{
		Type: agent.EventMessageSent,
		Fields: map[string]any{
			"channel": "#x", "sender": "turborg", "text": "pong",
		},
	})

	// Allow the EventBus delivery goroutine to drain.
	time.Sleep(50 * time.Millisecond)

	got, err := store.Recent(context.Background(), "#x", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Empty(t, got,
		"gateway must NOT persist to MessageStore — that's runtime.makeStoreSubmitter's job. "+
			"If this fires, the gateway has re-introduced its own writer and the DB now gets every "+
			"message twice with different msg_ids.")
}

func TestOnMessageReadsEnvelopeFieldsWhenPresent(t *testing.T) {
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	// Publish an EventMessage with an envelope (not channel/sender/
	// text fields). The gateway must read from env.* — covers the
	// `if env != nil` branch in onMessage.
	env := &agent.InboundEnvelope{
		Connector: "irc",
		Channel:   "#fromenv",
		Sender:    "envbob",
		Text:      "via envelope",
	}
	a.Events.Publish(context.Background(), &agent.Event{
		Type:   agent.EventMessage,
		Fields: map[string]any{"envelope": env},
	})
	got := readJSON(t, conn)
	assert.Equal(t, "message", got["op"])
	assert.Equal(t, "#fromenv", got["channel"], "channel must come from the envelope")
	assert.Equal(t, "envbob", got["nick"], "sender must come from the envelope")
	assert.Equal(t, "via envelope", got["text"])
}

func TestRecordChannelTrimsBufferAtCap(t *testing.T) {
	// Push more than channelLogCap (200) messages and reconnect — the
	// replay must come back capped at 200, exercising the trim branch
	// in recordChannel.
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	for i := 0; i < 220; i++ {
		a.Events.Publish(context.Background(), &agent.Event{
			Type: agent.EventMessage,
			Fields: map[string]any{
				"channel": "#busy", "sender": "alice", "text": "spam",
			},
		})
	}
	// Allow the EventBus to drain before connecting.
	time.Sleep(50 * time.Millisecond)

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	replayed := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && replayed < 230 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, _, err := conn.Read(ctx)
		cancel()
		if err != nil {
			break
		}
		replayed++
	}
	assert.LessOrEqual(t, replayed, 200,
		"channel log must be capped at channelLogCap (200) by the trim branch")
}

func TestServerNoticeBufferTrimsAtCap(t *testing.T) {
	// Push more than serverLogCap (100) notices and verify the buffer
	// stays bounded; we observe via the replay frame count.
	bridge := newFakeBridge("turborg")
	g, a, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()

	for i := 0; i < 150; i++ {
		a.Events.Publish(context.Background(), &agent.Event{
			Type:   agent.EventServerNotice,
			Fields: map[string]any{"text": "n", "kind": "info"},
		})
	}

	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	replayed := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, _, err := conn.Read(ctx)
		cancel()
		if err != nil {
			break
		}
		replayed++
		if replayed > 120 {
			break // we've seen enough — buffer is not bounded
		}
	}
	assert.LessOrEqual(t, replayed, 100, "server log must be capped at serverLogCap")
}

// --- Operator-policy / throttle gates -------------------------------------

func TestInboundNickDeniedWhenLocked(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.limits = irc.ClientLimits{NickLocked: true}

	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{"op": "nick", "nick": "newnick"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "policy_denied", got["op"])
	assert.Equal(t, "nick", got["source_op"])
	assert.Equal(t, "newnick", got["source_target"],
		"policy_denied must echo the requested nick so the UI can render the rejection where the user typed it")
	assert.Equal(t, "nick_locked", got["kind"])
	assert.Contains(t, got["reason"].(string), "nick")

	// Nothing forwarded to the bridge.
	assert.Empty(t, bridge.Sent(),
		"NICK must not flow upstream when ClientLimits.NickLocked is set")
}

func TestInboundJoinDeniedAtChannelCap(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.limits = irc.ClientLimits{MaxChannels: 2}
	bridge.state.OnSelfJoin("#a")
	bridge.state.OnSelfJoin("#b")

	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{"op": "join", "channel": "#c"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	got := readJSON(t, conn)
	assert.Equal(t, "policy_denied", got["op"])
	assert.Equal(t, "channels", got["kind"])
	assert.Equal(t, "join", got["source_op"])
	assert.Equal(t, "#c", got["source_target"],
		"policy_denied must echo the attempted channel so the UI can render the rejection in that tab")
	assert.Contains(t, got["reason"].(string), "2")

	assert.Empty(t, bridge.Sent(),
		"JOIN must not flow upstream when MaxChannels is reached")
}

func TestInboundJoinAllowedBelowChannelCap(t *testing.T) {
	bridge := newFakeBridge("turborg")
	bridge.limits = irc.ClientLimits{MaxChannels: 5}
	bridge.state.OnSelfJoin("#a")

	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	body, _ := json.Marshal(map[string]any{"op": "join", "channel": "#b"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))

	require.Eventually(t, func() bool { return len(bridge.Sent()) == 1 },
		time.Second, 10*time.Millisecond)
	assert.Equal(t, "JOIN #b", bridge.Sent()[0])
}

func TestInboundSayRateLimitedEmitsRateLimitedEvent(t *testing.T) {
	throttle, err := irc.NewThrottle(2, 30*time.Second, nil)
	require.NoError(t, err)
	bridge := newFakeBridge("turborg")
	bridge.throttle = throttle

	sender := &fakeSender{}
	g, _, td := startGateway(t, newOptions(t, "p"), bridge, sender)
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	// First 2 sends pass the throttle and produce the MESSAGE_SENT echo.
	for i := 0; i < 2; i++ {
		body, _ := json.Marshal(map[string]any{"op": "say", "channel": "#x", "text": "hi"})
		require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
		got := readJSON(t, conn)
		require.Equal(t, "message", got["op"], "send %d should produce a message echo", i+1)
	}

	// Third send hits the cap and gets a rate_limited event back.
	body, _ := json.Marshal(map[string]any{"op": "say", "channel": "#x", "text": "hi"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, body))
	got := readJSON(t, conn)
	assert.Equal(t, "rate_limited", got["op"])
	assert.Equal(t, "#x", got["target"])
	assert.GreaterOrEqual(t, got["retry_after"].(float64), 1.0)

	// Sender saw only the first two — the throttled call short-circuits
	// before reaching agent.Sender.
	assert.Len(t, sender.Outbound(), 2)
}

func TestInboundSayPerTargetThrottleScopesIndependently(t *testing.T) {
	throttle, err := irc.NewThrottle(1, 30*time.Second, nil)
	require.NoError(t, err)
	bridge := newFakeBridge("turborg")
	bridge.throttle = throttle

	g, _, td := startGateway(t, newOptions(t, "p"), bridge, &fakeSender{})
	defer td()
	conn := dialWS(t, g.Addr(), "p")
	defer conn.Close(websocket.StatusNormalClosure, "")
	drainInitialFrames(t, conn)

	// #a uses its bucket.
	bodyA, _ := json.Marshal(map[string]any{"op": "say", "channel": "#a", "text": "hi"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, bodyA))
	require.Equal(t, "message", readJSON(t, conn)["op"])

	// #b is a different bucket — should NOT be throttled.
	bodyB, _ := json.Marshal(map[string]any{"op": "say", "channel": "#b", "text": "hi"})
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText, bodyB))
	require.Equal(t, "message", readJSON(t, conn)["op"],
		"per-target scope: chatty user pestering #a must not lock out #b")
}
