package irc

import (
	"bufio"
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/version"
)

func TestIsWellFormed(t *testing.T) {
	cases := []struct {
		name string
		msg  Message
		want bool
	}{
		{"nick with param", Message{Command: CmdNick, Params: []string{"newnick"}}, true},
		{"nick with trailing", Message{Command: CmdNick, Trailing: "newnick"}, true},
		{"nick without anything", Message{Command: CmdNick}, false},
		{"privmsg with target", Message{Command: CmdPrivmsg, Params: []string{"#ch"}, Trailing: "hi"}, true},
		{"privmsg empty target", Message{Command: CmdPrivmsg, Params: []string{""}, Trailing: "hi"}, false},
		{"join no target", Message{Command: CmdJoin}, false},
		{"unknown command always ok", Message{Command: "WHOIS"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isWellFormed(tc.msg))
		})
	}
}

func TestShouldFanOutToBouncer(t *testing.T) {
	skip := []string{"", CmdPing, CmdPong, CmdCap, CmdAuthenticate,
		RplSaslLoggedIn, RplSaslSuccess, ErrSaslFail, ErrSaslAborted,
		ErrSaslAlready, ErrSaslTooLong}
	for _, cmd := range skip {
		assert.False(t, shouldFanOutToBouncer(cmd), "%s should be filtered out", cmd)
	}
	keep := []string{CmdPrivmsg, CmdNotice, CmdJoin, CmdPart, CmdQuit, CmdKick, CmdMode, CmdTopic, RplWelcome, RplEndOfMOTD}
	for _, cmd := range keep {
		assert.True(t, shouldFanOutToBouncer(cmd), "%s should be forwarded", cmd)
	}
}

func TestStartsWithChannelSigil(t *testing.T) {
	for _, s := range []string{"#chan", "&local", "+plus", "!safe"} {
		assert.True(t, startsWithChannelSigil(s), s)
	}
	for _, s := range []string{"", "no-sigil", "$server", "0channel"} {
		assert.False(t, startsWithChannelSigil(s), s)
	}
}

func TestJoinTargetFallsBackToTrailing(t *testing.T) {
	assert.Equal(t, "#ch", joinTarget(Message{Params: []string{"#ch"}}))
	assert.Equal(t, "#ch", joinTarget(Message{Trailing: "#ch"}))
	assert.Equal(t, "", joinTarget(Message{}))
	// Empty Params[0] should still fall back to Trailing.
	assert.Equal(t, "#ch", joinTarget(Message{Params: []string{""}, Trailing: "#ch"}))
}

func TestParseUnixSeconds(t *testing.T) {
	assert.Equal(t, int64(0), parseUnixSeconds(""))
	assert.Equal(t, int64(0), parseUnixSeconds("not-numeric"))
	assert.Equal(t, int64(1700000000), parseUnixSeconds("1700000000"))
	assert.Equal(t, int64(0), parseUnixSeconds("123abc"))
}

func TestCTCPHelpers(t *testing.T) {
	assert.True(t, isCTCP("\x01VERSION\x01"))
	assert.False(t, isCTCP("plain"))
	assert.False(t, isCTCP("\x01"))
	assert.True(t, isACTION("\x01ACTION hugs\x01"))
	assert.True(t, isACTION("\x01action hugs\x01"))
	assert.False(t, isACTION("\x01VERSION\x01"))
}

func TestCTCPReplyCovers(t *testing.T) {
	cases := map[string]string{
		"\x01VERSION\x01":        "VERSION turborg " + version.Version + " (https://github.com/turborg/turborg)",
		"\x01PING 12345\x01":     "PING 12345",
		"\x01CLIENTINFO\x01":     "CLIENTINFO VERSION PING TIME CLIENTINFO SOURCE USERINFO",
		"\x01SOURCE\x01":         "SOURCE https://github.com/turborg/turborg",
		"\x01USERINFO\x01":       "USERINFO turborg agent",
		"\x01\x01":               "",  // empty inner
		"\x01UNKNOWN\x01":        "",  // unrecognized
		"\x01version lowercase\x01": "VERSION turborg " + version.Version + " (https://github.com/turborg/turborg)",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			got := ctcpReply(in)
			if want == "" {
				assert.Empty(t, got)
			} else {
				assert.Contains(t, got, want)
			}
		})
	}
	// TIME returns the current UTC RFC3339, which we can only sanity-check.
	got := ctcpReply("\x01TIME\x01")
	assert.True(t, len(got) > len("TIME 20"))
}

func TestSendWhenNotConnected(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	err := c.Send(&agent.OutboundEnvelope{Channel: "#ch", Text: "x"})
	require.Error(t, err)
}

func TestStopWithoutStartIsNoOp(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	require.NoError(t, c.Stop(context.Background()))
}

func TestRunWithoutStartReturnsError(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	err := c.Run(context.Background())
	require.Error(t, err)
}

func TestPeerIPOnNilClient(t *testing.T) {
	assert.Equal(t, "", peerIP(nil))
}

func TestPeerIPOnNilConn(t *testing.T) {
	assert.Equal(t, "", peerIP(&BouncerClient{}))
}

func TestPeerIPNonTCPFallback(t *testing.T) {
	// Our recordingConn returns a fakeAddr that is NOT *net.TCPAddr,
	// so peerIP must fall through to RemoteAddr().String() — covers
	// the non-TCP transport branch.
	bc := newBouncerClient(newRecordingConn(), slog.Default())
	assert.Equal(t, "broken:0", peerIP(bc),
		"non-TCP addr must fall back to RemoteAddr().String() as-is")
}

func TestUpstreamPrefixEmptyWhenNickUnset(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "", b.upstreamPrefix(), "no nick → empty prefix")
}

func TestNewBouncerDefaultsHost(t *testing.T) {
	b, err := NewBouncer("p", "", 0, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", b.host, "empty host must default to 127.0.0.1")
}

func TestMaskSecretsPrivmsgNickServNoColonReturnsLine(t *testing.T) {
	// PRIVMSG NickServ format without a trailing colon — unusual but
	// possible. The maskSecrets fallback must return the line as-is
	// rather than mis-redacting.
	in := "PRIVMSG NickServ HI"
	assert.Equal(t, in, maskSecrets(in))
}

func TestMaskSecretsAuthenticatePlainPassthrough(t *testing.T) {
	// AUTHENTICATE PLAIN starts SASL — no secret in this line itself.
	assert.Equal(t, "AUTHENTICATE PLAIN", maskSecrets("AUTHENTICATE PLAIN"))
}

func TestBouncerClientAddressNilConn(t *testing.T) {
	// Direct field-only construction — verifies the nil-conn guard in
	// Address() returns an empty string instead of panicking.
	bc := &BouncerClient{}
	assert.Equal(t, "", bc.Address())
}

func TestAddrBeforeStart(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "", b.Addr(), "Addr before Start is empty")
}

func TestRecordForReplayIgnoresNonChannel(t *testing.T) {
	state := NewChannelState()
	state.OnSelfJoin("#known")
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(state, "nick", "ident", "host")

	// Non-PRIVMSG/NOTICE: noop.
	b.recordForReplay("PING :x")
	// Channel-targeted to an unknown channel: noop.
	b.recordForReplay(":a!u@h PRIVMSG #unknown :hi")
	// Non-channel target (DM): noop.
	b.recordForReplay(":a!u@h PRIVMSG nick :hi")
	// Channel we track: should be captured.
	b.recordForReplay(":a!u@h PRIVMSG #known :hi")

	b.logMu.Lock()
	got := b.channelLog
	b.logMu.Unlock()
	require.Len(t, got, 1, "only the known-channel line should be captured: %v", got)
	require.Equal(t, 1, len(got["#known"]))
}

// Direct handler tests via private API to cover guard-rail branches that
// the integration tests don't naturally hit.

func TestHandlersIgnoreMalformedMessages(t *testing.T) {
	c := New(&Settings{Nick: "turborg"}, nil, nil)
	ctx := context.Background()

	// All of these should be silent no-ops on insufficient params.
	c.handlePrivmsg(ctx, Message{}, "")
	c.handlePart(ctx, Message{})
	c.handleKick(ctx, Message{Params: []string{"#ch"}}) // missing victim
	c.handleNickChange(ctx, Message{}) // no old
	c.handleTopic(ctx, Message{})
	c.handleJoin(ctx, Message{}) // empty target

	assert.Empty(t, c.state.JoinedChannels(), "no channel state should be touched by malformed input")
}

func TestHandleQuitWithoutPrefix(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	c.handleQuit(context.Background(), Message{Trailing: "bye"})
	// Nothing to assert directly; covered by lack of panic and the
	// empty-nick early return path.
}

func TestSendNotConnectedErrorPath(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	require.Error(t, c.Send(&agent.OutboundEnvelope{Channel: "#x", Text: "hi"}))
}

func TestPublishWithoutEventBusIsNoop(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	c.publish(context.Background(), agent.Event{Type: agent.EventBoot})
}

func TestStartBouncerInvalidRatelimit(t *testing.T) {
	// Negative max-failures must error from startBouncer through
	// NewRateLimiter.
	c := New(&Settings{
		BouncerPassword:             "p",
		BouncerRatelimitEnabled:     true,
		BouncerMaxFailedAttempts:    0, // invalid
		BouncerFailureWindowSeconds: 60,
		BouncerLockoutSeconds:       300,
	}, nil, nil)
	err := c.startBouncer(context.Background())
	require.Error(t, err)
}

func TestStartBouncerEmptyPassword(t *testing.T) {
	c := New(&Settings{BouncerPassword: ""}, nil, nil)
	err := c.startBouncer(context.Background())
	require.Error(t, err, "empty bouncer password must error from NewBouncer")
}

func TestDialTLSPathRoundtrips(t *testing.T) {
	// Self-signed TLS server on a random port. The handler is a no-op
	// — we just want to know Dial completes the TLS handshake and
	// returns a usable Client. The TLS-acceptance branch in dial() is
	// otherwise uncovered by the integration tests, which dial plain
	// TCP against fakeirc.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.StartTLS()
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	host, portStr, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	// httptest's TLS server uses a self-signed cert; we accept it with
	// InsecureSkipVerify since this is a test-only path. Production
	// callers go through Dial() which never sets this flag.
	insecure := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} //nolint:gosec
	client, err := dial(context.Background(), host, port, true, insecure)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NoError(t, client.Close())
}

// --- Live-nick sync ---------------------------------------------------

func TestRplWelcomeSyncsCurrentNick(t *testing.T) {
	c := New(&Settings{Nick: "requested"}, nil, nil)
	require.Equal(t, "requested", c.CurrentNick())

	c.dispatchLine(context.Background(), ":fake 001 reborn :Welcome to the Internet Relay Chat Network reborn")
	assert.Equal(t, "reborn", c.CurrentNick(),
		"001's first param is the server-assigned nick; live nick must follow")
}

func TestSelfNickChangeSyncsCurrentNickAndBouncer(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)

	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	c.bouncer = b
	b.AttachState(c.state, "bot", "user", "host")

	c.dispatchLine(context.Background(), ":bot!u@h NICK :renamed")

	assert.Equal(t, "renamed", c.CurrentNick(),
		"self-NICK observed must update CurrentNick")
	prefix := b.upstreamPrefix()
	assert.Contains(t, prefix, "renamed!",
		"setCurrentNick must propagate to the bouncer's upstream prefix")
}

func TestNonSelfNickChangeDoesNotShiftBotNick(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.dispatchLine(context.Background(), ":alice!u@h NICK :alice2")
	assert.Equal(t, "bot", c.CurrentNick(),
		"another user's NICK must not change the bot's own nick")
}

func TestSplitIdentHost(t *testing.T) {
	cases := []struct {
		in    string
		ident string
		host  string
		ok    bool
	}{
		{"bot!~user@cloak.example", "~user", "cloak.example", true},
		{"bot!u@h", "u", "h", true},
		{"server.example.net", "", "", false},
		{"bot", "", "", false},
		{"bot!u-no-at", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			ident, host, ok := splitIdentHost(tc.in)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.ident, ident)
			assert.Equal(t, tc.host, host)
		})
	}
}

func TestSelfPrefixedUpstreamLineUpdatesBouncerIdentity(t *testing.T) {
	// Regression: BroadcastAsSelf used to fan messages with a synthetic
	// "ident@turborg" prefix that didn't match HexChat's idea of its
	// own identity (the real "~ident@cloak"). Echo-message-routed
	// self-PRIVMSGs were rejected as not-mine. Now any self-prefixed
	// upstream line teaches the bouncer the real ident/host.
	c := New(&Settings{Nick: "bot"}, nil, nil)
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	c.bouncer = b
	b.AttachState(c.state, "bot", "fakeuser", "turborg")

	c.dispatchLine(context.Background(), ":bot!~realident@user/realcloak JOIN #test")

	prefix := b.upstreamPrefix()
	assert.Contains(t, prefix, "~realident@user/realcloak",
		"observed self-prefix must replace the synthetic ident@turborg fallback")
}

func TestNonSelfPrefixedLineDoesNotMoveBouncerIdentity(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	c.bouncer = b
	b.AttachState(c.state, "bot", "originaluser", "originalhost")

	c.dispatchLine(context.Background(), ":alice!~someoneelse@elsewhere.com PRIVMSG #test :hi")

	prefix := b.upstreamPrefix()
	assert.Contains(t, prefix, "originaluser@originalhost",
		"foreign-prefixed lines must not shift bouncer's idea of OUR identity")
	assert.NotContains(t, prefix, "someoneelse")
}

// --- Outbound logging + secret masking --------------------------------

type recordHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *recordHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *recordHandler) lines() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.records))
	for _, r := range h.records {
		if r.Message == "irc >>" {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "line" {
					out = append(out, a.Value.String())
				}
				return true
			})
		}
	}
	return out
}

func TestClientWriteLineLogsAtDebug(t *testing.T) {
	// Pair of net.Pipes — we only care that the write hits Logger,
	// not what's on the wire.
	server, client := net.Pipe()
	defer server.Close()

	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()

	c := &Client{conn: client, reader: bufio.NewReader(client)}
	h := &recordHandler{}
	c.SetLog(slog.New(h))

	require.NoError(t, c.WriteLine("PONG :token"))
	require.NoError(t, c.WriteLine("PRIVMSG #ch :hello"))

	lines := h.lines()
	require.Len(t, lines, 2, "every WriteLine must produce one irc >> log line")
	assert.Equal(t, "PONG :token", lines[0])
	assert.Equal(t, "PRIVMSG #ch :hello", lines[1])
}

func TestMaskSecrets(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"PONG :token", "PONG :token"},
		{"PRIVMSG #ch :hello", "PRIVMSG #ch :hello"},
		{"JOIN #ch", "JOIN #ch"},
		{"PASS hunter2", "PASS <redacted>"},
		{"pass hunter2", "PASS <redacted>"},
		{"AUTHENTICATE PLAIN", "AUTHENTICATE PLAIN"},
		{"AUTHENTICATE AGFsaWNlAGh1bnRlcjI=", "AUTHENTICATE <redacted>"},
		{"PRIVMSG NickServ :IDENTIFY hunter2", "PRIVMSG NickServ :IDENTIFY <redacted>"},
		{"PRIVMSG NickServ :REGISTER hunter2 alice@example.com", "PRIVMSG NickServ :REGISTER <redacted>"},
		{"PRIVMSG nickserv :ghost mybot hunter2", "PRIVMSG nickserv :GHOST <redacted>"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, maskSecrets(tc.in))
		})
	}
}

func TestSendRawWhenNotConnected(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	err := c.SendRaw("PRIVMSG #ch :hi")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestSendRawWhenConnectedReachesUpstream(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	received := make(chan string, 4)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := server.Read(buf)
			if err != nil {
				return
			}
			received <- string(buf[:n])
		}
	}()

	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.client = &Client{conn: client, reader: bufio.NewReader(client)}

	require.NoError(t, c.SendRaw("WHOIS alice"))

	select {
	case got := <-received:
		assert.Contains(t, got, "WHOIS alice")
	case <-time.After(time.Second):
		t.Fatal("SendRaw did not reach the upstream pipe within 1s")
	}
}

func TestSendBroadcastsThroughBouncerWhenAttached(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()

	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.client = &Client{conn: client, reader: bufio.NewReader(client)}

	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	c.bouncer = b
	b.AttachState(c.state, "bot", "user", "host")
	c.state.OnSelfJoin("#x")

	// Send must call bouncer.BroadcastAsSelf — visible via the replay buffer.
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "#x", Text: "hi"}))

	b.logMu.Lock()
	defer b.logMu.Unlock()
	require.Len(t, b.channelLog["#x"], 1,
		"Send must record self-prefixed line into the bouncer's replay buffer")
}

func TestHandlePrivmsgRoutesDMToSenderChannel(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.handlePrivmsg(context.Background(),
		Message{Prefix: "alice!u@h", Params: []string{"bot"}, Trailing: "psst"},
		":alice!u@h PRIVMSG bot :psst")

	select {
	case env := <-c.Inbound():
		assert.True(t, env.IsDirect, "DM target → IsDirect must be true")
		assert.Equal(t, "alice", env.Channel, "DM channel must be the sender's nick for replies")
		assert.Equal(t, "psst", env.Text)
	case <-time.After(time.Second):
		t.Fatal("expected inbound DM envelope")
	}
}

func TestHandlePrivmsgActionPassesThrough(t *testing.T) {
	// ACTION (/me) is chat, not metadata — must NOT be treated as a CTCP
	// auto-reply.
	c := New(&Settings{Nick: "bot", CTCPAutoReply: true}, nil, nil)
	c.handlePrivmsg(context.Background(),
		Message{Prefix: "alice!u@h", Params: []string{"#ch"}, Trailing: "\x01ACTION waves\x01"},
		":alice!u@h PRIVMSG #ch :\x01ACTION waves\x01")
	select {
	case env := <-c.Inbound():
		assert.Equal(t, "#ch", env.Channel)
	case <-time.After(time.Second):
		t.Fatal("ACTION must surface as a normal inbound envelope")
	}
}

func TestHandlePartSelfTriggersSelfPart(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.state.OnSelfJoin("#x")
	c.handlePart(context.Background(), Message{Prefix: "bot!u@h", Params: []string{"#x"}})
	// After self-PART the channel must be absent from joined list.
	for _, info := range c.state.JoinedChannels() {
		assert.NotEqual(t, "#x", info.Name, "self-PART must remove channel from joined list")
	}
}

func TestHandleNickChangeFallsBackToTrailing(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	// Some servers carry the new nick in Params instead of Trailing.
	c.handleNickChange(context.Background(),
		Message{Prefix: "alice!u@h", Params: []string{"alice2"}})
}

func TestHandleNickChangeEmptyOldOrNewIsNoop(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.handleNickChange(context.Background(), Message{Prefix: "alice!u@h"})
	c.handleNickChange(context.Background(), Message{Prefix: "", Trailing: "alice2"})
}

func TestSetCurrentNickIgnoresEmpty(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.setCurrentNick("")
	assert.Equal(t, "bot", c.CurrentNick(),
		"setCurrentNick must ignore empty input — the requested nick stays")
}

func TestSetCurrentNickIdempotent(t *testing.T) {
	c := New(&Settings{Nick: "bot"}, nil, nil)
	c.setCurrentNick("bot") // same value — no bouncer push needed
	assert.Equal(t, "bot", c.CurrentNick())
}

// --- Bouncer internal coverage: error + edge branches ----------------

// brokenConn is a net.Conn whose Write always fails. Used to drive the
// bouncer's "client dropped on broken write" branches without needing a
// real socket teardown.
type brokenConn struct {
	net.Conn
	closeErr error
}

func (b *brokenConn) Write(_ []byte) (int, error) { return 0, errBrokenWrite }
func (b *brokenConn) Read(p []byte) (int, error)  { return 0, errBrokenWrite }
func (b *brokenConn) Close() error                { return b.closeErr }
func (b *brokenConn) RemoteAddr() net.Addr        { return fakeAddr{} }
func (b *brokenConn) LocalAddr() net.Addr         { return fakeAddr{} }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "tcp" }
func (fakeAddr) String() string  { return "broken:0" }

var errBrokenWrite = &netErr{msg: "broken"}

type netErr struct{ msg string }

func (e *netErr) Error() string { return e.msg }

func TestBroadcastDropsClientOnWriteError(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")

	// Manually wire a fake authenticated client whose Write fails.
	bc := newBouncerClient(&brokenConn{}, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.Broadcast("PRIVMSG #x :hi", nil)

	b.mu.Lock()
	_, present := b.clients[bc]
	b.mu.Unlock()
	assert.False(t, present, "broken-write client must be removed from the set")
}

func TestBroadcastAsSelfDropsClientOnWriteError(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")

	bc := newBouncerClient(&brokenConn{}, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.BroadcastAsSelf("PRIVMSG #x :hi", nil)

	b.mu.Lock()
	_, present := b.clients[bc]
	b.mu.Unlock()
	assert.False(t, present, "broken-write client must be removed from the set")
}

func TestBroadcastAsSelfTagsZNCSelfMessageCap(t *testing.T) {
	// A client that ACK'd znc.in/self-message gets the tag prefix on
	// fan-out. We capture the bytes through a recordingConn instead of
	// the real socket.
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "user", "host")

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()
	bc.ackCap("znc.in/self-message")
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.BroadcastAsSelf("PRIVMSG #x :hello", nil)
	got := rc.snapshot()
	require.NotEmpty(t, got, "client must receive the fanned-out line")
	assert.Contains(t, got[0], "@znc.in/self-message ",
		"clients with znc.in/self-message must receive the tag prefix")
}

func TestBroadcastSkipsUnauthenticatedClients(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	// Don't call setAuthenticated — should be skipped.
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.Broadcast("PRIVMSG #x :should-not-arrive", nil)
	assert.Empty(t, rc.snapshot(),
		"unauthenticated clients must not receive broadcast lines")
}

func TestUpstreamPrefixEmptyShortCircuitsBroadcastAsSelf(t *testing.T) {
	// With no upstream nick, upstreamPrefix() returns "" and
	// BroadcastAsSelf passes the line through untouched. recordForReplay
	// runs with no state attached (early return).
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	// No AttachState call → state == nil, upstreamNick == "" → both
	// short-circuit branches fire.
	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.BroadcastAsSelf("PRIVMSG #x :no-prefix", nil)
	got := rc.snapshot()
	require.Len(t, got, 1)
	// No upstream prefix means the line went out as-is.
	assert.Equal(t, "PRIVMSG #x :no-prefix", got[0])
}

// recordingConn captures everything written to it so tests can assert
// the bytes the bouncer emits to a client.
type recordingConn struct {
	mu    sync.Mutex
	writes [][]byte
}

func newRecordingConn() *recordingConn { return &recordingConn{} }

func (c *recordingConn) Read(_ []byte) (int, error) {
	// Block until close; the bouncer's handleClient won't call this in
	// these unit tests because we don't run acceptLoop.
	return 0, errBrokenWrite
}

func (c *recordingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(p))
	copy(cp, p)
	c.writes = append(c.writes, cp)
	return len(p), nil
}

func (c *recordingConn) Close() error                       { return nil }
func (c *recordingConn) LocalAddr() net.Addr                { return fakeAddr{} }
func (c *recordingConn) RemoteAddr() net.Addr               { return fakeAddr{} }
func (c *recordingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingConn) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.writes))
	for _, w := range c.writes {
		out = append(out, strings.TrimRight(string(w), "\r\n"))
	}
	return out
}

// --- handleLine forwarding + observer fire path ---------------------

func TestHandleLineForwardsPrivmsgUpstreamAndFiresObserver(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")

	var sentUpstream []string
	b.AttachUpstream(func(line string) error {
		sentUpstream = append(sentUpstream, line)
		return nil
	})

	var observed struct {
		ch, sender, text, kind string
		fired                  int
	}
	b.AttachOutboundObserver(func(channel, sender, text, kind string) {
		observed.ch, observed.sender, observed.text, observed.kind = channel, sender, text, kind
		observed.fired++
	})

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	// Authenticated client tunneling a PRIVMSG to a channel. The
	// bouncer must forward upstream AND fan to other clients AND fire
	// the observer.
	b.handleLine(bc, "PRIVMSG #x :hello world")

	require.Len(t, sentUpstream, 1, "PRIVMSG must be forwarded upstream once")
	assert.Equal(t, "PRIVMSG #x :hello world", sentUpstream[0])
	assert.Equal(t, 1, observed.fired)
	assert.Equal(t, "#x", observed.ch)
	assert.Equal(t, "bot", observed.sender)
	assert.Equal(t, "hello world", observed.text)
	assert.Equal(t, "PRIVMSG", observed.kind)
}

func TestHandleLineMalformedPrivmsgDropped(t *testing.T) {
	// Authenticated client sends a PRIVMSG with empty target (well-
	// formed check fails). Must be silently dropped — no upstream
	// forward, no fan-out, no observer fire.
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")
	called := false
	b.AttachUpstream(func(_ string) error { called = true; return nil })

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.handleLine(bc, "PRIVMSG  :no-target")
	assert.False(t, called, "malformed PRIVMSG must not be forwarded upstream")
}

func TestHandleLineNonForwardableCommandDropped(t *testing.T) {
	// Authenticated client sends a non-forwardable command (e.g.,
	// QUIT). Must not be forwarded upstream.
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")
	called := false
	b.AttachUpstream(func(_ string) error { called = true; return nil })

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	b.handleLine(bc, "QUIT :goodbye")
	assert.False(t, called, "non-forwardable QUIT must not be forwarded")
}

func TestHandleLinePingWithParamsCookie(t *testing.T) {
	// PING via Params rather than Trailing — exercises the cookie
	// fallback assignment in the PING case.
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	b.handleLine(bc, "PING server.name")
	got := rc.snapshot()
	require.NotEmpty(t, got)
	assert.Contains(t, got[0], ":server.name", "params-form PING must echo back via trailing")
}

func TestHandleLineUpstreamReturnsErrorIsLogged(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")
	b.AttachUpstream(func(_ string) error { return errBrokenWrite })

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()
	b.mu.Lock()
	b.clients[bc] = struct{}{}
	b.mu.Unlock()

	// Send failure on the upstream returns early — no fan-out, no
	// observer fire. The test just verifies nothing panics and the
	// log path is exercised.
	b.handleLine(bc, "PRIVMSG #x :nope")
}

func TestHandleLineWithoutUpstreamAttachedNoOps(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")
	// Note: NO AttachUpstream call — send is nil.

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()

	// Forwardable command with no upstream attached: silent return.
	b.handleLine(bc, "JOIN #x")
	// Nothing should have been broadcast to the client either.
	assert.Empty(t, rc.snapshot())
}

func TestHandleLineUnauthenticatedNonPassDropped(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	// no setAuthenticated

	b.handleLine(bc, "JOIN #x")
	// No upstream attached, no client writes — silent drop.
	assert.Empty(t, rc.snapshot())
}

func TestHandleLinePingRepliesLocally(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.setAuthenticated()

	b.handleLine(bc, "PING :token123")
	got := rc.snapshot()
	require.NotEmpty(t, got)
	assert.Contains(t, got[0], "PONG turborg-bouncer :token123",
		"PING must be answered locally without touching upstream")
}

// --- handleCap LIST + END branches ----------------------------------

func TestHandleCapListAndEnd(t *testing.T) {
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(NewChannelState(), "bot", "u", "h")

	rc := newRecordingConn()
	bc := newBouncerClient(rc, slog.Default())
	bc.ackCap("echo-message")

	// CAP LIST returns the negotiated set.
	b.handleCap(bc, Message{Command: CmdCap, Params: []string{"LIST"}})
	lines := rc.snapshot()
	require.NotEmpty(t, lines)
	assert.Contains(t, lines[len(lines)-1], "CAP * LIST :echo-message")

	// CAP END with no deferred welcome is a no-op.
	b.handleCap(bc, Message{Command: CmdCap, Params: []string{"END"}})

	// CAP REQ with a multi-word REQ via params (no trailing).
	b.handleCap(bc, Message{Command: CmdCap, Params: []string{"REQ", "echo-message", "server-time"}})
	final := rc.snapshot()
	// Expect at least one CAP * ACK reply.
	gotAck := false
	for _, l := range final {
		if strings.Contains(l, "CAP * ACK") {
			gotAck = true
			break
		}
	}
	assert.True(t, gotAck, "CAP REQ via params (not trailing) must still ACK")
}

func TestRecordForReplayBoundsRing(t *testing.T) {
	state := NewChannelState()
	state.OnSelfJoin("#busy")
	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(state, "nick", "ident", "host")

	for i := 0; i < channelLogCap+50; i++ {
		b.recordForReplay(":a!u@h PRIVMSG #busy :spam")
	}
	b.logMu.Lock()
	got := len(b.channelLog["#busy"])
	b.logMu.Unlock()
	assert.Equal(t, channelLogCap, got, "ring should cap at channelLogCap")
}

func TestWatchdogPollInterval(t *testing.T) {
	cases := []struct {
		name        string
		warn, pause time.Duration
		want        time.Duration
	}{
		{"both zero falls back to 1s default", 0, 0, time.Second},
		{"warn only, picks smaller target", 4 * time.Second, 0, time.Second},
		{"pause only, picks smaller target", 0, 4 * time.Second, time.Second},
		{"pause smaller than warn wins", time.Minute, 4 * time.Second, time.Second},
		{"warn smaller than pause wins", 4 * time.Second, time.Minute, time.Second},
		{"floor at 10ms for tiny targets", 20 * time.Millisecond, 0, 10 * time.Millisecond},
		{"clamped to 1s ceiling for hour-long", time.Hour, time.Hour, time.Second},
		{"derived as quarter of target", 200 * time.Millisecond, 0, 50 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, watchdogPollInterval(tc.warn, tc.pause))
		})
	}
}

func TestBouncerWarnHookBroadcastsStrongerNotice(t *testing.T) {
	machine := NewUpstreamStateMachine(nil)
	state := NewChannelState()
	state.OnSelfJoin("#a")

	b, err := NewBouncer("p", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachUpstreamState(machine, "Libera Chat")
	b.AttachUpstream(func(string) error { return nil })

	require.NoError(t, b.Start(context.Background()))
	t.Cleanup(func() { _ = b.Stop() })

	addr := b.Addr()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n')
	_, err = conn.Write([]byte("PASS p\r\n"))
	require.NoError(t, err)
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, " 001 ") {
			break
		}
	}

	// Simulate the supervisor's escalation watchdog firing — unexported
	// method, so this test lives next to the implementation rather than
	// in the external _test package.
	b.onUpstreamWarn("Ping timeout: 240 seconds", 11*time.Minute)

	conn.SetReadDeadline(time.Now().Add(time.Second))
	var got string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "still retrying") {
			got = line
			break
		}
	}
	require.NotEmpty(t, got, "warn hook must broadcast a stronger NOTICE")
	assert.Contains(t, got, "#a")
	assert.Contains(t, got, "11m")
	assert.Contains(t, got, "Ping timeout: 240 seconds")
}

func TestParseJoinLine(t *testing.T) {
	cases := []struct {
		name     string
		params   []string
		trailing string
		channels []string
		keys     []string
	}{
		{
			name:     "single channel no key",
			params:   []string{"#a"},
			channels: []string{"#a"},
			keys:     []string{""},
		},
		{
			name:     "single channel with key",
			params:   []string{"#a", "secret"},
			channels: []string{"#a"},
			keys:     []string{"secret"},
		},
		{
			name:     "comma-list with paired keys",
			params:   []string{"#a,#b,#c", "k1,k2,k3"},
			channels: []string{"#a", "#b", "#c"},
			keys:     []string{"k1", "k2", "k3"},
		},
		{
			name:     "comma-list with fewer keys padded",
			params:   []string{"#a,#b,#c", "k1"},
			channels: []string{"#a", "#b", "#c"},
			keys:     []string{"k1", "", ""},
		},
		{
			name:     "comma-list with empty middle key preserved",
			params:   []string{"#a,#b,#c", "k1,,k3"},
			channels: []string{"#a", "#b", "#c"},
			keys:     []string{"k1", "", "k3"},
		},
		{
			name:     "channel in trailing slot",
			params:   nil,
			trailing: "#a",
			channels: []string{"#a"},
			keys:     []string{""},
		},
		{
			name: "empty surfaces no channels",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCh, gotKeys := parseJoinLine(tc.params, tc.trailing)
			assert.Equal(t, tc.channels, gotCh)
			assert.Equal(t, tc.keys, gotKeys)
		})
	}
}
