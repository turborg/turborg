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
	"sync"
	"testing"

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
