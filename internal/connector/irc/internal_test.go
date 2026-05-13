package irc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
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
		"\x01VERSION\x01":        "VERSION turborg-go",
		"\x01PING 12345\x01":     "PING 12345",
		"\x01CLIENTINFO\x01":     "CLIENTINFO VERSION PING TIME CLIENTINFO SOURCE USERINFO",
		"\x01SOURCE\x01":         "SOURCE https://github.com/turborg/turborg",
		"\x01USERINFO\x01":       "USERINFO turborg agent",
		"\x01\x01":               "",  // empty inner
		"\x01UNKNOWN\x01":        "",  // unrecognized
		"\x01version lowercase\x01": "VERSION turborg-go",
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
