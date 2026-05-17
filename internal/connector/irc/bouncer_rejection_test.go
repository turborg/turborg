package irc_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// noticeRe extracts the target out of a `:prefix NOTICE <target> :body`
// line. Used by the per-target reject tests to assert routing.
var noticeRe = regexp.MustCompile(`^:[^ ]+ NOTICE (\S+) :`)

func noticeTarget(line string) string {
	m := noticeRe.FindStringSubmatch(strings.TrimRight(line, "\r\n"))
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// TestBouncerOutboundThrottleRejectionTargetsChannel covers scenario 1
// from the rejection-feedback plan: when the outbound throttle kills a
// client PRIVMSG, the resulting NOTICE must land in the same buffer
// the user typed in (the channel target) — not the server status tab,
// and not the *turborg service buffer. The user's attention is in the
// channel they were just typing in; inline feedback is the right UX
// for the throttle case (unlike policy denials, which use *turborg).
func TestBouncerOutboundThrottleRejectionTargetsChannel(t *testing.T) {
	throttle, err := irc.NewThrottle(1, 30*time.Second, nil)
	require.NoError(t, err)

	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachOutboundThrottle(throttle)

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "PRIVMSG #archlinux :hi")
	time.Sleep(50 * time.Millisecond)
	writeLine(t, conn, "PRIVMSG #archlinux :hi again")

	notice := readUntilContains(r, conn, "rate-limited", 500*time.Millisecond)
	require.NotEmpty(t, notice, "throttle kill must produce a NOTICE")
	assert.Equal(t, "#archlinux", noticeTarget(notice),
		"throttle NOTICE must target the rejected PRIVMSG's channel, not the status tab")
	assert.Contains(t, notice, "NOT sent",
		"throttle NOTICE body must explicitly say the message was NOT sent")
}

// TestBouncerOutboundThrottleRejectionTargetsNick verifies the same
// channel-targeting rule for nick-targeted PRIVMSGs (private messages):
// the NOTICE lands in the recipient's query buffer, which is the only
// place the user has a meaningful conversational context with that
// nick.
func TestBouncerOutboundThrottleRejectionTargetsNick(t *testing.T) {
	throttle, err := irc.NewThrottle(1, 30*time.Second, nil)
	require.NoError(t, err)

	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachOutboundThrottle(throttle)

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "PRIVMSG alice :one")
	time.Sleep(50 * time.Millisecond)
	writeLine(t, conn, "PRIVMSG alice :two")

	notice := readUntilContains(r, conn, "rate-limited", 500*time.Millisecond)
	require.NotEmpty(t, notice, "throttle kill must produce a NOTICE")
	assert.Equal(t, "alice", noticeTarget(notice),
		"nick-DM rejection NOTICE must target the recipient nick")
}

// servicePrivmsgRe matches a `:*turborg!… PRIVMSG <target> :<body>`
// line. Used by the service-buffer tests to assert both the source
// (*turborg virtual nick) and the target (bot's own nick) in one
// parse.
var servicePrivmsgRe = regexp.MustCompile(`^:\*turborg![^ ]+ PRIVMSG (\S+) :`)

func servicePrivmsgTarget(line string) string {
	m := servicePrivmsgRe.FindStringSubmatch(strings.TrimRight(line, "\r\n"))
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// TestBouncerNickLockedRejectionRoutesToServiceBuffer verifies that
// NICK policy denials surface as PRIVMSG from the *turborg virtual
// service nick — most IRC clients open a dedicated query tab for it,
// so meta-conversation accumulates in one predictable place rather
// than spamming every joined channel buffer (the prior routing's
// failure mode).
func TestBouncerNickLockedRejectionRoutesToServiceBuffer(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK shinynewnick")

	line := readUntilContains(r, conn, "Nick change", 500*time.Millisecond)
	require.NotEmpty(t, line, "NICK denial must produce a service PRIVMSG")
	assert.Equal(t, "turborg", servicePrivmsgTarget(line),
		"service PRIVMSG must target the bot's nick — that's where the IRC client opens the query buffer")
	assert.Contains(t, line, "shinynewnick",
		"service PRIVMSG body must echo the requested nick so the user knows what was rejected")

	// No channel-targeted NOTICE may have leaked out — the broadcast-
	// to-every-channel behavior the previous routing used is exactly
	// what this commit removes.
	conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	for {
		extra, err := r.ReadString('\n')
		if err != nil {
			break
		}
		assert.False(t, strings.Contains(extra, "NOTICE #a") ||
			strings.Contains(extra, "NOTICE #b"),
			"channel buffers must stay clean — NICK denials no longer broadcast: %s", extra)
	}
}

// TestBouncerJoinOverCapRejectionRoutesToServiceBuffer verifies that
// JOIN policy denials surface as PRIVMSG from *turborg. The body
// names the attempted channel (so the user knows what got rejected),
// but the rejection does NOT open a #channel tab — fake-opening a
// tab for a channel the user never made it into is wrong UX.
func TestBouncerJoinOverCapRejectionRoutesToServiceBuffer(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 2})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #c")

	line := readUntilContains(r, conn, "Channel cap", 500*time.Millisecond)
	require.NotEmpty(t, line, "channel-cap policy must produce a service PRIVMSG")
	assert.Equal(t, "turborg", servicePrivmsgTarget(line),
		"JOIN denial must route to the bot's nick via *turborg, not the attempted channel")
	assert.Contains(t, line, "#c",
		"body must name the channel the user tried to join")
	assert.Contains(t, line, "/part",
		"body must hint at the recovery step")

	// The previous routing emitted a NOTICE #c that opened a tab the
	// user never wanted. Confirm no such line leaks now.
	conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	for {
		extra, err := r.ReadString('\n')
		if err != nil {
			break
		}
		assert.NotContains(t, extra, "NOTICE #c",
			"#c tab must not be fake-opened by a rejected JOIN")
	}
}

// TestBouncerJoinOverCapCommaListMentionsFirstChannel — when the user
// JOINs a comma-list (`JOIN #x,#y,#z`) and the cap rejects, the
// service-buffer body names the first attempted channel so the user
// knows what they tried to do.
func TestBouncerJoinOverCapCommaListMentionsFirstChannel(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#existing")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 1})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #x,#y,#z")

	line := readUntilContains(r, conn, "Channel cap", 500*time.Millisecond)
	require.NotEmpty(t, line)
	assert.Equal(t, "turborg", servicePrivmsgTarget(line),
		"comma-list JOIN still routes to *turborg")
	assert.Contains(t, line, "#x",
		"body must name the first channel of the comma-list so the user knows what failed")
}

// TestBouncerPolicyRejectionFallsBackToStarBeforeUpstreamNick covers
// the corner case where the bouncer hasn't yet learned the bot's
// upstream nick (pre-Dial window, or upstream still mid-registration).
// The NOTICE must still go through, addressed to the IRC `*`
// placeholder. NICK with no joined channels is the path that exercises
// the fallback most cleanly.
func TestBouncerPolicyRejectionFallsBackToStarBeforeUpstreamNick(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	// AttachState with an empty upstream nick — this is the state during
	// the pre-Dial window before Connector.bringUp has set the bot's nick.
	b.AttachState(irc.NewChannelState(), "", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK whatever")

	notice := readUntilContains(r, conn, "Nick change", 500*time.Millisecond)
	require.NotEmpty(t, notice)
	assert.Equal(t, "*", noticeTarget(notice),
		"pre-registration NOTICEs must fall back to the * placeholder target")
}
