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
// the user typed in (the channel target) — not the server status tab.
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

	notice := readUntilContains(r, conn, "NOTICE", 500*time.Millisecond)
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

	notice := readUntilContains(r, conn, "NOTICE", 500*time.Millisecond)
	require.NotEmpty(t, notice, "throttle kill must produce a NOTICE")
	assert.Equal(t, "alice", noticeTarget(notice),
		"nick-DM rejection NOTICE must target the recipient nick")
}

// TestBouncerNickLockedRejectionTargetsBotNick verifies that operator-
// policy rejections that aren't tied to a channel context (nick locks)
// route to the bot's nick so most clients render them in the server
// status tab — historically these used `*` regardless of registration
// state, which made the NOTICE indistinguishable from a pre-auth
// system message.
func TestBouncerNickLockedRejectionTargetsBotNick(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK newnick")

	notice := readUntilContains(r, conn, "NOTICE", 500*time.Millisecond)
	require.NotEmpty(t, notice, "nick-locked policy must produce a NOTICE")
	assert.Equal(t, "turborg", noticeTarget(notice),
		"nick-lock NOTICE must target the bot's nick once upstream identity is known")
	assert.Contains(t, notice, "nick changes are disabled")
}

// TestBouncerJoinOverCapRejectionTargetsBotNick mirrors the nick-lock
// case for JOIN-over-cap — no channel context to attach to (the JOIN
// itself failed), so the NOTICE goes to the bot's nick.
func TestBouncerJoinOverCapRejectionTargetsBotNick(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 2})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #c")

	notice := readUntilContains(r, conn, "NOTICE", 500*time.Millisecond)
	require.NotEmpty(t, notice, "channel-cap policy must produce a NOTICE")
	assert.Equal(t, "turborg", noticeTarget(notice),
		"channel-cap NOTICE must target the bot's nick")
	assert.Contains(t, notice, "channel limit reached")
}

// TestBouncerPolicyRejectionFallsBackToStarBeforeUpstreamNick covers
// the corner case where the bouncer hasn't yet learned the bot's
// upstream nick (pre-Dial window, or upstream still mid-registration).
// The NOTICE must still go through, addressed to the IRC `*`
// placeholder.
func TestBouncerPolicyRejectionFallsBackToStarBeforeUpstreamNick(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	// AttachState with an empty upstream nick — this is the state during
	// the pre-Dial window before Connector.bringUp has set the bot's nick.
	b.AttachState(irc.NewChannelState(), "", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK whatever")

	notice := readUntilContains(r, conn, "NOTICE", 500*time.Millisecond)
	require.NotEmpty(t, notice)
	assert.Equal(t, "*", noticeTarget(notice),
		"pre-registration NOTICEs must fall back to the * placeholder target")
}
