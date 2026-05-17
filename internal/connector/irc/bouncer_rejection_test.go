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

// TestBouncerNickLockedRejectionBroadcastsToJoinedChannels covers the
// per-command routing for NICK policy denials: nick changes are global
// so the NOTICE fans out to every joined channel — the user sees it
// wherever they happen to be looking. Falls back to the status target
// only when no channels are joined (covered by a sibling test).
func TestBouncerNickLockedRejectionBroadcastsToJoinedChannels(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK shinynewnick")

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var sawA, sawB bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if !strings.Contains(line, "Nick change") {
			continue
		}
		assert.Contains(t, line, "shinynewnick",
			"NOTICE body must echo the requested nick so the user knows what was rejected")
		switch noticeTarget(line) {
		case "#a":
			sawA = true
		case "#b":
			sawB = true
		}
	}
	assert.True(t, sawA, "#a buffer must receive the NICK denial NOTICE")
	assert.True(t, sawB, "#b buffer must receive the NICK denial NOTICE")
}

// TestBouncerNickLockedRejectionFallsBackToStatusWhenNoChannels covers
// the empty-joined-set branch of the NICK denial routing: with no
// channels joined, the NOTICE goes to the bot's nick / `*` so it
// lands in the client's server status tab.
func TestBouncerNickLockedRejectionFallsBackToStatusWhenNoChannels(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK newnick")

	notice := readUntilContains(r, conn, "Nick change", 500*time.Millisecond)
	require.NotEmpty(t, notice)
	assert.Equal(t, "turborg", noticeTarget(notice),
		"NICK denial falls back to the bot's nick when no channels are joined")
}

// TestBouncerJoinOverCapRejectionTargetsAttemptedChannel verifies the
// per-command routing for JOIN policy denials: the NOTICE targets the
// channel the user tried to join, so their IRC client opens that tab
// and renders the rejection inline rather than burying it in the
// server status window the user wasn't looking at.
func TestBouncerJoinOverCapRejectionTargetsAttemptedChannel(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 2})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #c")

	notice := readUntilContains(r, conn, "Channel cap", 500*time.Millisecond)
	require.NotEmpty(t, notice, "channel-cap policy must produce a NOTICE")
	assert.Equal(t, "#c", noticeTarget(notice),
		"channel-cap NOTICE must target the channel the user tried to join, not the status tab")
	assert.Contains(t, notice, "#c",
		"NOTICE body must name the attempted channel")
	assert.Contains(t, notice, "/part",
		"NOTICE body must hint at the recovery step")
}

// TestBouncerJoinOverCapCommaListTargetsFirstChannel — when the user
// JOINs a comma-list (`JOIN #a,#b`) and the cap rejects, target the
// FIRST channel of the list so the NOTICE lands somewhere
// deterministic.
func TestBouncerJoinOverCapCommaListTargetsFirstChannel(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	trackForwarded(b)
	state := irc.NewChannelState()
	state.OnSelfJoin("#existing")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 1})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #x,#y,#z")

	notice := readUntilContains(r, conn, "Channel cap", 500*time.Millisecond)
	require.NotEmpty(t, notice)
	assert.Equal(t, "#x", noticeTarget(notice),
		"comma-list JOIN must surface the rejection on the first channel target")
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
