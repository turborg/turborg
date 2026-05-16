package irc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestClientLimitsDefaultAllowsEverything(t *testing.T) {
	l := irc.ClientLimits{}

	for _, cmd := range []string{
		irc.CmdNick, irc.CmdUser, irc.CmdJoin, irc.CmdPart,
		irc.CmdPrivmsg, irc.CmdNotice, irc.CmdMode,
	} {
		allow, reason := l.AllowCommand(cmd, 0)
		assert.Truef(t, allow, "default ClientLimits should allow %s, got reason %q", cmd, reason)
	}
}

func TestClientLimitsNickLockedRejectsNick(t *testing.T) {
	l := irc.ClientLimits{NickLocked: true}

	allow, reason := l.AllowCommand(irc.CmdNick, 0)
	assert.False(t, allow)
	assert.Contains(t, reason, "nick")
}

func TestClientLimitsNickLockedStillAllowsOtherCommands(t *testing.T) {
	// NickLocked specifically gates /NICK and nothing else.
	l := irc.ClientLimits{NickLocked: true}

	allow, _ := l.AllowCommand(irc.CmdJoin, 0)
	assert.True(t, allow)
	allow, _ = l.AllowCommand(irc.CmdPrivmsg, 0)
	assert.True(t, allow)
}

func TestClientLimitsRealnameLockedRejectsUser(t *testing.T) {
	l := irc.ClientLimits{RealnameLocked: true}

	allow, reason := l.AllowCommand(irc.CmdUser, 0)
	assert.False(t, allow)
	assert.Contains(t, reason, "realname")
}

func TestClientLimitsMaxChannelsRejectsJoinAtCap(t *testing.T) {
	l := irc.ClientLimits{MaxChannels: 5}

	allow, reason := l.AllowCommand(irc.CmdJoin, 5)
	assert.False(t, allow)
	assert.Contains(t, reason, "5")
}

func TestClientLimitsMaxChannelsAllowsJoinBelowCap(t *testing.T) {
	l := irc.ClientLimits{MaxChannels: 5}

	allow, _ := l.AllowCommand(irc.CmdJoin, 4)
	assert.True(t, allow)
}

func TestClientLimitsZeroMaxChannelsAllowsAnyJoin(t *testing.T) {
	// 0 = unrestricted, even for absurd current counts.
	l := irc.ClientLimits{MaxChannels: 0}

	allow, _ := l.AllowCommand(irc.CmdJoin, 9999)
	assert.True(t, allow)
}

func TestClientLimitsUnknownCommandAlwaysAllowed(t *testing.T) {
	// Commands the policy has no opinion on must pass through. Future
	// commands shouldn't need a policy update unless they're load-bearing.
	l := irc.ClientLimits{NickLocked: true, RealnameLocked: true, MaxChannels: 1}

	allow, _ := l.AllowCommand("MADE_UP_CMD", 0)
	assert.True(t, allow)
}

func TestClientLimitsRealnameLockedDoesNotGateNick(t *testing.T) {
	// Cross-check: each lock is independent. RealnameLocked must not
	// also lock NICK, and NickLocked must not also lock USER.
	l := irc.ClientLimits{RealnameLocked: true}
	allow, _ := l.AllowCommand(irc.CmdNick, 0)
	assert.True(t, allow, "RealnameLocked must not gate NICK")
}

func TestCapHitKindMapping(t *testing.T) {
	cases := []struct {
		cmd      string
		wantKind string
	}{
		{irc.CmdNick, "nick_locked"},
		{irc.CmdUser, "realname_locked"},
		{irc.CmdJoin, "channels"},
		{irc.CmdPrivmsg, irc.CmdPrivmsg}, // fall-through: kind == cmd
		{"UNKNOWN", "UNKNOWN"},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.wantKind, irc.CapHitKind(tc.cmd),
			"CapHitKind(%q)", tc.cmd)
	}
}

func TestClientLimitsMaxChannelsDoesNotGateNonJoin(t *testing.T) {
	// Reaching the channel cap must not silently lock other commands.
	l := irc.ClientLimits{MaxChannels: 1}
	for _, cmd := range []string{irc.CmdPrivmsg, irc.CmdNotice, irc.CmdPart, irc.CmdMode} {
		allow, _ := l.AllowCommand(cmd, 99)
		assert.Truef(t, allow, "MaxChannels must not gate %s even when count > cap", cmd)
	}
}
