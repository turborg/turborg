package irc

import "fmt"

// ClientLimits captures the operator-policy values that gate client-
// initiated IRC actions. Both the bouncer (commands sent by an attached
// IRC client like HexChat) and the WS gateway (actions sent by a web UI
// client) consult the same struct so the two surfaces enforce the same
// rules.
//
// Zero / empty values uniformly mean "unrestricted" — leaving every field
// at its zero value gives the pre-policy behavior.
//
// All decisions are made from the struct's own fields; there's no
// per-action state. State-dependent caps (e.g. "you're already at the
// channel cap") get the current count passed in via AllowCommand's
// arguments so the policy stays free of hidden coupling.
type ClientLimits struct {
	// NickLocked rejects /NICK changes initiated by attached clients.
	// Server-forced nick changes (KILL, services, collision) bypass the
	// policy entirely — they don't pass through AllowCommand.
	NickLocked bool

	// RealnameLocked rejects mid-session realname rewrites via USER.
	// In practice clients rarely re-send USER; this guards the edge
	// case of a tampered or non-standard client that does.
	RealnameLocked bool

	// MaxChannels caps the number of channels the agent is allowed to
	// have joined at any one time. 0 = unrestricted. JOIN is rejected
	// when the current count would exceed the cap; PART always allowed.
	MaxChannels int

	// TBSummarizeMaxMessages caps how many channel messages /tb summarize
	// can consume. 0 = feature disabled.
	TBSummarizeMaxMessages int

	// AIStrict gates the channel-history-consuming /tb AI command family
	// (e.g. /tb summarize) behind channel-operator consent, for upstream
	// networks whose bot policy requires it. When true, those commands only
	// run if the bot holds channel-operator status (mode +o or higher) in
	// the target channel — an op granting the bot +o is the consent signal.
	// False (the default) leaves the AI commands unrestricted. Other /tb
	// subcommands (e.g. usage) are never gated by this flag.
	AIStrict bool

	// AIStrictMessage is the operator-facing notice returned when an AI
	// history command is blocked under AIStrict because the bot lacks
	// channel-operator status. Empty falls back to DefaultAIStrictMessage,
	// so a strict network's specific policy text can be supplied by the
	// operator without baking any one network's wording into the framework.
	AIStrictMessage string
}

// DefaultAIStrictMessage is the network-neutral notice sent when an AI
// history command is denied under AIStrict and no AIStrictMessage override
// was configured. Operators on a network with a published bot policy
// typically override it with that policy's specific wording and URL.
const DefaultAIStrictMessage = "AI commands that read channel history require channel-operator status here, per this network's bot policy."

// AIStrictDenyMessage returns the notice to send when an AI history command
// is blocked under AIStrict — the configured override, or the neutral
// default when none is set.
func (l ClientLimits) AIStrictDenyMessage() string {
	if l.AIStrictMessage != "" {
		return l.AIStrictMessage
	}
	return DefaultAIStrictMessage
}

// CapHitKind maps an IRC command to the canonical "kind" label used in
// cap_hit telemetry. Stable identifier across surfaces (bouncer + WS
// gateway) so downstream counters can aggregate by kind. Lives next to
// ClientLimits because the kinds map to the same gates that struct
// implements.
func CapHitKind(cmd string) string {
	switch cmd {
	case CmdNick:
		return "nick_locked"
	case CmdUser:
		return "realname_locked"
	case CmdJoin:
		return "channels"
	default:
		return cmd
	}
}

// AllowCommand returns (true, "") when the action is permitted, or
// (false, reason) when it must be rejected. The caller is responsible
// for surfacing reason to the originator (NOTICE for bouncer clients,
// a WS event for gateway clients).
//
// currentChannels is the count of channels the agent is already in.
// Pass 0 for commands where channel count is irrelevant.
func (l ClientLimits) AllowCommand(cmd string, currentChannels int) (bool, string) {
	switch cmd {
	case CmdNick:
		if l.NickLocked {
			return false, "nick changes are disabled by operator policy"
		}
	case CmdUser:
		if l.RealnameLocked {
			return false, "realname is locked by operator policy"
		}
	case CmdJoin:
		if l.MaxChannels > 0 && currentChannels >= l.MaxChannels {
			return false, fmt.Sprintf("channel limit reached (%d)", l.MaxChannels)
		}
	}
	return true, ""
}
