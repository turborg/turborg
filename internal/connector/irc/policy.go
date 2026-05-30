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
