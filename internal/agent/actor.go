package agent

// Actor is the connector-agnostic action surface a skill engine uses to act on
// a chat network: send messages and notices, and perform channel moderation.
// Every protocol adapter that supports these provides an implementation;
// connectors that cannot perform an action return an error from it.
//
// Targets are connector-native identifiers (an IRC channel/nick, a Discord
// channel/user id, …). The engine never constructs protocol wire itself — it
// only calls these methods, so moderation stays portable across connectors.
type Actor interface {
	// Say sends a message to a channel (or a user, for a direct target).
	Say(channel, text string) error
	// Notice sends a notice to a target (channel or user).
	Notice(target, text string) error
	// Kick removes a nick from a channel with an optional reason.
	Kick(channel, nick, reason string) error
	// Ban bans a nick or mask from a channel.
	Ban(channel, mask string) error
	// SetMode applies a raw mode string (with optional args) to a channel.
	SetMode(channel, modes string, args ...string) error
	// Op grants channel-operator status to a nick.
	Op(channel, nick string) error
	// Voice grants voice to a nick.
	Voice(channel, nick string) error
	// Topic sets a channel's topic.
	Topic(channel, topic string) error
	// Invite invites a nick to a channel.
	Invite(channel, nick string) error
}
