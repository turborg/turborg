package discord

import (
	"errors"

	"github.com/bwmarrin/discordgo"
	"github.com/turborg/turborg/internal/agent"
)

// errUnsupported is returned for moderation actions the Actor contract can't
// express against Discord's API (the Actor surface carries no role id for
// Op/Voice, no mode string Discord honours, and Discord invites are channel
// links rather than user-targeted). The contract explicitly allows a connector
// to error on actions it doesn't support.
var errUnsupported = errors.New("discord: action not supported")

// errNotConnected is returned when an action is attempted while the Gateway
// session is parked / not yet open.
var errNotConnected = errors.New("discord: not connected")

// Actor implements agent.Actor over the Discord connector. Say/Notice post to a
// channel; Kick/Ban/Topic map to real guild moderation. Targets are Discord
// ids (a channel id, a user id). Required bot permissions: Kick Members, Ban
// Members, Manage Channels.
type Actor struct {
	conn *Connector
}

// NewActor builds a Discord Actor bound to a connector.
func NewActor(c *Connector) *Actor { return &Actor{conn: c} }

var _ agent.Actor = (*Actor)(nil)

// Say posts text to a channel and mirrors it onto the bus for persistence.
func (a *Actor) Say(channel, text string) error {
	s := a.conn.getSession()
	if s == nil {
		return errNotConnected
	}
	if _, err := s.ChannelMessageSend(channel, text); err != nil {
		return err
	}
	a.conn.publishSent(channel, text)
	return nil
}

// Notice renders identically to Say — Discord has no separate notice type.
func (a *Actor) Notice(target, text string) error { return a.Say(target, text) }

// Kick removes a user (nick = user id) from the connector's guild.
func (a *Actor) Kick(_, nick, reason string) error {
	s := a.conn.getSession()
	if s == nil {
		return errNotConnected
	}
	return s.GuildMemberDeleteWithReason(a.conn.settings.GuildID, nick, reason)
}

// Ban bans a user (mask = user id) from the connector's guild.
func (a *Actor) Ban(_, mask string) error {
	s := a.conn.getSession()
	if s == nil {
		return errNotConnected
	}
	return s.GuildBanCreateWithReason(a.conn.settings.GuildID, mask, "banned by turborg", 0)
}

// Topic sets a channel's topic.
func (a *Actor) Topic(channel, topic string) error {
	s := a.conn.getSession()
	if s == nil {
		return errNotConnected
	}
	_, err := s.ChannelEditComplex(channel, &discordgo.ChannelEdit{Topic: topic})
	return err
}

func (a *Actor) SetMode(_, _ string, _ ...string) error { return errUnsupported }
func (a *Actor) Op(_, _ string) error                   { return errUnsupported }
func (a *Actor) Voice(_, _ string) error                { return errUnsupported }
func (a *Actor) Invite(_, _ string) error               { return errUnsupported }
