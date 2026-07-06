package slack

import (
	"errors"

	slackapi "github.com/slack-go/slack"
	"github.com/turborg/turborg/internal/agent"
)

// errUnsupported is returned for moderation actions the Actor contract can't
// express against Slack's API (no channel-op/voice concept, no per-mode string,
// no bot-issued workspace ban). The contract explicitly allows a connector to
// error on actions it doesn't support.
var errUnsupported = errors.New("slack: action not supported")

// errNotConnected is returned when an action is attempted before the client is
// open.
var errNotConnected = errors.New("slack: not connected")

// Actor implements agent.Actor over the Slack connector. Say/Notice post to a
// channel; Kick removes a user from a conversation; Topic sets a channel topic;
// Invite adds users to a conversation. Targets are Slack ids (a channel id, a
// user id). The bot needs the matching scopes (chat:write, channels:manage,
// conversations kick/invite).
type Actor struct {
	conn *Connector
}

// NewActor builds a Slack Actor bound to a connector.
func NewActor(c *Connector) *Actor { return &Actor{conn: c} }

var _ agent.Actor = (*Actor)(nil)

// Say posts text to a channel and mirrors it onto the bus for persistence.
func (a *Actor) Say(channel, text string) error {
	api := a.conn.getAPI()
	if api == nil {
		return errNotConnected
	}
	if _, _, err := api.PostMessage(channel, slackapi.MsgOptionText(text, false)); err != nil {
		return err
	}
	a.conn.publishSent(channel, text)
	return nil
}

// Notice renders identically to Say — Slack has no separate notice type.
func (a *Actor) Notice(target, text string) error { return a.Say(target, text) }

// Kick removes a user (nick = user id) from a conversation.
func (a *Actor) Kick(channel, nick, _ string) error {
	api := a.conn.getAPI()
	if api == nil {
		return errNotConnected
	}
	return api.KickUserFromConversation(channel, nick)
}

// Topic sets a channel's topic.
func (a *Actor) Topic(channel, topic string) error {
	api := a.conn.getAPI()
	if api == nil {
		return errNotConnected
	}
	_, err := api.SetTopicOfConversation(channel, topic)
	return err
}

// Invite adds a user (nick = user id) to a conversation.
func (a *Actor) Invite(channel, nick string) error {
	api := a.conn.getAPI()
	if api == nil {
		return errNotConnected
	}
	_, err := api.InviteUsersToConversation(channel, nick)
	return err
}

func (a *Actor) Ban(_, _ string) error                  { return errUnsupported }
func (a *Actor) SetMode(_, _ string, _ ...string) error { return errUnsupported }
func (a *Actor) Op(_, _ string) error                   { return errUnsupported }
func (a *Actor) Voice(_, _ string) error                { return errUnsupported }
