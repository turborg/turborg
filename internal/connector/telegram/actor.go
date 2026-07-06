package telegram

import (
	"errors"
	"strconv"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/turborg/turborg/internal/agent"
)

// errUnsupported is returned for moderation actions the Actor contract can't
// express against Telegram's Bot API (no channel-op/voice concept, no per-mode
// string, no user-targeted invite). The contract explicitly allows a connector
// to error on actions it doesn't support.
var errUnsupported = errors.New("telegram: action not supported")

// errNotConnected is returned when an action is attempted before the client is
// open.
var errNotConnected = errors.New("telegram: not connected")

// Actor implements agent.Actor over the Telegram connector. Say/Notice send a
// message to a chat; Kick/Ban map to chat-member bans. Targets are Telegram ids
// as strings (a chat id, a user id). The bot must be a chat administrator with
// the "ban users" right for moderation.
type Actor struct {
	conn *Connector
}

// NewActor builds a Telegram Actor bound to a connector.
func NewActor(c *Connector) *Actor { return &Actor{conn: c} }

var _ agent.Actor = (*Actor)(nil)

// Say sends text to a chat and mirrors it onto the bus for persistence.
func (a *Actor) Say(channel, text string) error {
	api := a.conn.getAPI()
	if api == nil {
		return errNotConnected
	}
	chatID, err := strconv.ParseInt(channel, 10, 64)
	if err != nil {
		return errors.New("telegram: invalid chat id " + channel)
	}
	if _, err := api.Send(tgbotapi.NewMessage(chatID, text)); err != nil {
		return err
	}
	a.conn.publishSent(channel, text)
	return nil
}

// Notice renders identically to Say — Telegram has no separate notice type.
func (a *Actor) Notice(target, text string) error { return a.Say(target, text) }

// Kick bans a user (nick = user id) from a chat, leaving their prior messages
// intact.
func (a *Actor) Kick(channel, nick, _ string) error {
	return a.ban(channel, nick, false)
}

// Ban bans a user (mask = user id) from a chat and revokes their messages.
func (a *Actor) Ban(channel, mask string) error {
	return a.ban(channel, mask, true)
}

// ban issues a BanChatMember request against the chat/user pair.
func (a *Actor) ban(channel, user string, revokeMessages bool) error {
	api := a.conn.getAPI()
	if api == nil {
		return errNotConnected
	}
	chatID, err := strconv.ParseInt(channel, 10, 64)
	if err != nil {
		return errors.New("telegram: invalid chat id " + channel)
	}
	userID, err := strconv.ParseInt(user, 10, 64)
	if err != nil {
		return errors.New("telegram: invalid user id " + user)
	}
	_, err = api.Request(tgbotapi.BanChatMemberConfig{
		ChatMemberConfig: tgbotapi.ChatMemberConfig{ChatID: chatID, UserID: userID},
		RevokeMessages:   revokeMessages,
	})
	return err
}

func (a *Actor) SetMode(_, _ string, _ ...string) error { return errUnsupported }
func (a *Actor) Op(_, _ string) error                   { return errUnsupported }
func (a *Actor) Voice(_, _ string) error                { return errUnsupported }
func (a *Actor) Topic(_, _ string) error                { return errUnsupported }
func (a *Actor) Invite(_, _ string) error               { return errUnsupported }
