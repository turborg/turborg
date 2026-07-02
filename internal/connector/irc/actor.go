package irc

import (
	"fmt"
	"strings"

	"github.com/turborg/turborg/internal/agent"
)

// rawSender is the minimal connector surface the Actor needs: a single raw
// upstream write. The Connector satisfies it via SendRaw. Narrowing to this
// interface keeps the Actor unit-testable without a live upstream socket.
type rawSender interface {
	SendRaw(line string) error
}

// Actor implements agent.Actor over the IRC wire. It translates the
// connector-agnostic action surface into raw IRC commands sent upstream via
// the connector's SendRaw, so the skill engine can moderate any connector
// uniformly.
type Actor struct {
	send rawSender
}

// NewActor builds an IRC Actor bound to a connector.
func NewActor(c *Connector) *Actor { return &Actor{send: c} }

var _ agent.Actor = (*Actor)(nil)

func (a *Actor) Say(channel, text string) error {
	return a.sendLines(CmdPrivmsg, channel, text)
}

func (a *Actor) Notice(target, text string) error {
	return a.sendLines(CmdNotice, target, text)
}

// sendLines emits text as one IRC message per line. An embedded newline must
// never reach the wire intact: CRLF terminates an IRC command, so a multi-line
// reply would otherwise truncate at the first line and inject the remainder as
// raw commands (CRLF injection). Splitting here makes a multi-line flow reply
// render as consecutive messages and neutralizes the injection vector. Blank
// lines are skipped.
func (a *Actor) sendLines(cmd, target, text string) error {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if err := a.send.SendRaw(fmt.Sprintf("%s %s :%s", cmd, target, line)); err != nil {
			return err
		}
	}
	return nil
}

func (a *Actor) Kick(channel, nick, reason string) error {
	line := fmt.Sprintf("%s %s %s", CmdKick, channel, nick)
	if reason != "" {
		line += " :" + reason
	}
	return a.send.SendRaw(line)
}

// Ban sets +b on a resolved mask. A bare nick is widened to a nick!*@* mask
// (the connector tracks no per-member host, so this is the best available
// identity); a value that already looks like a mask is used as given.
func (a *Actor) Ban(channel, mask string) error {
	return a.SetMode(channel, "+b", resolveBanMask(mask))
}

func (a *Actor) SetMode(channel, modes string, args ...string) error {
	line := fmt.Sprintf("%s %s %s", CmdMode, channel, modes)
	if len(args) > 0 {
		line += " " + strings.Join(args, " ")
	}
	return a.send.SendRaw(strings.TrimRight(line, " "))
}

func (a *Actor) Op(channel, nick string) error    { return a.SetMode(channel, "+o", nick) }
func (a *Actor) Voice(channel, nick string) error { return a.SetMode(channel, "+v", nick) }

func (a *Actor) Topic(channel, topic string) error {
	return a.send.SendRaw(fmt.Sprintf("%s %s :%s", CmdTopic, channel, topic))
}

// Invite uses IRC argument order (INVITE <nick> <channel>).
func (a *Actor) Invite(channel, nick string) error {
	return a.send.SendRaw(fmt.Sprintf("%s %s %s", CmdInvite, nick, channel))
}

// resolveBanMask widens a bare nick to a nick!*@* mask; a value already
// containing mask syntax (! or @) is returned unchanged.
func resolveBanMask(mask string) string {
	if strings.ContainsAny(mask, "!@") {
		return mask
	}
	return mask + "!*@*"
}
