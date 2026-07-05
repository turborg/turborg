package web

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/turborg/turborg/internal/agent"
)

// errUnsupported is returned by the moderation actions a private console can't
// perform. The Actor contract explicitly allows a connector to error on
// actions it doesn't support; these wire to real room moderation once a public
// room ships.
var errUnsupported = errors.New("web: action not supported in console")

// Actor implements agent.Actor over the web-chat connector. Say/Notice fan bot
// output to attached clients and publish MESSAGE_SENT so the shared store
// submitter persists it — the same reason the IRC gateway republishes its own
// `say` ops. Moderation actions deny in the private console.
type Actor struct {
	conn *Connector
}

// NewActor builds a web Actor bound to a connector.
func NewActor(c *Connector) *Actor { return &Actor{conn: c} }

var _ agent.Actor = (*Actor)(nil)

// Say delivers text to the room as a bot message: it fans the frame to every
// client and publishes MESSAGE_SENT for persistence. The channel argument is
// ignored — a console has one room — so skill/flow output always lands there.
func (a *Actor) Say(_, text string) error {
	a.emit(text)
	return nil
}

// Notice renders identically to Say in the console (there is no separate notice
// styling); it exists to satisfy the Actor contract.
func (a *Actor) Notice(_, text string) error {
	a.emit(text)
	return nil
}

// emit broadcasts a bot frame and publishes MESSAGE_SENT so the runtime's store
// submitter persists the bot's own output (the connector doesn't subscribe to
// its own bus to re-broadcast, so there is no double send).
func (a *Actor) emit(text string) {
	a.conn.broadcast(map[string]any{
		"op":     "message",
		"kind":   "bot",
		"sender": a.conn.settings.BotNick,
		"text":   text,
		"ts":     time.Now().Unix(),
		"id":     uuid.NewString(),
	})
	if a.conn.events != nil {
		a.conn.events.Publish(context.Background(), &agent.Event{
			Type: agent.EventMessageSent,
			Time: time.Now(),
			Fields: map[string]any{
				"connector": "web",
				"channel":   a.conn.settings.Room,
				"sender":    a.conn.settings.BotNick,
				"text":      text,
			},
		})
	}
}

func (a *Actor) Kick(_, _, _ string) error              { return errUnsupported }
func (a *Actor) Ban(_, _ string) error                  { return errUnsupported }
func (a *Actor) SetMode(_, _ string, _ ...string) error { return errUnsupported }
func (a *Actor) Op(_, _ string) error                   { return errUnsupported }
func (a *Actor) Voice(_, _ string) error                { return errUnsupported }
func (a *Actor) Topic(_, _ string) error                { return errUnsupported }
func (a *Actor) Invite(_, _ string) error               { return errUnsupported }
