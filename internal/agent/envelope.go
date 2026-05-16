package agent

import (
	"time"

	"github.com/google/uuid"
)

// InboundEnvelope is a connector-originated message, normalized for the
// agent. Every connector produces these, every handler consumes them, and
// protocol-specific extras live in Metadata so the agent surface stays
// uniform across IRC / Discord / Telegram / etc.
type InboundEnvelope struct {
	ID                uuid.UUID
	Connector         string
	ConnectorInstance string
	Channel           string
	Sender            string
	Text              string
	IsDirect          bool
	Command           string
	Args              []string
	Raw               string
	Metadata          map[string]any
	ReceivedAt        time.Time
}

// OutboundEnvelope is a handler-produced message the connector should
// deliver. ReplyTo carries the InboundEnvelope.ID this message responds to;
// connectors that support threaded replies (Discord, Slack) use it,
// flat-protocol connectors (IRC) ignore it.
type OutboundEnvelope struct {
	ID                uuid.UUID
	Connector         string
	ConnectorInstance string
	Channel           string
	Text              string
	ReplyTo           *uuid.UUID
	Metadata          map[string]any
}

func NewInbound(connector, channel, sender, text string) *InboundEnvelope {
	return &InboundEnvelope{
		ID:                uuid.New(),
		Connector:         connector,
		ConnectorInstance: "default",
		Channel:           channel,
		Sender:            sender,
		Text:              text,
		Metadata:          map[string]any{},
		ReceivedAt:        time.Now().UTC(),
	}
}

// ReplyTo builds an OutboundEnvelope addressed back at the source of in.
// If the source was a direct/DM message, the reply is routed to the
// sender instead of the channel.
func ReplyTo(in *InboundEnvelope, text string) *OutboundEnvelope {
	channel := in.Channel
	if in.IsDirect {
		channel = in.Sender
	}
	id := in.ID
	return &OutboundEnvelope{
		ID:                uuid.New(),
		Connector:         in.Connector,
		ConnectorInstance: in.ConnectorInstance,
		Channel:           channel,
		Text:              text,
		ReplyTo:           &id,
		Metadata:          map[string]any{},
	}
}
