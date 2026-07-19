package server

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/statepush"
)

// tenantStateDebounce coalesces bursty transitions (a multi-channel JOIN, a
// reconnect flurry) into one POST. Mirrors the single-tenant default.
const tenantStateDebounce = 250 * time.Millisecond

// buildTenantStateEmitter wires a connector-state emitter for one pooled tenant.
// Unlike the single-instance path (which PUTs to STATE_WEBHOOK_URL), the pool
// POSTs each tenant's snapshot straight to the control plane's per-tenant
// receiver — it already holds the control-plane URL + token (it polls the feed
// from there), and the receiver authorizes it via the host token. Returns nil
// when no control plane is configured (the file-source path) or the tenant has
// no connectors, so state-sync is simply off.
func buildTenantStateEmitter(conns []agent.Connector, turborgID, controlPlaneURL, token string, log *slog.Logger) *statepush.Emitter {
	if controlPlaneURL == "" || turborgID == "" || len(conns) == 0 {
		return nil
	}
	url := strings.TrimRight(controlPlaneURL, "/") + "/turborgs/" + turborgID + "/state"
	client := statepush.NewClientWithMethod(url, token, http.MethodPost, log)
	emitter := statepush.NewEmitter(client, buildTenantSnapshot(conns), tenantStateDebounce, log)
	wireTenantStateEmitter(conns, emitter)
	return emitter
}

// buildTenantSnapshot returns the authoritative-state builder the emitter calls
// before each POST. It spans every connector the tenant runs: IRC contributes
// the rich, IRC-native snapshot (nick / channels / state machine); every other
// connector that exposes its live state (agent.StateReporter) contributes a
// generic snapshot with the connector-agnostic state mapped to the wire vocab.
func buildTenantSnapshot(conns []agent.Connector) statepush.SnapshotBuilder {
	return func() statepush.Snapshot {
		out := make(map[string]statepush.ConnectorSnapshot, len(conns))
		for _, conn := range conns {
			switch c := conn.(type) {
			case *irc.Connector:
				out[c.Name()] = ircConnectorSnapshot(c)
			default:
				if st, ok := conn.(agent.StateReporter); ok {
					out[conn.Name()] = genericConnectorSnapshot(conn, st)
				}
			}
		}
		return statepush.Snapshot{Connectors: out}
	}
}

// ircConnectorSnapshot builds the IRC connector's authoritative-state snapshot.
// Mirrors runtime.buildIRCSnapshot (single-tenant): the queued preferred nick
// wins when set (the user's stated intent for the next registration), else the
// live current nick.
func ircConnectorSnapshot(conn *irc.Connector) statepush.ConnectorSnapshot {
	machine := conn.UpstreamState()
	nick := conn.PreferredNick()
	if nick == "" {
		nick = conn.CurrentNick()
	}
	wanted := conn.WantedChannels().Snapshot()
	channels := make([]statepush.ChannelSnapshot, 0, len(wanted))
	for _, w := range wanted {
		channels = append(channels, statepush.NewChannelSnapshot(w.Name, w.Key))
	}
	return statepush.ConnectorSnapshot{
		State:       string(machine.State()),
		Since:       machine.EnteredAt().UTC(),
		Channels:    channels,
		Nick:        nick,
		DesiredNick: conn.DesiredNick(),
		Reason:      machine.ServerReason(),
	}
}

// genericConnectorSnapshot builds a connector-agnostic snapshot for any
// non-IRC connector that reports its live state. Channels is always a non-nil
// (empty) slice so the wire carries `[]`, never `null`. Nick is the bot's own
// display name when the connector exposes one.
func genericConnectorSnapshot(conn agent.Connector, reporter agent.StateReporter) statepush.ConnectorSnapshot {
	st := reporter.ConnectorState()
	var nick string
	if named, ok := conn.(interface{ BotName() string }); ok {
		nick = named.BotName()
	}
	return statepush.ConnectorSnapshot{
		State:    mapConnectorStateToWire(st.State),
		Since:    st.Since.UTC(),
		Reason:   st.Reason,
		Channels: []statepush.ChannelSnapshot{},
		Nick:     nick,
	}
}

// mapConnectorStateToWire translates the connector-agnostic state vocabulary
// into the wire state values the control plane validates against. This mapping
// is a locked contract — changing a right-hand value breaks the receiver.
func mapConnectorStateToWire(s string) string {
	switch s {
	case "connected":
		return "registered"
	case "connecting":
		return "connecting"
	case "suspended":
		return "disconnected_by_user"
	case "error":
		return "disconnected_auth_failed"
	case "disconnected":
		return "disconnected_transient"
	default:
		return "disconnected_transient"
	}
}

// wireTenantStateEmitter fires the emitter's NotifyChange on each connector's
// authoritative-state sources. IRC wires its three native sources (upstream
// state-machine transitions, wanted-channels mutations, preferred-nick
// changes); any other connector that supports push notification
// (agent.StateSubscriber) forwards its state-change hook to NotifyChange.
func wireTenantStateEmitter(conns []agent.Connector, emitter *statepush.Emitter) {
	if emitter == nil {
		return
	}
	notify := emitter.NotifyChange
	for _, conn := range conns {
		switch c := conn.(type) {
		case *irc.Connector:
			c.UpstreamState().Subscribe(func(irc.UpstreamStateChange) { notify() })
			c.WantedChannels().SetOnChange(notify)
			c.SetPreferredNickChangeHook(notify)
		default:
			if sub, ok := conn.(agent.StateSubscriber); ok {
				sub.OnStateChange(emitter.NotifyChange)
			}
		}
	}
}
