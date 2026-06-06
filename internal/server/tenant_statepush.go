package server

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

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
// when no control plane is configured (the file-source path), so state-sync is
// simply off.
func buildTenantStateEmitter(conn *irc.Connector, turborgID, controlPlaneURL, token string, log *slog.Logger) *statepush.Emitter {
	if controlPlaneURL == "" || turborgID == "" {
		return nil
	}
	url := strings.TrimRight(controlPlaneURL, "/") + "/turborgs/" + turborgID + "/state"
	client := statepush.NewClientWithMethod(url, token, http.MethodPost, log)
	emitter := statepush.NewEmitter(client, buildIRCSnapshot(conn), tenantStateDebounce, log)
	wireStatePushEmitter(conn, emitter)
	return emitter
}

// buildIRCSnapshot returns the authoritative-state builder the emitter calls
// before each POST. Mirrors runtime.buildIRCSnapshot (single-tenant): the
// queued preferred nick wins when set (the user's stated intent for the next
// registration), else the live current nick.
func buildIRCSnapshot(conn *irc.Connector) statepush.SnapshotBuilder {
	return func() statepush.Snapshot {
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
		return statepush.Snapshot{
			Connectors: map[string]statepush.ConnectorSnapshot{
				conn.Name(): {
					State:    string(machine.State()),
					Since:    machine.EnteredAt().UTC(),
					Channels: channels,
					Nick:     nick,
					Reason:   machine.ServerReason(),
				},
			},
		}
	}
}

// wireStatePushEmitter fires the emitter's NotifyChange on the three
// authoritative-state sources: upstream state-machine transitions,
// wanted-channels mutations, and preferred-nick changes.
func wireStatePushEmitter(conn *irc.Connector, emitter *statepush.Emitter) {
	if conn == nil || emitter == nil {
		return
	}
	notify := emitter.NotifyChange
	conn.UpstreamState().Subscribe(func(irc.UpstreamStateChange) { notify() })
	conn.WantedChannels().SetOnChange(notify)
	conn.SetPreferredNickChangeHook(notify)
}
