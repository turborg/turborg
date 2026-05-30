package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
)

// Ring-buffer caps for replay on new client attach. Bumping these is a
// coordinated change with downstream UIs that size their local backfill
// against the same numbers — see docs/ui/PLAN.md.
const (
	serverLogCap  = 100
	channelLogCap = 200
)

// IRCBridge is the narrow slice of the IRC connector the gateway
// depends on. Keeps the package boundary clean — the gateway never
// reaches into IRC-specific internals beyond these methods.
type IRCBridge interface {
	CurrentNick() string
	State() *irc.ChannelState
	SendRaw(line string) error
	// ClientLimits returns the operator-policy struct the gateway
	// consults before forwarding web-originated actions. Mirror of the
	// bouncer's policy gate — same struct, two surfaces.
	ClientLimits() irc.ClientLimits
	// OutboundThrottle returns the per-target PRIVMSG throttle (may
	// be nil = unrestricted). Shared with the bouncer so a user with
	// both surfaces open shares one bucket per target.
	OutboundThrottle() *irc.Throttle
	// UpstreamState returns the per-connector upstream state machine.
	// The gateway subscribes to it during Subscribe so it can emit
	// connector.state_changed events to attached WS clients on every
	// state transition, and gate WS send_message frames on the
	// `registered` state.
	UpstreamState() *irc.UpstreamStateMachine
}

// Sender is the agent surface for outbound envelopes — the gateway
// hands off `say` ops through this so the bouncer + EventBus see the
// same MESSAGE_SENT signal the rest of the agent expects.
type Sender interface {
	Send(env *agent.OutboundEnvelope) error
}

// Options configures a Gateway. Host/Port carry the listen address;
// the rest is opt-in.
type Options struct {
	Host        string
	Port        int
	Verifier    TokenVerifier
	RateLimiter *irc.RateLimiter
	Log         *slog.Logger

	// IdleShutdownSeconds + OnIdleShutdown wire the free-tier auto-pause
	// path: when both are set and the client count drops to zero, the
	// gateway starts a timer; if no client reconnects within the window,
	// OnIdleShutdown fires (the SaaS sidecar maps that to a docker
	// stop).
	IdleShutdownSeconds int
	OnIdleShutdown      func()

	// OnClientAttached fires once per successful WS handshake, with a
	// stable reason identifier. nil = disabled. Wired by runtime so an
	// external observer can learn that a dashboard client is actively
	// using this agent — distinct from "bot is alive" log signal.
	OnClientAttached func(reason string)

	// MessageStore is the read/write seam for channel history. The
	// gateway calls Submit on every observed inbound + outbound
	// channel message (so DB-backed deployments mirror durably) and
	// Recent on attach + on the `history` WS op for scrollback. nil
	// means "no history available" — clients still see live traffic,
	// but replay and scrollback frames are empty.
	MessageStore messages.Store

	// LLMProvider powers /tb subcommands. Nil = /tb is unavailable.
	LLMProvider llm.Provider

	// TBSummarizeMaxMessages caps how many messages /tb summarize can
	// consume per invocation. 0 = feature disabled.
	TBSummarizeMaxMessages int
}

// Gateway is an HTTP server exposing /ws (WebSocket), /health, /metrics,
// and the bundled static reference UI at /. Wire it up by calling
// Subscribe(agent.EventBus) before Serve; Subscribe registers the
// EventBus handlers that fan out to connected clients.
type Gateway struct {
	opts   Options
	bridge IRCBridge
	sender Sender
	bus    *agent.EventBus
	log    *slog.Logger

	mu       sync.Mutex
	clients  map[*client]struct{}
	server   *http.Server
	listener net.Listener
	cancel   context.CancelFunc
	started  time.Time

	// stateSub is the gateway's subscription to the per-connector
	// upstream state machine. Cleaned up in Stop so a re-Subscribed
	// gateway doesn't leak handlers across lifecycle restarts.
	stateSub *irc.Subscription

	// Server-log ring of recent state-level notices (connection
	// transitions, etc). Channel-message history lives in
	// opts.MessageStore instead — the gateway no longer maintains a
	// parallel channelLog ring, so the bouncer + WS gateway share one
	// canonical history view.
	logMu     sync.Mutex
	serverLog []map[string]any

	idleMu   sync.Mutex
	idleStop chan struct{} // closed by cancelIdleTimer

	metMu   sync.Mutex
	metrics struct {
		connections       uint64
		authFailures      uint64
		messagesForwarded uint64
	}
}

type client struct {
	conn *websocket.Conn
	addr string
}

func New(bridge IRCBridge, sender Sender, opts Options) (*Gateway, error) {
	if bridge == nil {
		return nil, errors.New("web: IRCBridge is required")
	}
	if sender == nil {
		return nil, errors.New("web: Sender is required")
	}
	if opts.Verifier == nil {
		return nil, errors.New("web: TokenVerifier is required")
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	// Port 0 deliberately stays 0 so net.Listen picks an OS-assigned
	// port — required for parallel tests, harmless in production where
	// the runtime composer always passes an explicit port from
	// TURBORG_GATEWAY_PORT (default 8765 in config.Settings).
	g := &Gateway{
		opts:    opts,
		bridge:  bridge,
		sender:  sender,
		log:     opts.Log,
		clients: map[*client]struct{}{},
		started: time.Now(),
	}
	return g, nil
}

// Subscribe registers handlers on bus for every event the gateway
// fans out. Call before Serve; calling more than once unsubscribes
// the prior set and re-registers. Also subscribes to the IRC
// upstream state machine so connector.state_changed events flow to
// WS clients on every transition.
func (g *Gateway) Subscribe(bus *agent.EventBus) {
	g.mu.Lock()
	g.bus = bus
	g.mu.Unlock()
	bus.Subscribe(agent.EventMessage, g.onMessage)
	bus.Subscribe(agent.EventMessageSent, g.onMessageSent)
	bus.Subscribe(agent.EventUserJoin, g.onUserJoin)
	bus.Subscribe(agent.EventUserLeave, g.onUserLeave)
	bus.Subscribe(agent.EventUserKicked, g.onUserKicked)
	bus.Subscribe(agent.EventUserNickChange, g.onUserNick)
	bus.Subscribe(agent.EventTopicChanged, g.onTopic)
	bus.Subscribe(agent.EventChannelNames, g.onNames)
	bus.Subscribe(agent.EventServerNotice, g.onServerNotice)
	bus.Subscribe(agent.EventModeChanged, g.onModeChanged)
	bus.Subscribe(agent.EventWhoisResult, g.onWhoisResult)
	bus.Subscribe(agent.EventListResult, g.onListResult)
	bus.Subscribe(agent.EventWhoResult, g.onWhoResult)
	bus.Subscribe(agent.EventJoinFailed, g.onJoinFailed)

	if machine := g.bridge.UpstreamState(); machine != nil {
		sub := machine.Subscribe(g.onUpstreamStateChange)
		g.mu.Lock()
		if g.stateSub != nil {
			g.stateSub.Unsubscribe()
		}
		g.stateSub = sub
		g.mu.Unlock()
	}
}

// Handler returns the gateway's HTTP routes — /ws (WebSocket), /health,
// /metrics, and the static UI. Serve mounts it on the gateway's own listener
// (dedicated / single-tenant); the pooled runtime mounts the SAME handler
// behind its per-tenant router. One route set + one handler chain for both
// modes — a change here affects both, no reimplementation.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.handleWS)
	mux.HandleFunc("/health", g.handleHealth)
	mux.HandleFunc("/metrics", g.handleMetrics)
	if staticSub, err := fs.Sub(staticFS, "static"); err == nil {
		mux.Handle("/", http.FileServer(http.FS(staticSub)))
	}
	return mux
}

// Serve binds the listener and blocks until ctx is canceled. Returns
// http.ErrServerClosed on clean shutdown, or any net.Listen error.
func (g *Gateway) Serve(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", g.opts.Host, g.opts.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("web listen %s: %w", addr, err)
	}
	srvCtx, cancel := context.WithCancel(ctx)
	g.mu.Lock()
	g.listener = l
	g.cancel = cancel
	g.mu.Unlock()

	srv := &http.Server{
		Handler:           g.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	g.mu.Lock()
	g.server = srv
	g.mu.Unlock()

	// Shutdown gets its own bounded context — using srvCtx (already
	// cancelled by the time this fires) would short-circuit the
	// graceful close, so gosec G118 is intentional here.
	go func() { //nolint:gosec // G118
		<-srvCtx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
		_ = g.closeAllClients()
	}()

	g.log.Info("web gateway listening", "addr", l.Addr().String())
	err = srv.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Addr returns the resolved listen address; useful for tests using
// port 0.
func (g *Gateway) Addr() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.listener == nil {
		return ""
	}
	return g.listener.Addr().String()
}

// Stop signals the gateway to shut down. Safe to call multiple times.
func (g *Gateway) Stop() {
	g.mu.Lock()
	cancel := g.cancel
	sub := g.stateSub
	g.cancel = nil
	g.stateSub = nil
	g.mu.Unlock()
	g.cancelIdleTimer()
	if sub != nil {
		sub.Unsubscribe()
	}
	if cancel != nil {
		cancel()
	}
}

// --- handlers --------------------------------------------------------------

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	token := extractToken(r)

	if g.opts.RateLimiter != nil && ip != "" && g.opts.RateLimiter.IsLocked(ip) {
		remaining := g.opts.RateLimiter.TimeUntilUnlock(ip)
		g.log.Warn("web auth blocked", "ip", ip, "remaining", remaining)
		http.Error(w, "Too many attempts", http.StatusTooManyRequests)
		return
	}

	if !g.opts.Verifier.Verify(token) {
		if g.opts.RateLimiter != nil && ip != "" {
			outcome := g.opts.RateLimiter.RecordFailure(ip)
			switch {
			case outcome.Locked:
				g.log.Warn("web auth lockout", "ip", ip)
			default:
				g.log.Info("web auth fail", "ip", ip, "count", outcome.Count)
			}
		} else {
			g.log.Info("web auth fail", "ip", ip)
		}
		g.metMu.Lock()
		g.metrics.authFailures++
		g.metMu.Unlock()
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}
	if g.opts.RateLimiter != nil && ip != "" {
		g.opts.RateLimiter.RecordSuccess(ip)
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // origin checks are the deployment's job
	})
	if err != nil {
		g.log.Debug("web ws accept", "err", err)
		return
	}

	c := &client{conn: conn, addr: ip}
	g.mu.Lock()
	g.clients[c] = struct{}{}
	g.mu.Unlock()
	g.cancelIdleTimer()
	g.metMu.Lock()
	g.metrics.connections++
	g.metMu.Unlock()
	g.log.Info("web auth success", "ip", ip)
	if g.opts.OnClientAttached != nil {
		g.opts.OnClientAttached("ws_attach")
	}

	connectedAt := time.Now()
	defer func() {
		g.mu.Lock()
		delete(g.clients, c)
		empty := len(g.clients) == 0
		g.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "")
		g.log.Info("web disconnect", "ip", ip,
			"session", time.Since(connectedAt).Round(time.Second))
		if empty {
			g.scheduleIdleShutdown()
		}
	}()

	ctx := r.Context()
	g.sendState(ctx, c)
	g.sendInitialConnectorState(ctx, c)
	g.replayBuffers(ctx, c)
	g.readLoop(ctx, c)
}

// sendInitialConnectorState publishes a one-shot connector.state_changed
// to the newly-attached client carrying the upstream state machine's
// CURRENT value. onUpstreamStateChange only fires on transitions, so a
// client that connects mid-outage (or after registration completed
// before the WS handshake finished) would otherwise never learn the
// upstream isn't registered, the SPA's pill would stay green, and
// users would have to wait for the next transition to find out.
func (g *Gateway) sendInitialConnectorState(ctx context.Context, c *client) {
	machine := g.bridge.UpstreamState()
	if machine == nil {
		return
	}
	state := machine.State()
	g.sendTo(ctx, c, map[string]any{
		"op":            "connector.state_changed",
		"state":         string(state),
		"prior_state":   "",
		"message":       irc.DescribeUpstreamState(state, "", machine.ServerReason()),
		"severity":      irc.SeverityForUpstreamState(state),
		"server_reason": machine.ServerReason(),
	})
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	body := map[string]any{
		"status":         "ok",
		"uptime_seconds": int(time.Since(g.started).Seconds()),
		"ws_clients":     g.clientCount(),
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (g *Gateway) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	g.metMu.Lock()
	conns := g.metrics.connections
	fails := g.metrics.authFailures
	fwd := g.metrics.messagesForwarded
	g.metMu.Unlock()

	lines := []string{
		"# HELP turborg_ws_connections_total Total successful WS auths.",
		"# TYPE turborg_ws_connections_total counter",
		fmt.Sprintf("turborg_ws_connections_total %d", conns),
		"# HELP turborg_ws_auth_failures_total Total failed WS auth attempts.",
		"# TYPE turborg_ws_auth_failures_total counter",
		fmt.Sprintf("turborg_ws_auth_failures_total %d", fails),
		"# HELP turborg_messages_forwarded_total Total IRC messages forwarded to WS.",
		"# TYPE turborg_messages_forwarded_total counter",
		fmt.Sprintf("turborg_messages_forwarded_total %d", fwd),
		"# HELP turborg_ws_clients_current Currently connected WS clients.",
		"# TYPE turborg_ws_clients_current gauge",
		fmt.Sprintf("turborg_ws_clients_current %d", g.clientCount()),
		"# HELP turborg_uptime_seconds Process uptime since gateway init.",
		"# TYPE turborg_uptime_seconds gauge",
		fmt.Sprintf("turborg_uptime_seconds %d", int(time.Since(g.started).Seconds())),
	}
	_, _ = fmt.Fprintln(w, strings.Join(lines, "\n"))
}

// --- state replay ---------------------------------------------------------

func (g *Gateway) sendState(ctx context.Context, c *client) {
	joined := g.bridge.State().JoinedChannels()
	channels := make([]map[string]any, 0, len(joined))
	for _, info := range joined {
		members := make([]map[string]any, 0, len(info.Members))
		for nick, mode := range info.Members {
			members = append(members, map[string]any{"nick": nick, "mode": mode})
		}
		channels = append(channels, map[string]any{
			"name":       info.Name,
			"topic":      info.Topic,
			"topicSetBy": info.TopicSetBy,
			"topicSetAt": info.TopicSetAt,
			"members":    members,
		})
	}
	g.sendTo(ctx, c, map[string]any{
		"op":       "state",
		"nick":     g.bridge.CurrentNick(),
		"channels": channels,
	})
}

func (g *Gateway) replayBuffers(ctx context.Context, c *client) {
	// Server log: a small ring of state-level notices the gateway
	// itself produced (connection transitions, etc) — kept in-process
	// because they're per-instance, not per-message.
	g.logMu.Lock()
	serverSnap := append([]map[string]any{}, g.serverLog...)
	g.logMu.Unlock()
	for _, entry := range serverSnap {
		dup := mapCopy(entry)
		dup["replayed"] = true
		g.sendTo(ctx, c, dup)
	}

	// Channel-message history comes from the shared store. Per joined
	// channel, fetch the last channelLogCap (default 200) entries and
	// stream them oldest-first as `replayed: true` frames. The store
	// returns newest-first; the loop walks in reverse so the UI sees
	// chronological order.
	store := g.opts.MessageStore
	if store == nil {
		return
	}
	for _, info := range g.bridge.State().JoinedChannels() {
		msgs, err := store.Recent(ctx, info.Name, time.Time{}, channelLogCap)
		if err != nil {
			g.log.Debug("history fetch on attach", "channel", info.Name, "err", err)
			continue
		}
		for i := len(msgs) - 1; i >= 0; i-- {
			m := msgs[i]
			g.sendTo(ctx, c, map[string]any{
				"op":       "message",
				"channel":  m.Channel,
				"nick":     m.Nick,
				"text":     m.Text,
				"ts":       m.TS.Unix(),
				"replayed": true,
			})
		}
	}
}

// --- inbound dispatch -----------------------------------------------------

func (g *Gateway) readLoop(ctx context.Context, c *client) {
	for {
		_, raw, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		g.dispatchInbound(ctx, c, payload)
	}
}

// allowByPolicy consults the bridge's ClientLimits for the IRC command
// the WS op maps to. Returns (true, "") when permitted, (false, reason)
// when the gateway should drop the op and surface the rejection back to
// the client via a policy_denied event.
//
// Mirrors irc.Bouncer's policy gate so the two client-action surfaces
// (bouncer + WS) enforce the same rules.
func (g *Gateway) allowByPolicy(ircCmd string) (allow bool, reason string, kind string) {
	limits := g.bridge.ClientLimits()
	currentChannels := 0
	if ircCmd == irc.CmdJoin {
		if st := g.bridge.State(); st != nil {
			currentChannels = st.Count()
		}
	}
	allow, reason = limits.AllowCommand(ircCmd, currentChannels)
	kind = irc.CapHitKind(ircCmd)
	return allow, reason, kind
}

// denyAction sends a policy_denied event to the originating client and
// emits a cap_hit structured log line for telemetry pipelines. The log
// line is the canonical signal a sidecar log-watcher (or any future
// metrics scraper) consumes — never log the user-facing reason from any
// other path, so cap_hit stays the single source of truth.
//
// sourceTarget carries the user's intent so the test UI (and any
// downstream consumer) can render the rejection in the originating
// context: the channel they were trying to join, the new nick they
// were trying to change to. Empty when the source op has no inherent
// target (e.g. a malformed frame).
func (g *Gateway) denyAction(ctx context.Context, c *client, sourceOp, sourceTarget, kind, reason string) {
	g.sendTo(ctx, c, map[string]any{
		"op":            "policy_denied",
		"source_op":     sourceOp,
		"source_target": sourceTarget,
		"kind":          kind,
		"reason":        reason,
	})
	g.log.Info("cap_hit", "surface", "ws_gateway", "kind", kind, "source_op", sourceOp)
}

// rateLimited is the per-target outbound-throttle counterpart to
// denyAction. Emits {op: "rate_limited", ...} so the UI can render an
// inline "wait Ns" badge next to the user's message, plus the same
// cap_hit telemetry line for aggregation.
func (g *Gateway) rateLimited(ctx context.Context, c *client, target string, retryAfter time.Duration) {
	seconds := int(retryAfter.Round(time.Second) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	g.sendTo(ctx, c, map[string]any{
		"op":          "rate_limited",
		"target":      target,
		"retry_after": seconds,
		"reason":      "outbound rate limit",
	})
	g.log.Info("cap_hit",
		"surface", "ws_gateway",
		"kind", "outbound_rate",
		"target", target,
		"retry_after_s", seconds,
	)
}

// dispatchInbound is a per-op switch; complexity grows with the WS protocol.
//
//nolint:gocyclo
func (g *Gateway) dispatchInbound(ctx context.Context, c *client, p map[string]any) {
	op, _ := p["op"].(string)
	switch op {
	case "say":
		ch, _ := p["channel"].(string)
		text, _ := p["text"].(string)
		if ch == "" || text == "" {
			return
		}
		// Gate on upstream state — if the connector isn't registered
		// the WS send_message would either silently disappear (state
		// != registered, supervisor reconnecting) or race against
		// the write side. Refuse with a send_message.rejected frame
		// the UI can render against its optimistically-shown bubble.
		if machine := g.bridge.UpstreamState(); machine != nil {
			if state := machine.State(); state != irc.UpstreamStateRegistered {
				g.rejectSend(ctx, c, ch, text, string(state),
					irc.DescribeUpstreamState(state, "", machine.ServerReason()))
				return
			}
		}
		// Per-target outbound throttle — shared with the bouncer's
		// PRIVMSG path. Bucket key is the target so a chatty user
		// pestering one channel doesn't deny their other channels.
		if throttle := g.bridge.OutboundThrottle(); throttle != nil {
			if res := throttle.AllowWithReason(ch); !res.Allow {
				g.rateLimited(ctx, c, ch, res.RetryAfter)
				return
			}
		}
		if err := g.sender.Send(&agent.OutboundEnvelope{
			Connector:         "irc",
			ConnectorInstance: "default",
			Channel:           ch,
			Text:              text,
		}); err != nil {
			g.log.Warn("web say send", "err", err)
			// Write-error race fix: per-frame rejection NOTICE
			// travels with the failing write rather than leaving the
			// optimistically-rendered bubble in an indeterminate
			// state. State may already have flipped to non-registered;
			// describe it if so, otherwise surface the raw error.
			rejReason := "send_failed"
			rejMessage := err.Error()
			if machine := g.bridge.UpstreamState(); machine != nil {
				if state := machine.State(); state != irc.UpstreamStateRegistered {
					rejReason = string(state)
					rejMessage = irc.DescribeUpstreamState(state, "", machine.ServerReason())
				}
			}
			g.rejectSend(ctx, c, ch, text, rejReason, rejMessage)
			return
		}
		// Publish MESSAGE_SENT so the gateway's own onMessageSent
		// handler echoes the message back to every connected WS client
		// (including the one that typed it). Without this, the sender
		// types into the box, the message goes to IRC and to other WS
		// clients via the bus, but the originator never sees it in
		// their own UI because IRC servers don't echo own PRIVMSGs and
		// the only path that would have triggered MESSAGE_SENT — the
		// agent's command dispatch — isn't on the WS "say" code path.
		g.mu.Lock()
		bus := g.bus
		g.mu.Unlock()
		if bus != nil {
			bus.Publish(context.Background(), &agent.Event{
				Type: agent.EventMessageSent,
				Fields: map[string]any{
					"connector": "irc",
					"channel":   ch,
					"sender":    g.bridge.CurrentNick(),
					"text":      text,
				},
			})
		}
	case "join":
		if ch, _ := p["channel"].(string); ch != "" {
			if allow, reason, kind := g.allowByPolicy(irc.CmdJoin); !allow {
				g.denyAction(ctx, c, op, ch, kind, reason)
				return
			}
			_ = g.bridge.SendRaw(irc.CmdJoin + " " + ch)
		}
	case "part":
		if ch, _ := p["channel"].(string); ch != "" {
			_ = g.bridge.SendRaw(irc.CmdPart + " " + ch)
		}
	case "nick":
		if n, _ := p["nick"].(string); n != "" {
			if allow, reason, kind := g.allowByPolicy(irc.CmdNick); !allow {
				g.denyAction(ctx, c, op, n, kind, reason)
				return
			}
			_ = g.bridge.SendRaw(irc.CmdNick + " " + n)
		}
	case "mode":
		ch, _ := p["channel"].(string)
		modes, _ := p["modes"].(string)
		target, _ := p["target"].(string)
		if ch == "" || modes == "" {
			return
		}
		line := irc.CmdMode + " " + ch + " " + modes
		if target != "" {
			line += " " + target
		}
		_ = g.bridge.SendRaw(line)
	case "kick":
		ch, _ := p["channel"].(string)
		victim, _ := p["nick"].(string)
		reason, _ := p["reason"].(string)
		if ch == "" || victim == "" {
			return
		}
		line := irc.CmdKick + " " + ch + " " + victim
		if reason != "" {
			line += " :" + reason
		}
		_ = g.bridge.SendRaw(line)
	case "whois":
		if n, _ := p["nick"].(string); n != "" {
			_ = g.bridge.SendRaw(irc.CmdWhois + " " + n)
		}
	case "topic":
		ch, _ := p["channel"].(string)
		if ch == "" {
			return
		}
		if t, present := p["topic"].(string); present {
			_ = g.bridge.SendRaw(irc.CmdTopic + " " + ch + " :" + t)
		} else {
			_ = g.bridge.SendRaw(irc.CmdTopic + " " + ch)
		}
	case "list":
		pattern, _ := p["pattern"].(string)
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			_ = g.bridge.SendRaw(irc.CmdList)
		} else {
			_ = g.bridge.SendRaw(irc.CmdList + " " + pattern)
		}
	case "who":
		if t, _ := p["target"].(string); strings.TrimSpace(t) != "" {
			_ = g.bridge.SendRaw(irc.CmdWho + " " + strings.TrimSpace(t))
		}
	case "raw":
		line, _ := p["line"].(string)
		if line == "" || strings.ContainsAny(line, "\r\n") {
			return
		}
		line = strings.TrimPrefix(line, "/")
		_ = g.bridge.SendRaw(line)
	case "history":
		g.handleHistoryOp(ctx, c, p)
	case "tb":
		g.handleTBOp(ctx, c, p)
	}
}

// handleHistoryOp answers a `{op: "history", channel, before, limit}`
// scrollback request from a WS client. The infinite-scroll UI in the
// reference + appui front-ends fires this when the user nears the top
// of the channel pane. Wire shape:
//
//	inbound:  {op:"history", channel:"#x", before:"2026-05-21T12:00:00.000Z", limit:200}
//	outbound: {op:"history_result", channel, messages:[{nick,text,ts,id}], has_more:bool}
//
// `before` and `limit` are both optional: empty `before` returns the
// most recent `limit` messages (equivalent to the initial attach
// replay's depth, but on demand). `limit` defaults to 200, capped to
// 200 — the SAME cap the bouncer's CHATHISTORY enforces, so the two
// surfaces agree on a single page size.
func (g *Gateway) handleHistoryOp(ctx context.Context, c *client, p map[string]any) {
	channel, _ := p["channel"].(string)
	if channel == "" || !startsWithChannelSigil(channel) {
		return
	}
	limit := 200
	// JSON numbers unmarshal as float64 — no int branch needed.
	if v, ok := p["limit"].(float64); ok && int(v) > 0 {
		limit = int(v)
	}
	if limit > 200 {
		limit = 200
	}
	var before time.Time
	if raw, ok := p["before"].(string); ok && raw != "" {
		if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			before = t
		} else if t, err := time.Parse("2006-01-02T15:04:05.000Z", raw); err == nil {
			before = t
		}
	}

	store := g.opts.MessageStore
	if store == nil {
		g.sendTo(ctx, c, map[string]any{
			"op": "history_result", "channel": channel,
			"messages": []map[string]any{}, "has_more": false,
		})
		return
	}
	msgs, err := store.Recent(ctx, channel, before, limit)
	if err != nil {
		g.log.Debug("history op", "err", err, "channel", channel)
		g.sendTo(ctx, c, map[string]any{
			"op": "history_result", "channel": channel,
			"messages": []map[string]any{}, "has_more": false,
		})
		return
	}
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, map[string]any{
			"nick": m.Nick,
			"text": m.Text,
			"ts":   m.TS.Unix(),
			"id":   m.ID,
		})
	}
	// has_more is "the result hit the limit". Strictly speaking that
	// can over-report (the next page might be empty), but the UI's
	// next scroll-up will fire one more call and see the empty page,
	// so worst case is a single redundant request.
	g.sendTo(ctx, c, map[string]any{
		"op":       "history_result",
		"channel":  channel,
		"messages": out,
		"has_more": len(msgs) == limit,
	})
}

// handleTBOp handles {op:"tb", sub:"summarize", channel:"#x", n:200}.
func (g *Gateway) handleTBOp(ctx context.Context, c *client, p map[string]any) {
	sub, _ := p["sub"].(string)
	channel, _ := p["channel"].(string)

	switch strings.ToLower(sub) {
	case "summarize":
		if channel == "" || !startsWithChannelSigil(channel) {
			g.sendTo(ctx, c, map[string]any{
				"op": "tb_error", "message": "Channel is required.",
			})
			return
		}
		n := 0
		if v, ok := p["n"].(float64); ok && int(v) > 0 {
			n = int(v)
		}
		g.handleTBSummarize(ctx, c, channel, n)
	default:
		g.sendTo(ctx, c, map[string]any{
			"op": "tb_error", "message": "Unknown /tb subcommand. Available: summarize",
		})
	}
}

func (g *Gateway) handleTBSummarize(ctx context.Context, c *client, channel string, n int) {
	provider := g.opts.LLMProvider
	store := g.opts.MessageStore
	cap := g.opts.TBSummarizeMaxMessages

	if provider == nil {
		g.sendTo(ctx, c, map[string]any{"op": "tb_error", "message": "No LLM provider configured."})
		return
	}
	if store == nil {
		g.sendTo(ctx, c, map[string]any{"op": "tb_error", "message": "No message history available."})
		return
	}
	if cap <= 0 {
		g.sendTo(ctx, c, map[string]any{"op": "tb_error", "message": "/tb summarize is not available on your plan."})
		return
	}
	if n <= 0 {
		n = 200
	}
	if n > cap {
		n = cap
	}

	g.sendTo(ctx, c, map[string]any{"op": "tb_status", "message": "Summarizing " + channel + "..."})

	go func(c *client) { //nolint:gosec // intentionally detached — LLM call outlives the WS frame
		bgCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		msgs, err := store.Recent(bgCtx, channel, time.Time{}, n)
		if err != nil {
			g.sendTo(bgCtx, c, map[string]any{"op": "tb_error", "message": "Failed to fetch history."})
			return
		}
		if len(msgs) == 0 {
			g.sendTo(bgCtx, c, map[string]any{"op": "tb_error", "message": "No messages in " + channel + " to summarize."})
			return
		}
		for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
			msgs[i], msgs[j] = msgs[j], msgs[i]
		}
		var sb strings.Builder
		for _, m := range msgs {
			fmt.Fprintf(&sb, "<%s> %s\n", m.Nick, m.Text)
		}
		prompt := fmt.Sprintf("Summarize these %d messages from %s:\n\n%s", len(msgs), channel, sb.String())
		summary, err := provider.Ask(bgCtx, prompt,
			llm.WithSystem("Summarize the following IRC conversation concisely. Output a short, readable summary suitable for a single IRC message (max ~400 chars). No markdown."),
			llm.WithMaxTokens(300))
		if err != nil {
			g.sendTo(bgCtx, c, map[string]any{"op": "tb_error", "message": "LLM request failed."})
			return
		}
		trimmed := strings.TrimSpace(summary)
		if store != nil {
			_ = store.Submit(bgCtx, messages.Message{
				Channel: channel,
				Nick:    "*turborg",
				Text:    "\U0001f4cb Summary: " + trimmed,
				TS:      time.Now(),
			})
		}
		g.sendTo(bgCtx, c, map[string]any{
			"op":      "tb_result",
			"sub":     "summarize",
			"channel": channel,
			"summary": trimmed,
		})
	}(c)
}

// --- EventBus handlers ----------------------------------------------------

func (g *Gateway) onMessage(_ context.Context, ev *agent.Event) {
	env, _ := ev.Fields["envelope"].(*agent.InboundEnvelope)
	channel, _ := ev.Fields["channel"].(string)
	sender, _ := ev.Fields["sender"].(string)
	text, _ := ev.Fields["text"].(string)
	if env != nil {
		channel, sender, text = env.Channel, env.Sender, env.Text
	}
	// Durable persistence lives on runtime.makeStoreSubmitter (one
	// canonical subscriber per process). The gateway only handles the
	// WS broadcast half — submitting from here too caused every message
	// to land in the DB twice with different msg_ids.
	g.broadcast(map[string]any{
		"op":      "message",
		"channel": channel,
		"nick":    sender,
		"text":    text,
		"ts":      time.Now().Unix(),
	})
}

func (g *Gateway) onMessageSent(_ context.Context, ev *agent.Event) {
	channel, _ := ev.Fields["channel"].(string)
	sender, _ := ev.Fields["sender"].(string)
	text, _ := ev.Fields["text"].(string)
	// Agent.handle publishes MESSAGE_SENT with envelope-only (the
	// command-dispatch path). The bouncer's ForwardedObserver and the
	// gateway's own "say"-op publish with explicit channel/sender/text.
	// Handle both shapes so command replies (e.g. !ping → pong) and
	// bouncer-tunneled messages render uniformly in the WS frame.
	if env, ok := ev.Fields["envelope"].(*agent.OutboundEnvelope); ok && env != nil {
		if channel == "" {
			channel = env.Channel
		}
		if text == "" {
			text = env.Text
		}
	}
	// OutboundEnvelope carries no sender (the bot is implicit). Fall
	// back to the live current nick so command replies attribute
	// correctly in the UI.
	if sender == "" {
		sender = g.bridge.CurrentNick()
	}
	// Persistence is handled by runtime.makeStoreSubmitter (single
	// canonical subscriber); the gateway only broadcasts to WS clients.
	g.broadcast(map[string]any{
		"op":      "message",
		"channel": channel,
		"nick":    sender,
		"text":    text,
		"ts":      time.Now().Unix(),
	})
}

// (submitToStore removed — message persistence is the runtime
// EventBus subscriber's job. Keeping a duplicate writer here caused
// every channel message to land in the DB twice with different
// msg_ids since both the runtime submitter and the gateway's own
// onMessage / onMessageSent subscribers ran on the same events.)

func startsWithChannelSigil(s string) bool {
	if s == "" {
		return false
	}
	switch s[0] {
	case '#', '&', '+', '!':
		return true
	}
	return false
}

func (g *Gateway) onUserJoin(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":      "join",
		"channel": ev.Fields["channel"],
		"nick":    ev.Fields["nick"],
	})
}

func (g *Gateway) onUserLeave(_ context.Context, ev *agent.Event) {
	reason, _ := ev.Fields["reason"].(string)
	g.broadcast(map[string]any{
		"op":      "part",
		"channel": ev.Fields["channel"],
		"nick":    ev.Fields["nick"],
		"reason":  reason,
	})
}

func (g *Gateway) onUserKicked(_ context.Context, ev *agent.Event) {
	reason, _ := ev.Fields["reason"].(string)
	g.broadcast(map[string]any{
		"op":      "kick",
		"channel": ev.Fields["channel"],
		"nick":    ev.Fields["nick"],
		"kicker":  ev.Fields["by"],
		"reason":  reason,
	})
}

func (g *Gateway) onUserNick(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":  "nick",
		"old": ev.Fields["old"],
		"new": ev.Fields["new"],
	})
}

func (g *Gateway) onTopic(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":      "topic",
		"channel": ev.Fields["channel"],
		"topic":   ev.Fields["topic"],
		"setBy":   ev.Fields["by"],
	})
}

func (g *Gateway) onNames(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":      "names",
		"channel": ev.Fields["channel"],
		"members": ev.Fields["members"],
	})
}

func (g *Gateway) onServerNotice(_ context.Context, ev *agent.Event) {
	text, _ := ev.Fields["text"].(string)
	kind, _ := ev.Fields["kind"].(string)
	payload := map[string]any{
		"op":   "server",
		"text": text,
		"kind": kind,
		"ts":   time.Now().Unix(),
	}
	g.logMu.Lock()
	g.serverLog = append(g.serverLog, payload)
	if len(g.serverLog) > serverLogCap {
		g.serverLog = g.serverLog[len(g.serverLog)-serverLogCap:]
	}
	g.logMu.Unlock()
	g.broadcast(payload)
}

func (g *Gateway) onModeChanged(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":      "mode_changed",
		"channel": ev.Fields["channel"],
		"modes":   ev.Fields["modes"],
		"args":    ev.Fields["args"],
		"setBy":   ev.Fields["by"],
	})
}

func (g *Gateway) onWhoisResult(_ context.Context, ev *agent.Event) {
	out := map[string]any{"op": "whois_result"}
	for k, v := range ev.Fields {
		out[k] = v
	}
	g.broadcast(out)
}

func (g *Gateway) onListResult(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":       "list_result",
		"channels": ev.Fields["channels"],
	})
}

func (g *Gateway) onWhoResult(_ context.Context, ev *agent.Event) {
	g.broadcast(map[string]any{
		"op":     "who_result",
		"target": ev.Fields["target"],
		"users":  ev.Fields["users"],
	})
}

func (g *Gateway) onJoinFailed(_ context.Context, ev *agent.Event) {
	reason, _ := ev.Fields["reason"].(string)
	g.broadcast(map[string]any{
		"op":      "channel.rejoin_failed",
		"channel": ev.Fields["channel"],
		"code":    ev.Fields["code"],
		"reason":  reason,
	})
}

// onUpstreamStateChange fans connector.state_changed events to every
// attached WS client when the IRC connector's upstream state machine
// transitions. Severity colour-coding + human-readable body come from
// the irc package so the bouncer NOTICE wording and the gateway event
// message stay in lockstep.
func (g *Gateway) onUpstreamStateChange(change irc.UpstreamStateChange) {
	g.broadcast(map[string]any{
		"op":            "connector.state_changed",
		"state":         string(change.To),
		"prior_state":   string(change.From),
		"message":       irc.DescribeUpstreamState(change.To, "", change.ServerReason),
		"severity":      irc.SeverityForUpstreamState(change.To),
		"server_reason": change.ServerReason,
	})
}

// rejectSend emits a send_message.rejected frame to the originating
// client when a WS-originated say op can't reach upstream. Carries
// the original target + body so the UI can mark the optimistically-
// rendered message as failed and surface the reason inline.
func (g *Gateway) rejectSend(ctx context.Context, c *client, target, body, reasonState, message string) {
	g.sendTo(ctx, c, map[string]any{
		"op":      "send_message.rejected",
		"target":  target,
		"body":    body,
		"reason":  reasonState,
		"message": message,
	})
}

// --- broadcast + send -----------------------------------------------------

func (g *Gateway) broadcast(payload map[string]any) {
	g.mu.Lock()
	clients := make([]*client, 0, len(g.clients))
	for c := range g.clients {
		clients = append(clients, c)
	}
	g.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, c := range clients {
		g.sendTo(ctx, c, payload)
	}
}

func (g *Gateway) sendTo(ctx context.Context, c *client, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	g.metMu.Lock()
	g.metrics.messagesForwarded++
	g.metMu.Unlock()
	if err := c.conn.Write(ctx, websocket.MessageText, body); err != nil {
		g.mu.Lock()
		delete(g.clients, c)
		g.mu.Unlock()
	}
}

func (g *Gateway) clientCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.clients)
}

// --- idle shutdown --------------------------------------------------------

func (g *Gateway) scheduleIdleShutdown() {
	if g.opts.IdleShutdownSeconds <= 0 || g.opts.OnIdleShutdown == nil {
		return
	}
	stop := make(chan struct{})
	g.idleMu.Lock()
	if g.idleStop != nil {
		close(g.idleStop)
	}
	g.idleStop = stop
	g.idleMu.Unlock()

	go func() {
		select {
		case <-stop:
			return
		case <-time.After(time.Duration(g.opts.IdleShutdownSeconds) * time.Second):
		}
		if g.clientCount() > 0 {
			return
		}
		g.log.Info("web idle shutdown",
			"reason", "no_clients", "seconds", g.opts.IdleShutdownSeconds)
		g.opts.OnIdleShutdown()
	}()
}

func (g *Gateway) cancelIdleTimer() {
	g.idleMu.Lock()
	defer g.idleMu.Unlock()
	if g.idleStop != nil {
		close(g.idleStop)
		g.idleStop = nil
	}
}

func (g *Gateway) closeAllClients() error {
	g.mu.Lock()
	clients := make([]*client, 0, len(g.clients))
	for c := range g.clients {
		clients = append(clients, c)
	}
	g.clients = map[*client]struct{}{}
	g.mu.Unlock()
	for _, c := range clients {
		_ = c.conn.Close(websocket.StatusGoingAway, "shutting down")
	}
	return nil
}

// --- helpers --------------------------------------------------------------

func extractToken(r *http.Request) string {
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func mapCopy(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
