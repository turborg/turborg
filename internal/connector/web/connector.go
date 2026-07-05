package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
)

// defaultConsoleSystemPrompt shapes the assistant's voice for free-text chat in
// a web console. Operators can override it via SetConsolePrompt.
const defaultConsoleSystemPrompt = "You are a helpful, friendly AI assistant chatting with a user in a web console. " +
	"Answer directly and concisely. If you don't know something, say so."

// consoleHistoryDepth is how many prior room messages are fed to the model as
// conversation context, so the console feels like a real multi-turn assistant.
const consoleHistoryDepth = 20

// consoleMaxTokens bounds a single assistant reply.
const consoleMaxTokens = 600

// roomLogCap bounds how many recent messages a newly-attached client is
// replayed on connect, and the ceiling on an on-demand history page. Matches
// the IRC gateway's channelLogCap so both surfaces agree on one page size.
const roomLogCap = 200

// Settings configures a web-chat Connector.
type Settings struct {
	// BotNick is the display name attributed to bot output in the room.
	BotNick string
	// Room is the single chat room this connector serves (e.g. "console").
	// Envelopes use it as the channel; the token's room claim must match it.
	Room string
	// Public reports whether the room is a shared/public room. False = a
	// private owner console, where inbound messages are marked IsDirect so
	// owner-only handlers treat them as a DM from the owner.
	Public bool
}

// Connector is the hosted web-chat adapter. It serves browser WebSocket
// clients directly (no upstream network), normalizing each `say` frame into an
// agent.InboundEnvelope and fanning bot output back out as `message` frames.
//
// Lifecycle: Start is a no-op (nothing to dial); Run blocks until ctx is
// cancelled; Stop closes every client socket. ServeWS is called per browser
// connection by the pooled web router after it resolves the tenant.
type Connector struct {
	settings Settings
	verifier *SignedTokenVerifier
	log      *slog.Logger

	// events is the tenant-owned bus. Actor.Say publishes MESSAGE_SENT here so
	// the shared store submitter persists bot output. nil is tolerated (the
	// connector still serves live traffic, just without persistence signals).
	events *agent.EventBus

	// store backs attach-replay + the history op. nil = no scrollback (live
	// traffic only).
	store messages.Store

	// activityHook fires on genuine owner presence (an inbound chat message) so
	// a web-only tenant isn't idle-paused while its owner is talking to it. nil
	// = not wired.
	activityHook func(reason string)

	// llm answers free-text console messages (anything that isn't a command)
	// as an AI assistant. nil = no AI chat — non-command messages get no reply.
	// When it's a *llm.BudgetedProvider, a spent daily budget surfaces to the
	// client as a distinct budget-exhausted signal instead of a generic error.
	llm llm.Provider
	// consolePrompt is the system prompt for AI chat. Empty uses the default.
	consolePrompt string
	// commandPrefix marks a message as a command (routed to the agent) rather
	// than AI chat. Empty defaults to "!".
	commandPrefix string
	// historyDepth is how many prior messages of conversation memory the AI
	// chat feeds the model. 0 uses the package default. Sourced from the plan's
	// conversation-memory capability so a higher tier remembers more context.
	historyDepth int

	inbox chan *agent.InboundEnvelope

	mu      sync.Mutex
	clients map[*wsClient]struct{}
	closed  bool
	done    chan struct{}
}

// wsClient is one attached browser connection.
type wsClient struct {
	conn *websocket.Conn
	role string
}

var _ agent.Connector = (*Connector)(nil)

// New builds a web-chat Connector. events is the tenant-owned bus (may be nil
// in tests that don't need persistence signals); verifier authenticates every
// attaching client's token.
func New(s Settings, log *slog.Logger, events *agent.EventBus, verifier *SignedTokenVerifier) *Connector {
	if log == nil {
		log = slog.Default()
	}
	if s.Room == "" {
		s.Room = "console"
	}
	return &Connector{
		settings: s,
		verifier: verifier,
		log:      log.With("connector", "web"),
		events:   events,
		inbox:    make(chan *agent.InboundEnvelope, 16),
		clients:  map[*wsClient]struct{}{},
		done:     make(chan struct{}),
	}
}

// SetMessageStore installs the scrollback store used for attach-replay and the
// history op. Call before Start.
func (c *Connector) SetMessageStore(s messages.Store) { c.store = s }

// SetActivityHook installs the owner-presence hook fired when the owner sends a
// chat message. Call before Start.
func (c *Connector) SetActivityHook(h func(reason string)) { c.activityHook = h }

// SetLLMProvider installs the provider that answers free-text console messages
// as an AI assistant. Call before Start. Nil disables AI chat.
func (c *Connector) SetLLMProvider(p llm.Provider) { c.llm = p }

// SetConsolePrompt overrides the assistant system prompt. Empty keeps the
// default. Call before Start.
func (c *Connector) SetConsolePrompt(s string) { c.consolePrompt = s }

// SetCommandPrefix sets the prefix that routes a message to the agent's command
// dispatch instead of AI chat. Empty defaults to "!". Call before Start.
func (c *Connector) SetCommandPrefix(p string) { c.commandPrefix = p }

// SetHistoryDepth sets how many prior messages of conversation memory the AI
// chat feeds the model (the plan's conversation-memory capability). <= 0 keeps
// the package default. Call before Start.
func (c *Connector) SetHistoryDepth(n int) { c.historyDepth = n }

// isCommand reports whether text is an agent command (routed to dispatch) rather
// than free-text AI chat.
func (c *Connector) isCommand(text string) bool {
	prefix := c.commandPrefix
	if prefix == "" {
		prefix = "!"
	}
	return strings.HasPrefix(text, prefix)
}

func (c *Connector) Name() string                           { return "web" }
func (c *Connector) ClaimSupervision() bool                 { return true }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }

// Start has no upstream handshake — a hosted room has nothing to dial.
func (c *Connector) Start(context.Context) error { return nil }

// Run blocks until ctx is cancelled. Client I/O is driven by ServeWS
// goroutines the web router owns, not here.
func (c *Connector) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Stop closes every attached socket and unblocks any in-flight inbound push.
// Idempotent and safe after Run returns. The inbox is intentionally left open —
// the agent's drain loop exits on ctx cancellation, and ServeWS goroutines
// select on c.done to abandon a pending push, so nothing ever sends on a closed
// channel.
func (c *Connector) Stop(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	clients := make([]*wsClient, 0, len(c.clients))
	for cl := range c.clients {
		clients = append(clients, cl)
	}
	c.clients = map[*wsClient]struct{}{}
	c.mu.Unlock()
	for _, cl := range clients {
		_ = cl.conn.Close(websocket.StatusGoingAway, "shutting down")
	}
	return nil
}

// Send delivers a handler-produced outbound message (a command reply routed
// through agent.handle) to every client in the room as a bot frame. Skill/flow
// output arrives via the Actor instead; both render identically.
func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	c.broadcast(map[string]any{
		"op":     "message",
		"kind":   "bot",
		"sender": c.settings.BotNick,
		"text":   env.Text,
		"ts":     time.Now().Unix(),
		"id":     uuid.NewString(),
	})
	return nil
}

// ServeWS authenticates one browser WebSocket request against the tenant it
// addresses, upgrades it, replays recent history, and runs the read loop until
// the socket closes. tenantID is the tenant resolved from the request path; the
// token is rejected unless its `tid` claim matches it (cross-tenant replay
// defence). Auth failures answer 401 with no detail.
func (c *Connector) ServeWS(w http.ResponseWriter, r *http.Request, tenantID string) {
	token := extractToken(r)
	claims, err := c.verifier.VerifyClaims(token, tenantID, c.settings.Room)
	if err != nil {
		c.log.Info("web chat auth fail", "ip", clientIP(r), "err", err)
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // origin checks are the deployment's job
	})
	if err != nil {
		c.log.Debug("web chat ws accept", "err", err)
		return
	}

	cl := &wsClient{conn: conn, role: claims.Role}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close(websocket.StatusGoingAway, "shutting down")
		return
	}
	c.clients[cl] = struct{}{}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.clients, cl)
		c.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()

	ctx := r.Context()
	c.sendState(ctx, cl, claims)
	c.replay(ctx, cl)
	c.readLoop(ctx, cl, claims)
}

// sendState sends the initial room descriptor so a fresh client knows the room
// name, whether it's public, and its own identity.
func (c *Connector) sendState(ctx context.Context, cl *wsClient, claims *Claims) {
	c.sendTo(ctx, cl, map[string]any{
		"op":     "state",
		"room":   c.settings.Room,
		"public": c.settings.Public,
		"you":    claims.Role,
	})
}

// replay streams the most recent room history to a newly-attached client,
// oldest-first, each frame flagged replayed so the UI can distinguish backfill
// from live traffic.
func (c *Connector) replay(ctx context.Context, cl *wsClient) {
	if c.store == nil {
		return
	}
	msgs, err := c.store.Recent(ctx, c.settings.Room, time.Time{}, roomLogCap)
	if err != nil {
		c.log.Debug("web chat replay", "err", err)
		return
	}
	// Recent returns newest-first; walk in reverse so the UI sees chronological
	// order.
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		c.sendTo(ctx, cl, c.messageFrame(m, true))
	}
}

// readLoop reads frames from one client until the socket errors (close /
// cancellation), dispatching each recognized op.
func (c *Connector) readLoop(ctx context.Context, cl *wsClient, claims *Claims) {
	for {
		_, raw, err := cl.conn.Read(ctx)
		if err != nil {
			return
		}
		var p map[string]any
		if err := json.Unmarshal(raw, &p); err != nil {
			continue
		}
		switch op, _ := p["op"].(string); op {
		case "say":
			c.handleSay(ctx, cl, claims, p)
		case "history":
			c.handleHistory(ctx, cl, p)
		}
	}
}

// handleSay turns a `{op:"say",text}` frame into an inbound envelope: it echoes
// the message to every client as a user frame, marks owner presence, and pushes
// the envelope onto the inbox for the agent to dispatch. The room name is the
// envelope channel; IsDirect is set for a private (non-public) room so
// owner-only handlers treat it as a DM.
func (c *Connector) handleSay(ctx context.Context, cl *wsClient, claims *Claims, p map[string]any) {
	text, _ := p["text"].(string)
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}

	// The owner typed and sent a message — genuine presence.
	if c.activityHook != nil {
		c.activityHook("web_message")
	}

	env := agent.NewInbound("web", c.settings.Room, claims.Role, text)
	env.IsDirect = !c.settings.Public
	env.Metadata["role"] = claims.Role
	env.Metadata["vid"] = claims.VisitorID

	// Echo the sender's own message back to all clients so it renders in every
	// open tab (there is no server round-trip for user messages otherwise).
	c.broadcast(map[string]any{
		"op":     "message",
		"kind":   "user",
		"sender": claims.Role,
		"text":   text,
		"ts":     time.Now().Unix(),
		"id":     env.ID.String(),
	})

	select {
	case c.inbox <- env:
	case <-c.done:
		return
	case <-ctx.Done():
		return
	}

	// Free-text (non-command) messages are answered by the AI assistant. The
	// message still went to the inbox above, so it's persisted (EventMessage)
	// and any command/skill still sees it; the LLM reply is the console's
	// conversational layer on top. Detached so the model call outlives this
	// frame.
	if c.llm != nil && !c.isCommand(text) {
		go c.answerWithLLM(text) //nolint:gosec // G118: intentionally detached — the model call outlives the WS frame
	}
}

// answerWithLLM streams an assistant reply to a free-text console message,
// token by token, so the client renders it live (like a real assistant) rather
// than waiting for the whole reply. The wire shape mirrors a streaming chat:
//
//	{op:"message_start", id, kind:"bot", sender}
//	{op:"message_delta", id, text}   (repeated)
//	{op:"message_end",   id}
//
// The full reply is persisted once at the end (MESSAGE_SENT) so it becomes
// context for the next turn — the deltas already showed it, so it is NOT also
// re-broadcast as a whole frame. A spent daily budget ends the (empty) message
// and emits a budget_exhausted signal instead.
func (c *Connector) answerWithLLM(userText string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	system := c.consolePrompt
	if system == "" {
		system = defaultConsoleSystemPrompt
	}

	id := uuid.NewString()
	c.broadcast(map[string]any{
		"op": "message_start", "id": id, "kind": "bot",
		"sender": c.settings.BotNick, "ts": time.Now().Unix(),
	})

	var full strings.Builder
	var streamErr error
	for text, err := range c.llm.Stream(ctx, c.buildChatPrompt(ctx, userText),
		llm.WithSystem(system), llm.WithMaxTokens(consoleMaxTokens)) {
		if err != nil {
			streamErr = err
			break
		}
		if text == "" {
			continue
		}
		full.WriteString(text)
		c.broadcast(map[string]any{"op": "message_delta", "id": id, "text": text})
	}

	if streamErr != nil {
		c.broadcast(map[string]any{"op": "message_end", "id": id, "aborted": true})
		if errors.Is(streamErr, llm.ErrBudgetExhausted) {
			c.broadcast(map[string]any{
				"op":      "budget_exhausted",
				"message": "The daily AI budget has been reached.",
			})
			c.broadcast(map[string]any{
				"op": "message", "kind": "system", "sender": "system",
				"text": "The daily AI budget has been reached.",
				"ts":   time.Now().Unix(), "id": uuid.NewString(),
			})
			return
		}
		c.log.Debug("console llm", "err", streamErr)
		c.broadcast(map[string]any{"op": "error", "message": "The assistant is unavailable right now."})
		return
	}

	c.broadcast(map[string]any{"op": "message_end", "id": id})

	// Persist the assembled reply for history/context (deltas already rendered
	// it, so don't re-broadcast a full bot frame).
	if reply := strings.TrimSpace(full.String()); reply != "" && c.events != nil {
		c.events.Publish(context.Background(), &agent.Event{
			Type: agent.EventMessageSent,
			Time: time.Now(),
			Fields: map[string]any{
				"connector": "web", "channel": c.settings.Room,
				"sender": c.settings.BotNick, "text": reply,
			},
		})
	}
}

// buildChatPrompt assembles the model prompt: recent room history as a
// transcript (conversation memory) followed by the new user message.
func (c *Connector) buildChatPrompt(ctx context.Context, userText string) string {
	var sb strings.Builder
	if c.store != nil {
		depth := c.historyDepth
		if depth <= 0 {
			depth = consoleHistoryDepth
		}
		if msgs, err := c.store.Recent(ctx, c.settings.Room, time.Time{}, depth); err == nil {
			// Recent is newest-first; walk in reverse for chronological order.
			for i := len(msgs) - 1; i >= 0; i-- {
				role := "User"
				if msgs[i].Nick == c.settings.BotNick {
					role = "Assistant"
				}
				fmt.Fprintf(&sb, "%s: %s\n", role, msgs[i].Text)
			}
		}
	}
	fmt.Fprintf(&sb, "User: %s\nAssistant:", userText)
	return sb.String()
}

// handleHistory answers a `{op:"history",before,limit}` scrollback request:
// `{op:"history_result",messages,has_more}`. `before` (RFC3339Nano) and `limit`
// are optional; limit defaults to and is capped at roomLogCap.
func (c *Connector) handleHistory(ctx context.Context, cl *wsClient, p map[string]any) {
	limit := roomLogCap
	if v, ok := p["limit"].(float64); ok && int(v) > 0 {
		limit = int(v)
	}
	if limit > roomLogCap {
		limit = roomLogCap
	}
	var before time.Time
	if raw, ok := p["before"].(string); ok && raw != "" {
		if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			before = t
		}
	}
	if c.store == nil {
		c.sendTo(ctx, cl, map[string]any{
			"op": "history_result", "messages": []map[string]any{}, "has_more": false,
		})
		return
	}
	msgs, err := c.store.Recent(ctx, c.settings.Room, before, limit)
	if err != nil {
		c.log.Debug("web chat history", "err", err)
		c.sendTo(ctx, cl, map[string]any{
			"op": "history_result", "messages": []map[string]any{}, "has_more": false,
		})
		return
	}
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, c.messageFrame(m, false))
	}
	c.sendTo(ctx, cl, map[string]any{
		"op":       "history_result",
		"messages": out,
		"has_more": len(msgs) == limit,
	})
}

// messageFrame renders a stored message as an outbound `message` frame,
// inferring kind from whether the bot authored it.
func (c *Connector) messageFrame(m messages.Message, replayed bool) map[string]any {
	kind := "user"
	if m.Nick == c.settings.BotNick {
		kind = "bot"
	}
	frame := map[string]any{
		"op":     "message",
		"kind":   kind,
		"sender": m.Nick,
		"text":   m.Text,
		"ts":     m.TS.Unix(),
		"id":     m.ID,
	}
	if replayed {
		frame["replayed"] = true
	}
	return frame
}

// broadcast fans a frame out to every attached client.
func (c *Connector) broadcast(payload map[string]any) {
	c.mu.Lock()
	clients := make([]*wsClient, 0, len(c.clients))
	for cl := range c.clients {
		clients = append(clients, cl)
	}
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, cl := range clients {
		c.sendTo(ctx, cl, payload)
	}
}

// sendTo writes one JSON frame to one client, dropping the client on a write
// error (a dead socket) so a wedged peer can't stall future broadcasts.
func (c *Connector) sendTo(ctx context.Context, cl *wsClient, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	if err := cl.conn.Write(ctx, websocket.MessageText, body); err != nil {
		c.mu.Lock()
		delete(c.clients, cl)
		c.mu.Unlock()
	}
}

// clientCount reports the number of attached clients (tests + diagnostics).
func (c *Connector) clientCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.clients)
}

// extractToken reads the auth token from the `?token=` query or an
// `Authorization: Bearer` header.
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
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
