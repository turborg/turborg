// Package telegram bridges a Telegram bot into a turborg agent. The connector
// long-polls the Bot API (getUpdates) for inbound messages, normalizes each into
// an agent.InboundEnvelope, and sends handler output back as bot messages
// (threaded replies when the outbound envelope references an inbound one). It
// dials OUT only — no inbound port, no public URL.
package telegram

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
	"github.com/turborg/turborg/internal/agent"
)

// replyRefCap bounds the inbound-id → native-message-id map kept so a handler's
// threaded reply (OutboundEnvelope.ReplyTo) can resolve the Telegram message it
// answers. Oldest entries are evicted first.
const replyRefCap = 512

// newBotAPI builds the Bot API client. It is a package var (defaulting to the
// library constructor) so tests can point the client at a local stub endpoint
// instead of the real api.telegram.org, keeping Start/Run hermetic.
var newBotAPI = tgbotapi.NewBotAPI

// api is the narrow slice of *tgbotapi.BotAPI the connector calls for outbound +
// moderation. It is an interface so tests inject a fake and never hit the real
// Bot API. *tgbotapi.BotAPI satisfies it directly.
type api interface {
	Send(c tgbotapi.Chattable) (tgbotapi.Message, error)
	Request(c tgbotapi.Chattable) (*tgbotapi.APIResponse, error)
}

// replyRef is the native coordinates stored per inbound envelope so a threaded
// reply can address the exact Telegram message.
type replyRef struct {
	chatID    int64
	messageID int
}

// Connector is the Telegram Bot API adapter. Lifecycle: Start opens the Bot API
// client (unless booted suspended); Run drives the long-poll update loop and
// supervises park/resume; Stop halts polling. Inbound delivers normalized
// envelopes; Send translates an OutboundEnvelope to a bot message.
type Connector struct {
	settings *Settings
	log      *slog.Logger
	events   *agent.EventBus
	inbox    chan *agent.InboundEnvelope

	mu       sync.Mutex
	bot      *tgbotapi.BotAPI // concrete client for the update loop (production)
	api      api              // outbound/moderation surface (fake in tests)
	selfID   int64
	selfName string
	allow    map[string]struct{}
	closed   bool
	done     chan struct{}
	// stopReceiverOnce guards bot.StopReceivingUpdates: both Run's defer (on ctx
	// cancel) and Stop (on teardown) want to halt the long-poll, but the library
	// closes an internal channel unconditionally, so a second call would panic.
	stopReceiverOnce sync.Once

	refMu   sync.Mutex
	refs    map[uuid.UUID]replyRef
	refRing []uuid.UUID

	lifecycleMu sync.Mutex
	suspended   bool

	stateMu       sync.Mutex
	state         string
	stateSince    time.Time
	stateReason   string
	onStateChange func()
}

var _ agent.Connector = (*Connector)(nil)
var _ agent.StateReporter = (*Connector)(nil)
var _ agent.StateSubscriber = (*Connector)(nil)

// ConnectorState returns a connector-agnostic snapshot of the live connection
// state for the state-mirror emitter.
func (c *Connector) ConnectorState() agent.ConnectorState {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return agent.ConnectorState{State: c.state, Since: c.stateSince, Reason: c.stateReason}
}

// OnStateChange registers a callback fired on every state transition so the
// emitter can be event-driven rather than polled.
func (c *Connector) OnStateChange(fn func()) {
	c.stateMu.Lock()
	c.onStateChange = fn
	c.stateMu.Unlock()
}

// setState records a new connection state (and reason) and fires the change
// hook synchronously when either moved.
func (c *Connector) setState(state, reason string) {
	c.stateMu.Lock()
	changed := c.state != state || c.stateReason != reason
	if changed {
		c.state, c.stateReason, c.stateSince = state, reason, time.Now()
	}
	fn := c.onStateChange
	c.stateMu.Unlock()
	if changed && fn != nil {
		fn()
	}
}

// New builds a Telegram Connector. events is the tenant-owned bus (may be nil in
// tests that don't need persistence signals).
func New(s *Settings, log *slog.Logger, events *agent.EventBus) *Connector {
	if log == nil {
		log = slog.Default()
	}
	allow := make(map[string]struct{}, len(s.Chats))
	for _, ch := range s.Chats {
		if ch != "" {
			allow[ch] = struct{}{}
		}
	}
	initialState := "connecting"
	if s.Suspended {
		initialState = "suspended"
	}
	return &Connector{
		settings:   s,
		log:        log.With("connector", "telegram"),
		events:     events,
		inbox:      make(chan *agent.InboundEnvelope, 64),
		allow:      allow,
		done:       make(chan struct{}),
		refs:       map[uuid.UUID]replyRef{},
		suspended:  s.Suspended,
		state:      initialState,
		stateSince: time.Now(),
	}
}

func (c *Connector) Name() string                           { return "telegram" }
func (c *Connector) ClaimSupervision() bool                 { return true }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }

// BotName returns the bot's own display name once known, or a neutral default.
func (c *Connector) BotName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.selfName != "" {
		return c.selfName
	}
	return "turborg"
}

// SetInitialSuspended records a boot-time disconnect intent before Start.
func (c *Connector) SetInitialSuspended(v bool) {
	c.lifecycleMu.Lock()
	c.suspended = v
	c.lifecycleMu.Unlock()
}

// Suspend records the user-requested disconnect; the poll loop then drops
// inbound updates until Resume. Idempotent.
func (c *Connector) Suspend() {
	c.lifecycleMu.Lock()
	c.suspended = true
	c.lifecycleMu.Unlock()
	c.setState("suspended", "")
}

// Resume clears a user-requested disconnect. Idempotent.
func (c *Connector) Resume() {
	c.lifecycleMu.Lock()
	c.suspended = false
	c.lifecycleMu.Unlock()
	c.setState("connected", "")
}

func (c *Connector) isSuspended() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.suspended
}

// Start opens the Bot API client unless the connector is pre-wired (tests) or
// booted suspended.
func (c *Connector) Start(context.Context) error {
	if c.isSuspended() {
		c.log.Info("telegram connector starting suspended; awaiting resume")
		c.setState("suspended", "")
		return nil
	}
	c.mu.Lock()
	preWired := c.bot != nil || c.api != nil
	c.mu.Unlock()
	if preWired {
		c.setState("connected", "")
		return nil
	}
	bot, err := newBotAPI(c.settings.Token)
	if err != nil {
		c.setState("error", err.Error())
		return err
	}
	c.mu.Lock()
	c.bot = bot
	c.api = bot
	c.selfID = bot.Self.ID
	c.selfName = bot.Self.UserName
	c.mu.Unlock()
	c.setState("connected", "")
	return nil
}

// Run drives the long-poll update loop until ctx cancel. When no real Bot API
// client is present (tests / booted suspended), it simply blocks on ctx.
// Suspended updates are dropped so a parked connector stays quiet without
// tearing the loop down.
func (c *Connector) Run(ctx context.Context) error {
	c.mu.Lock()
	bot := c.bot
	c.mu.Unlock()
	if bot == nil {
		<-ctx.Done()
		return nil
	}
	upd := tgbotapi.NewUpdate(0)
	upd.Timeout = 30
	updates := bot.GetUpdatesChan(upd)
	defer c.stopReceiver(bot)
	for {
		select {
		case <-ctx.Done():
			return nil
		case u, ok := <-updates:
			if !ok {
				return nil
			}
			if c.isSuspended() {
				continue
			}
			c.onUpdate(u)
		}
	}
}

// Stop unblocks any in-flight inbound push. Idempotent.
func (c *Connector) Stop(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	bot := c.bot
	c.mu.Unlock()
	c.stopReceiver(bot)
	return nil
}

// stopReceiver halts the Bot API long-poll exactly once. Safe to call from both
// Run's defer and Stop (and safe on a nil bot, e.g. tests / booted suspended).
func (c *Connector) stopReceiver(bot *tgbotapi.BotAPI) {
	if bot == nil {
		return
	}
	c.stopReceiverOnce.Do(bot.StopReceivingUpdates)
}

// onUpdate lifts a Bot API update into the connector's normalization path.
func (c *Connector) onUpdate(u tgbotapi.Update) {
	m := u.Message
	if m == nil || m.From == nil || m.Chat == nil {
		return
	}
	c.ingest(m.Chat.ID, m.From.ID, senderName(m.From), m.Text, m.MessageID, m.Chat.Type == "private")
}

// ingest normalizes a single message into an InboundEnvelope: it skips the
// bot's own messages, applies the chat allow-list (DMs bypass it), stashes the
// native ids in Metadata, records the reply reference, and pushes onto the
// inbox. Factored out of the update loop so tests drive it directly.
func (c *Connector) ingest(chatID, senderID int64, senderName, text string, msgID int, isDirect bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	c.mu.Lock()
	self := c.selfID
	c.mu.Unlock()
	if senderID != 0 && senderID == self {
		return
	}
	chat := strconv.FormatInt(chatID, 10)
	if !isDirect && !c.chatAllowed(chat) {
		return
	}

	env := agent.NewInbound("telegram", chat, senderName, text)
	env.IsDirect = isDirect
	env.Metadata["message_id"] = strconv.Itoa(msgID)
	env.Metadata["chat_id"] = chat
	env.Metadata["sender_id"] = strconv.FormatInt(senderID, 10)
	env.Metadata["sender_name"] = senderName
	c.rememberRef(env.ID, replyRef{chatID: chatID, messageID: msgID})

	select {
	case c.inbox <- env:
	case <-c.done:
	}
}

// chatAllowed reports whether a chat id passes the allow-list. Empty = all.
func (c *Connector) chatAllowed(chat string) bool {
	if len(c.allow) == 0 {
		return true
	}
	_, ok := c.allow[chat]
	return ok
}

// Send delivers a handler-produced outbound message. When the envelope
// references an inbound one (ReplyTo) whose native id is still known, it is sent
// as a threaded reply; otherwise a plain message.
func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	a := c.getAPI()
	if a == nil {
		return errors.New("telegram: not connected")
	}
	chatID, err := strconv.ParseInt(env.Channel, 10, 64)
	if err != nil {
		return errors.New("telegram: invalid chat id " + env.Channel)
	}
	msg := tgbotapi.NewMessage(chatID, env.Text)
	if env.ReplyTo != nil {
		if ref, ok := c.lookupRef(*env.ReplyTo); ok && ref.messageID != 0 {
			msg.ReplyToMessageID = ref.messageID
		}
	}
	_, err = a.Send(msg)
	return err
}

// getAPI returns the live outbound surface (nil before connect).
func (c *Connector) getAPI() api {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.api
}

func (c *Connector) rememberRef(id uuid.UUID, ref replyRef) {
	c.refMu.Lock()
	defer c.refMu.Unlock()
	if _, ok := c.refs[id]; !ok {
		c.refRing = append(c.refRing, id)
		if len(c.refRing) > replyRefCap {
			oldest := c.refRing[0]
			c.refRing = c.refRing[1:]
			delete(c.refs, oldest)
		}
	}
	c.refs[id] = ref
}

func (c *Connector) lookupRef(id uuid.UUID) (replyRef, bool) {
	c.refMu.Lock()
	defer c.refMu.Unlock()
	ref, ok := c.refs[id]
	return ref, ok
}

// publishSent mirrors bot output onto the event bus so the shared store
// submitter persists it.
func (c *Connector) publishSent(channel, text string) {
	if c.events == nil {
		return
	}
	c.events.Publish(context.Background(), &agent.Event{
		Type: agent.EventMessageSent,
		Time: time.Now(),
		Fields: map[string]any{
			"connector": "telegram",
			"channel":   channel,
			"sender":    c.BotName(),
			"text":      text,
		},
	})
}

// senderName picks the most human-friendly identifier for a Telegram user: the
// username, falling back to the first name, then the numeric id.
func senderName(u *tgbotapi.User) string {
	switch {
	case u == nil:
		return ""
	case u.UserName != "":
		return u.UserName
	case u.FirstName != "":
		return u.FirstName
	default:
		return strconv.FormatInt(u.ID, 10)
	}
}
