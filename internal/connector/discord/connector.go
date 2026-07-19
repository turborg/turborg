// Package discord bridges one Discord guild into a turborg agent. The connector
// dials OUT to Discord's Gateway (a persistent WSS) — no inbound port, no public
// URL — normalizes each guild/DM message into an agent.InboundEnvelope, and
// fans handler output back out as channel messages (threaded replies when the
// outbound envelope references an inbound one).
package discord

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"github.com/turborg/turborg/internal/agent"
)

// replyRefCap bounds the inbound-id → native-message-id map the connector keeps
// so a handler's threaded reply (OutboundEnvelope.ReplyTo) can resolve the
// Discord message it answers. Oldest entries are evicted first.
const replyRefCap = 512

// session is the narrow slice of *discordgo.Session the connector calls. It is
// an interface so tests inject a fake and never open a real Gateway socket
// (tests run on the host, where a real connection is a genuine side effect).
// *discordgo.Session satisfies it directly.
type session interface {
	Open() error
	Close() error
	ChannelMessageSend(channelID, content string, options ...discordgo.RequestOption) (*discordgo.Message, error)
	ChannelMessageSendReply(channelID, content string, ref *discordgo.MessageReference, options ...discordgo.RequestOption) (*discordgo.Message, error)
	GuildMemberDeleteWithReason(guildID, userID, reason string, options ...discordgo.RequestOption) error
	GuildBanCreateWithReason(guildID, userID, reason string, days int, options ...discordgo.RequestOption) error
	ChannelEditComplex(channelID string, data *discordgo.ChannelEdit, options ...discordgo.RequestOption) (*discordgo.Channel, error)
}

// replyRef is the native message coordinates stored per inbound envelope so a
// threaded reply can address the exact Discord message.
type replyRef struct {
	channel   string
	messageID string
	guild     string
}

// Connector is the Discord Gateway adapter. Lifecycle: Start opens the session
// (unless booted suspended); Run supervises park/resume on the suspend intent
// and blocks until ctx cancel (discordgo owns its own reconnect); Stop closes
// the session. Inbound delivers normalized envelopes; Send translates an
// OutboundEnvelope to a Discord message.
type Connector struct {
	settings *Settings
	log      *slog.Logger
	events   *agent.EventBus
	inbox    chan *agent.InboundEnvelope

	mu       sync.Mutex
	session  session
	selfID   string
	selfName string
	allow    map[string]struct{}
	closed   bool
	done     chan struct{}

	refMu   sync.Mutex
	refs    map[uuid.UUID]replyRef
	refRing []uuid.UUID

	lifecycleMu sync.Mutex
	suspended   bool
	lifecycleCh chan struct{}

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

// New builds a Discord Connector. events is the tenant-owned bus (may be nil in
// tests that don't need persistence signals).
func New(s *Settings, log *slog.Logger, events *agent.EventBus) *Connector {
	if log == nil {
		log = slog.Default()
	}
	allow := make(map[string]struct{}, len(s.Channels))
	for _, ch := range s.Channels {
		if ch != "" {
			allow[ch] = struct{}{}
		}
	}
	initialState := "connecting"
	if s.Suspended {
		initialState = "suspended"
	}
	return &Connector{
		settings:    s,
		log:         log.With("connector", "discord"),
		events:      events,
		inbox:       make(chan *agent.InboundEnvelope, 64),
		allow:       allow,
		done:        make(chan struct{}),
		refs:        map[uuid.UUID]replyRef{},
		lifecycleCh: make(chan struct{}, 1),
		suspended:   s.Suspended,
		state:       initialState,
		stateSince:  time.Now(),
	}
}

func (c *Connector) Name() string                           { return "discord" }
func (c *Connector) ClaimSupervision() bool                 { return true }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }

// BotName returns the bot's own display name once the session is open, or a
// neutral default before then. Used as the outbound sender attribution.
func (c *Connector) BotName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.selfName != "" {
		return c.selfName
	}
	return "turborg"
}

// SetInitialSuspended records a boot-time disconnect intent before Start. When
// true, Start skips the initial connect and Run parks until Resume.
func (c *Connector) SetInitialSuspended(v bool) {
	c.lifecycleMu.Lock()
	c.suspended = v
	c.lifecycleMu.Unlock()
}

// Suspend drops the Gateway session on user request without tearing the
// connector down, parking it until Resume. Idempotent.
func (c *Connector) Suspend() {
	c.lifecycleMu.Lock()
	if c.suspended {
		c.lifecycleMu.Unlock()
		return
	}
	c.suspended = true
	c.lifecycleMu.Unlock()
	c.disconnect()
	c.signalLifecycle()
	c.setState("suspended", "")
}

// Resume clears a user-requested disconnect and wakes Run to reconnect.
// Idempotent.
func (c *Connector) Resume() {
	c.lifecycleMu.Lock()
	if !c.suspended {
		c.lifecycleMu.Unlock()
		return
	}
	c.suspended = false
	c.lifecycleMu.Unlock()
	c.setState("connecting", "")
	c.signalLifecycle()
}

func (c *Connector) isSuspended() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.suspended
}

func (c *Connector) signalLifecycle() {
	select {
	case c.lifecycleCh <- struct{}{}:
	default:
	}
}

// Start opens the Gateway session unless the connector booted suspended, in
// which case Run parks until Resume.
func (c *Connector) Start(context.Context) error {
	if c.isSuspended() {
		c.log.Info("discord connector starting suspended; awaiting resume")
		return nil
	}
	return c.connect()
}

// connect opens the session: a pre-injected one (tests) is simply Opened;
// otherwise a real discordgo session is built with the message intents, a
// MessageCreate handler, and its own reconnect supervision.
func (c *Connector) connect() error {
	c.mu.Lock()
	existing := c.session
	c.mu.Unlock()
	if existing != nil {
		if err := existing.Open(); err != nil {
			c.setState("error", err.Error())
			return err
		}
		c.setState("connected", "")
		return nil
	}

	dg, err := discordgo.New("Bot " + c.settings.Token)
	if err != nil {
		c.setState("error", err.Error())
		return err
	}
	dg.Identify.Intents = discordgo.IntentGuildMessages | discordgo.IntentDirectMessages | discordgo.IntentMessageContent
	dg.AddHandler(c.onMessageCreate)
	if err := dg.Open(); err != nil {
		c.setState("error", err.Error())
		return err
	}
	c.mu.Lock()
	c.session = dg
	if dg.State != nil && dg.State.User != nil {
		c.selfID = dg.State.User.ID
		c.selfName = dg.State.User.Username
	}
	c.mu.Unlock()
	c.setState("connected", "")
	return nil
}

// disconnect closes and drops the live session (park / shutdown). Safe on nil.
func (c *Connector) disconnect() {
	c.mu.Lock()
	s := c.session
	c.session = nil
	c.mu.Unlock()
	if s != nil {
		_ = s.Close()
	}
}

// Run supervises the connect/park lifecycle. discordgo manages its own read +
// reconnect loop once Open succeeds, so Run only reacts to Suspend/Resume and
// blocks until ctx cancel.
func (c *Connector) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.lifecycleCh:
			if c.isSuspended() {
				c.disconnect()
			} else if err := c.connect(); err != nil {
				c.setState("error", err.Error())
				c.log.Warn("discord resume connect failed", "err", err)
			}
		}
	}
}

// Stop closes the session and unblocks any in-flight inbound push. Idempotent.
func (c *Connector) Stop(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	s := c.session
	c.session = nil
	c.mu.Unlock()
	if s != nil {
		return s.Close()
	}
	return nil
}

// onMessageCreate is the discordgo handler that lifts a Gateway message into
// the connector's normalization path.
func (c *Connector) onMessageCreate(_ *discordgo.Session, m *discordgo.MessageCreate) {
	if m == nil || m.Message == nil || m.Author == nil {
		return
	}
	c.ingest(m.ChannelID, m.Author.ID, authorName(m.Author), m.Content, m.ID, m.GuildID, m.GuildID == "")
}

// ingest normalizes a single observed message into an InboundEnvelope: it skips
// the bot's own messages, applies the channel allow-list (DMs bypass it), stashes
// the native ids in Metadata, records the reply reference, and pushes onto the
// inbox. Factored out of the discordgo handler so tests drive it directly.
func (c *Connector) ingest(channelID, senderID, senderName, text, msgID, guildID string, isDirect bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	c.mu.Lock()
	self := c.selfID
	c.mu.Unlock()
	if senderID != "" && senderID == self {
		return
	}
	if !isDirect && !c.channelAllowed(channelID) {
		return
	}

	env := agent.NewInbound("discord", channelID, senderName, text)
	env.IsDirect = isDirect
	env.Metadata["message_id"] = msgID
	env.Metadata["guild_id"] = guildID
	env.Metadata["author_id"] = senderID
	env.Metadata["author_name"] = senderName
	c.rememberRef(env.ID, replyRef{channel: channelID, messageID: msgID, guild: guildID})

	select {
	case c.inbox <- env:
	case <-c.done:
	}
}

// channelAllowed reports whether a channel id passes the allow-list. An empty
// list allows every channel.
func (c *Connector) channelAllowed(channelID string) bool {
	if len(c.allow) == 0 {
		return true
	}
	_, ok := c.allow[channelID]
	return ok
}

// Send delivers a handler-produced outbound message. When the envelope
// references an inbound one (ReplyTo) and that inbound's native message id is
// still known, it is sent as a threaded reply; otherwise a plain message.
func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	s := c.getSession()
	if s == nil {
		return errors.New("discord: not connected")
	}
	if env.ReplyTo != nil {
		if ref, ok := c.lookupRef(*env.ReplyTo); ok && ref.messageID != "" {
			_, err := s.ChannelMessageSendReply(env.Channel, env.Text, &discordgo.MessageReference{
				MessageID: ref.messageID,
				ChannelID: ref.channel,
				GuildID:   ref.guild,
			})
			return err
		}
	}
	_, err := s.ChannelMessageSend(env.Channel, env.Text)
	return err
}

// getSession returns the live session (nil while parked / before connect).
func (c *Connector) getSession() session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

// rememberRef records the native message coordinates for an inbound envelope,
// evicting the oldest entry once the map is full.
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
// submitter persists it, the same reason the web/IRC connectors republish
// their own sends.
func (c *Connector) publishSent(channel, text string) {
	if c.events == nil {
		return
	}
	c.events.Publish(context.Background(), &agent.Event{
		Type: agent.EventMessageSent,
		Time: time.Now(),
		Fields: map[string]any{
			"connector": "discord",
			"channel":   channel,
			"sender":    c.BotName(),
			"text":      text,
		},
	})
}

// authorName picks the most human-friendly identifier available for a Discord
// author: the username, falling back to the id.
func authorName(u *discordgo.User) string {
	if u == nil {
		return ""
	}
	if u.Username != "" {
		return u.Username
	}
	return u.ID
}
