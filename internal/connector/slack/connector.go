// Package slack bridges a Slack workspace into a turborg agent over Socket Mode
// (an outbound WebSocket) — no inbound port, no public URL. The connector
// normalizes each channel/DM message into an agent.InboundEnvelope and sends
// handler output back as messages, threading replies when the outbound envelope
// references an inbound one.
package slack

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	slackapi "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
	"github.com/turborg/turborg/internal/agent"
)

// replyRefCap bounds the inbound-id → native-timestamp map kept so a handler's
// threaded reply (OutboundEnvelope.ReplyTo) can resolve the Slack message it
// answers. Oldest entries are evicted first.
const replyRefCap = 512

// api is the narrow slice of *slackapi.Client the connector calls for outbound +
// moderation. It is an interface so tests inject a fake and never hit the real
// Web API. *slackapi.Client satisfies it directly.
type api interface {
	PostMessage(channelID string, options ...slackapi.MsgOption) (string, string, error)
	KickUserFromConversation(channelID, user string) error
	SetTopicOfConversation(channelID, topic string) (*slackapi.Channel, error)
	InviteUsersToConversation(channelID string, users ...string) (*slackapi.Channel, error)
}

// socketRunner is the Socket Mode event pump. *socketmode.Client satisfies it;
// nil in tests so no WebSocket is opened.
type socketRunner interface {
	RunContext(ctx context.Context) error
	Ack(req socketmode.Request, payload ...any) error
}

// replyRef is the native coordinates stored per inbound envelope so a threaded
// reply can address the exact Slack message (a thread is keyed by its ts).
type replyRef struct {
	channel string
	ts      string
}

// Connector is the Slack Socket Mode adapter. Lifecycle: Start opens the Web API
// client + Socket Mode client (unless booted suspended); Run pumps the Socket
// Mode event loop and supervises park/resume; Stop signals shutdown. Inbound
// delivers normalized envelopes; Send translates an OutboundEnvelope to a
// message.
type Connector struct {
	settings *Settings
	log      *slog.Logger
	events   *agent.EventBus
	inbox    chan *agent.InboundEnvelope

	mu       sync.Mutex
	api      api
	sm       socketRunner
	events2  chan socketmode.Event // the socket client's event channel (production)
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

// New builds a Slack Connector. events is the tenant-owned bus (may be nil in
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
		settings:   s,
		log:        log.With("connector", "slack"),
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

func (c *Connector) Name() string                           { return "slack" }
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

// Suspend records the user-requested disconnect; the event loop then drops
// inbound messages until Resume. Idempotent.
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

// Start opens the Web API + Socket Mode clients unless the connector is
// pre-wired (tests) or booted suspended.
func (c *Connector) Start(context.Context) error {
	if c.isSuspended() {
		c.log.Info("slack connector starting suspended; awaiting resume")
		c.setState("suspended", "")
		return nil
	}
	c.mu.Lock()
	preWired := c.api != nil || c.sm != nil
	c.mu.Unlock()
	if preWired {
		c.setState("connected", "")
		return nil
	}
	client := slackapi.New(c.settings.BotToken, slackapi.OptionAppLevelToken(c.settings.AppToken))
	sm := socketmode.New(client)
	c.mu.Lock()
	c.api = client
	c.sm = sm
	c.events2 = sm.Events
	c.mu.Unlock()
	if auth, err := client.AuthTest(); err == nil && auth != nil {
		c.mu.Lock()
		c.selfID = auth.UserID
		c.selfName = auth.User
		c.mu.Unlock()
	}
	c.setState("connected", "")
	return nil
}

// Run pumps the Socket Mode event loop until ctx cancel. When no real Socket
// Mode client is present (tests / booted suspended), it simply blocks on ctx.
func (c *Connector) Run(ctx context.Context) error {
	c.mu.Lock()
	sm := c.sm
	evCh := c.events2
	c.mu.Unlock()
	if sm == nil {
		<-ctx.Done()
		return nil
	}
	if evCh != nil {
		go c.consume(ctx, sm, evCh)
	}
	return sm.RunContext(ctx)
}

// consume reads Socket Mode events and dispatches message events. It runs
// alongside the socket client's own RunContext pump.
func (c *Connector) consume(ctx context.Context, sm socketRunner, evCh <-chan socketmode.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-evCh:
			if !ok {
				return
			}
			c.handleSocketEvent(sm, evt)
		}
	}
}

// handleSocketEvent acks a Socket Mode request and lifts a message event into
// the connector's normalization path.
func (c *Connector) handleSocketEvent(sm socketRunner, evt socketmode.Event) {
	if evt.Type != socketmode.EventTypeEventsAPI {
		return
	}
	eventsAPI, ok := evt.Data.(slackevents.EventsAPIEvent)
	if !ok {
		return
	}
	if evt.Request != nil {
		_ = sm.Ack(*evt.Request)
	}
	if eventsAPI.Type != slackevents.CallbackEvent {
		return
	}
	if ev, ok := eventsAPI.InnerEvent.Data.(*slackevents.MessageEvent); ok {
		if c.isSuspended() {
			return
		}
		c.ingest(ev.Channel, ev.User, ev.BotID, ev.Text, ev.TimeStamp, ev.ChannelType == "im")
	}
}

// ingest normalizes a single message into an InboundEnvelope: it skips the
// bot's own messages (a set BotID, or the bot's own user id), applies the
// channel allow-list (DMs bypass it), stashes the native ids in Metadata,
// records the reply reference, and pushes onto the inbox. Factored out of the
// socket loop so tests drive it directly.
func (c *Connector) ingest(channel, userID, botID, text, ts string, isDirect bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	c.mu.Lock()
	self := c.selfID
	c.mu.Unlock()
	if botID != "" {
		return // any bot-authored message (including our own) is skipped
	}
	if userID != "" && userID == self {
		return
	}
	if !isDirect && !c.channelAllowed(channel) {
		return
	}

	env := agent.NewInbound("slack", channel, userID, text)
	env.IsDirect = isDirect
	env.Metadata["message_ts"] = ts
	env.Metadata["channel"] = channel
	env.Metadata["user_id"] = userID
	c.rememberRef(env.ID, replyRef{channel: channel, ts: ts})

	select {
	case c.inbox <- env:
	case <-c.done:
	}
}

// channelAllowed reports whether a channel id passes the allow-list. Empty = all.
func (c *Connector) channelAllowed(channel string) bool {
	if len(c.allow) == 0 {
		return true
	}
	_, ok := c.allow[channel]
	return ok
}

// Send delivers a handler-produced outbound message. When the envelope
// references an inbound one (ReplyTo) whose native ts is still known, it is sent
// into that thread; otherwise a plain channel message.
func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	a := c.getAPI()
	if a == nil {
		return errors.New("slack: not connected")
	}
	opts := []slackapi.MsgOption{slackapi.MsgOptionText(env.Text, false)}
	if env.ReplyTo != nil {
		if ref, ok := c.lookupRef(*env.ReplyTo); ok && ref.ts != "" {
			opts = append(opts, slackapi.MsgOptionTS(ref.ts))
		}
	}
	_, _, err := a.PostMessage(env.Channel, opts...)
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
			"connector": "slack",
			"channel":   channel,
			"sender":    c.BotName(),
			"text":      text,
		},
	})
}

// Stop unblocks any in-flight inbound push. Idempotent. The Socket Mode loop
// unwinds on ctx cancel (owned by Run), so there is nothing to close here.
func (c *Connector) Stop(context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	return nil
}
