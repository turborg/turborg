package skill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
)

// webhookTimeout bounds a single outbound webhook POST so a slow endpoint
// can't stall the engine.
const webhookTimeout = 5 * time.Second

// PostFunc posts a JSON payload to a URL. It is the seam external flow engines
// (n8n and the like) plug into; the default uses net/http and tests substitute
// a recorder.
type PostFunc func(ctx context.Context, url string, payload []byte) error

// floodWindow is the sliding window over which an effect skill with severity
// thresholds counts a sender's messages. Kept a package constant — operators
// tune the action via the threshold counts, not the window.
const floodWindow = 10 * time.Second

// classifyMaxTokens caps an llm_classify verdict — a tiny JSON object.
const classifyMaxTokens = 128

// classifySystemPrompt steers an llm_classify action toward a compact,
// machine-parseable verdict.
const classifySystemPrompt = "You are a chat moderation classifier. Reply with ONLY a compact JSON " +
	"object and nothing else: {\"severity\":N,\"action\":\"...\",\"reason\":\"...\"} where severity is an " +
	"integer 0-3 (0 harmless, 3 severe), and action is one of none|warn|mute|kick|ban. No prose."

// Engine runs the event/match skills: it subscribes to the agent's EventBus,
// tests each inbound message (match) and lifecycle event (event) against its
// skills, and executes the matching skill's action via the connector-agnostic
// Actor or the LLM provider. The skill set is hot-swappable (ReplaceSkills)
// under a write lock, mirroring CommandRegistry.ReplaceDynamic semantics with
// a MaxSkills cap.
type Engine struct {
	actor    agent.Actor
	provider llm.Provider
	store    Store
	post     PostFunc
	platform string
	owner    string
	log      *slog.Logger

	mu        sync.RWMutex
	maxSkills int
	events    []compiled
	matches   []compiled
	bus       *agent.EventBus
}

// compiled is a runtime-ready skill: the definition plus the compiled match
// regex (match kind only) and a normalized channel-scope set.
type compiled struct {
	skill    Skill
	re       *regexp.Regexp
	channels map[string]struct{} // nil = all channels
}

// Options configures an Engine. A nil Store defaults to an in-process one; a
// nil Actor/Provider degrades the corresponding actions to no-ops gracefully.
type Options struct {
	Actor     agent.Actor
	Provider  llm.Provider
	Store     Store
	Platform  string
	Owner     string
	MaxSkills int
	// Post overrides the outbound webhook poster. Nil uses a net/http default.
	Post PostFunc
	Log  *slog.Logger
}

// NewEngine builds an Engine. It holds no skills until ReplaceSkills is called.
func NewEngine(o Options) *Engine {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	store := o.Store
	if store == nil {
		store = NewMemoryStore()
	}
	post := o.Post
	if post == nil {
		post = defaultPost
	}
	return &Engine{
		actor:     o.Actor,
		provider:  o.Provider,
		store:     store,
		post:      post,
		platform:  o.Platform,
		owner:     o.Owner,
		log:       log.With("component", "skill-engine"),
		maxSkills: o.MaxSkills,
	}
}

// defaultPost POSTs payload as application/json with a bounded timeout.
func defaultPost(ctx context.Context, url string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, webhookTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// SetMaxSkills caps how many engine skills ReplaceSkills installs: 0 = none
// (default), -1 = unrestricted, N = at most N. Mirrors the command registry's
// MaxDynamic cap.
func (e *Engine) SetMaxSkills(n int) {
	e.mu.Lock()
	e.maxSkills = n
	e.mu.Unlock()
}

// ReplaceSkills atomically swaps the engine's event/match skill set for the
// given batch (command and schedule kinds are ignored). A skill with an
// invalid match regex or an unknown event name is dropped with a log line.
// The MaxSkills cap is enforced as a safety net.
func (e *Engine) ReplaceSkills(skills []Skill) {
	var events, matches []compiled
	e.mu.RLock()
	limit := e.maxSkills
	e.mu.RUnlock()

	installed := 0
	for _, s := range skills {
		if limit != -1 && installed >= limit {
			break
		}
		switch s.Trigger.Kind {
		case KindMatch:
			re, err := regexp.Compile(s.Trigger.Match)
			if err != nil {
				e.log.Warn("skipping match skill: bad regex", "skill", s.Name, "err", err)
				continue
			}
			matches = append(matches, compiled{skill: s, re: re, channels: channelSet(s.Trigger.Channels)})
			installed++
		case KindEvent:
			if !validEvent(s.Trigger.Event) {
				e.log.Warn("skipping event skill: unknown event", "skill", s.Name, "event", s.Trigger.Event)
				continue
			}
			events = append(events, compiled{skill: s, channels: channelSet(s.Trigger.Channels)})
			installed++
		default:
			// command / schedule kinds are not the Engine's concern.
		}
	}

	e.mu.Lock()
	e.events = events
	e.matches = matches
	e.mu.Unlock()
}

// Subscribe wires the engine onto the agent's EventBus: inbound messages drive
// match skills and the user-event types drive event skills. Call once.
func (e *Engine) Subscribe(bus *agent.EventBus) {
	e.mu.Lock()
	e.bus = bus
	e.mu.Unlock()
	bus.Subscribe(agent.EventMessage, e.onMessage)
	for _, t := range []agent.EventType{
		agent.EventUserJoin, agent.EventUserLeave, agent.EventUserKicked,
		agent.EventUserNickChange, agent.EventTopicChanged, agent.EventModeChanged,
	} {
		bus.Subscribe(t, e.onUserEvent)
	}
}

func (e *Engine) onMessage(ctx context.Context, ev *agent.Event) {
	env, ok := ev.Fields["envelope"].(*agent.InboundEnvelope)
	if !ok || env == nil {
		return
	}
	e.mu.RLock()
	matches := e.matches
	e.mu.RUnlock()
	if len(matches) == 0 {
		return
	}
	f := renderFields{
		user:     env.Sender,
		channel:  env.Channel,
		text:     env.Text,
		platform: e.platform,
		owner:    e.owner,
	}
	for _, m := range matches {
		if !channelAllowed(m.channels, env.Channel) {
			continue
		}
		if m.re != nil && !m.re.MatchString(env.Text) {
			continue
		}
		e.fire(ctx, m.skill, f)
	}
}

func (e *Engine) onUserEvent(ctx context.Context, ev *agent.Event) {
	e.mu.RLock()
	events := e.events
	e.mu.RUnlock()
	if len(events) == 0 {
		return
	}
	name := string(ev.Type)
	f := fieldsFromEvent(ev)
	f.platform = e.platform
	f.owner = e.owner
	for _, m := range events {
		if m.skill.Trigger.Event != name {
			continue
		}
		if !channelAllowed(m.channels, f.channel) {
			continue
		}
		e.fire(ctx, m.skill, f)
	}
}

// fire executes a skill's action against the rendered fields.
func (e *Engine) fire(ctx context.Context, s Skill, f renderFields) {
	switch s.Action.Type {
	case TypeStatic:
		e.doReply(s, f)
	case TypeLLM:
		e.doLLM(ctx, s, f)
	case TypeEffect:
		e.doEffect(ctx, s, f)
	case TypeLLMClassify:
		e.doClassify(ctx, s, f)
	case TypeWebhook:
		e.doWebhook(ctx, s, f)
	default:
		e.log.Warn("skill has unknown action type", "skill", s.Name, "type", s.Action.Type)
	}
}

// doWebhook POSTs the trigger context (plus the rendered template as "message")
// as JSON to the skill's webhook URL — the bridge to external flow engines.
func (e *Engine) doWebhook(ctx context.Context, s Skill, f renderFields) {
	if s.Action.Webhook == "" {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"skill":    s.Name,
		"trigger":  string(s.Trigger.Kind),
		"event":    s.Trigger.Event,
		"channel":  f.channel,
		"user":     f.user,
		"text":     f.text,
		"target":   f.target,
		"reason":   f.reason,
		"topic":    f.topic,
		"modes":    f.modes,
		"old":      f.oldNick,
		"new":      f.newNick,
		"platform": f.platform,
		"message":  e.renderTmpl(s.Name, s.Action.Template, f),
	})
	if err != nil {
		return
	}
	if err := e.post(ctx, s.Action.Webhook, payload); err != nil {
		e.log.Warn("skill webhook failed", "skill", s.Name, "err", err)
	}
}

// doReply renders the template and sends it to the channel. Empty channel or
// empty rendered text is a no-op.
func (e *Engine) doReply(s Skill, f renderFields) {
	if e.actor == nil || f.channel == "" {
		return
	}
	text := strings.TrimSpace(e.renderTmpl(s.Name, s.Action.Template, f))
	if text == "" {
		return
	}
	if err := e.actor.Say(f.channel, text); err != nil {
		e.log.Warn("skill reply failed", "skill", s.Name, "err", err)
	}
}

func (e *Engine) doLLM(ctx context.Context, s Skill, f renderFields) {
	if e.provider == nil || e.actor == nil || f.channel == "" {
		return
	}
	prompt := e.renderTmpl(s.Name, s.Action.Template, f)
	if strings.TrimSpace(prompt) == "" {
		prompt = f.text
	}
	system := strings.TrimSpace(s.Action.Instructions)
	opts := []llm.CallOption{llm.WithMaxTokens(classifyMaxTokens * 4)}
	if system != "" {
		opts = append(opts, llm.WithSystem(system))
	}
	if s.Action.Model != "" {
		opts = append(opts, llm.WithModel(s.Action.Model))
	}
	answer, _, err := e.provider.Ask(ctx, prompt, opts...)
	if err != nil {
		if !errors.Is(err, llm.ErrBudgetExhausted) {
			e.log.Warn("skill llm failed", "skill", s.Name, "err", err)
		}
		return
	}
	answer = strings.Join(strings.Fields(answer), " ")
	if answer == "" {
		return
	}
	if err := e.actor.Say(f.channel, answer); err != nil {
		e.log.Warn("skill reply failed", "skill", s.Name, "err", err)
	}
}

// doEffect runs a moderation effect. With effect thresholds it is a flood gate
// keyed off a per-sender sliding-window message count; without thresholds it
// runs the effect's action directly.
func (e *Engine) doEffect(ctx context.Context, s Skill, f renderFields) {
	eff := s.Action.Effect
	if eff == nil {
		return
	}
	act := eff.Action
	if eff.Thresholds != nil {
		count, _ := e.store.Incr(s.Name, strings.ToLower(f.user), floodWindow)
		f.count = count
		act = actionForSeverity(count, eff.Thresholds)
		if act == "" {
			return
		}
	}
	e.applyEffect(ctx, s, f, eff, act)
}

func (e *Engine) doClassify(ctx context.Context, s Skill, f renderFields) {
	eff := s.Action.Effect
	if eff == nil || eff.Thresholds == nil {
		return
	}
	// Degrade gracefully to no action when the LLM is unavailable: the regex
	// prefilter already matched, but classification (and therefore any
	// punitive effect) requires the model — fail safe rather than guess.
	if e.provider == nil {
		return
	}
	system := classifySystemPrompt
	if instr := strings.TrimSpace(s.Action.Instructions); instr != "" {
		system = instr + "\n" + classifySystemPrompt
	}
	opts := []llm.CallOption{llm.WithSystem(system), llm.WithMaxTokens(classifyMaxTokens)}
	if s.Action.Model != "" {
		opts = append(opts, llm.WithModel(s.Action.Model))
	}
	answer, _, err := e.provider.Ask(ctx, f.text, opts...)
	if err != nil {
		// Budget exhausted (or any provider error) degrades to no action.
		if !errors.Is(err, llm.ErrBudgetExhausted) {
			e.log.Warn("skill classify failed", "skill", s.Name, "err", err)
		}
		return
	}
	verdict, ok := parseVerdict(answer)
	if !ok {
		e.log.Debug("skill classify: unparseable verdict", "skill", s.Name)
		return
	}
	f.reason = verdict.Reason
	act := actionForSeverity(verdict.Severity, eff.Thresholds)
	if act == "" {
		return
	}
	e.applyEffect(ctx, s, f, eff, act)
}

// applyEffect performs a single resolved moderation action via the Actor and
// emits an audit MODERATION event.
func (e *Engine) applyEffect(ctx context.Context, s Skill, f renderFields, eff *Effect, act EffectAction) {
	if e.actor == nil || f.channel == "" {
		return
	}
	// The offender is the acting user for message/effect triggers, or the
	// affected target for events that name one (kick/nick-change).
	offender := f.user
	if f.target != "" {
		offender = f.target
	}
	reason := eff.Reason
	if reason != "" {
		reason = e.renderTmpl(s.Name, reason, f)
	}
	var err error
	switch act {
	case EffectKick:
		err = e.actor.Kick(f.channel, offender, reason)
	case EffectBan:
		err = e.actor.Ban(f.channel, offender)
	case EffectMute:
		err = e.actor.SetMode(f.channel, "+q", offender)
	case EffectOp:
		err = e.actor.Op(f.channel, offender)
	case EffectVoice:
		err = e.actor.Voice(f.channel, offender)
	case EffectMode:
		err = e.actor.SetMode(f.channel, eff.Modes)
	case EffectNotice:
		text := reason
		if text == "" {
			text = e.renderTmpl(s.Name, s.Action.Template, f)
		}
		err = e.actor.Notice(f.channel, text)
	case EffectTopic:
		err = e.actor.Topic(f.channel, e.renderTmpl(s.Name, s.Action.Template, f))
	default:
		return
	}
	if err != nil {
		e.log.Warn("skill effect failed", "skill", s.Name, "action", act, "err", err)
	}
	e.publishModeration(ctx, s.Name, string(act), f.channel, offender, reason)
}

func (e *Engine) publishModeration(ctx context.Context, skill, action, channel, target, reason string) {
	e.mu.RLock()
	bus := e.bus
	e.mu.RUnlock()
	if bus == nil {
		return
	}
	bus.Publish(ctx, &agent.Event{
		Type: agent.EventModeration,
		Fields: map[string]any{
			"skill":   skill,
			"action":  action,
			"channel": channel,
			"target":  target,
			"reason":  reason,
		},
	})
}

// verdict is the structured llm_classify response.
type verdict struct {
	Severity int    `json:"severity"`
	Action   string `json:"action"`
	Reason   string `json:"reason"`
}

// parseVerdict extracts the JSON verdict object from an LLM reply, tolerating
// surrounding prose by slicing between the first '{' and last '}'.
func parseVerdict(answer string) (verdict, bool) {
	start := strings.IndexByte(answer, '{')
	end := strings.LastIndexByte(answer, '}')
	if start < 0 || end <= start {
		return verdict{}, false
	}
	var v verdict
	if err := json.Unmarshal([]byte(answer[start:end+1]), &v); err != nil {
		return verdict{}, false
	}
	if v.Severity < 0 {
		v.Severity = 0
	}
	return v, true
}

// actionForSeverity maps a 0..3 severity (or a flood count) onto the highest
// matched threshold tier. Returns "" when no tier is reached.
func actionForSeverity(s int, th *Thresholds) EffectAction {
	if th == nil {
		return ""
	}
	switch {
	case th.Ban > 0 && s >= th.Ban:
		return EffectBan
	case th.Kick > 0 && s >= th.Kick:
		return EffectKick
	case th.Mute > 0 && s >= th.Mute:
		return EffectMute
	case th.Warn > 0 && s >= th.Warn:
		return EffectNotice
	default:
		return ""
	}
}

// Store-helper patterns: {getvar:key} reads, {setvar:key,value} writes (and
// renders empty). They run before the field substitution.
var (
	getvarRe = regexp.MustCompile(`\{getvar:([^},]*)\}`)
	setvarRe = regexp.MustCompile(`\{setvar:([^},]*),([^}]*)\}`)
)

// renderTmpl expands the store helpers (scoped to the skill), then the
// standard placeholders and dynamic helpers.
func (e *Engine) renderTmpl(skillName, tmpl string, f renderFields) string {
	tmpl = setvarRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		sm := setvarRe.FindStringSubmatch(m)
		e.store.Set(skillName, strings.TrimSpace(sm[1]), strings.TrimSpace(sm[2]), 0)
		return ""
	})
	tmpl = getvarRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		key := strings.TrimSpace(getvarRe.FindStringSubmatch(m)[1])
		v, _ := e.store.Get(skillName, key)
		return v
	})
	return render(tmpl, f)
}

// fieldsFromEvent maps an EventBus user-event's fields onto renderFields.
func fieldsFromEvent(ev *agent.Event) renderFields {
	get := func(k string) string {
		v, _ := ev.Fields[k].(string)
		return v
	}
	f := renderFields{
		channel: get("channel"),
		reason:  get("reason"),
		topic:   get("topic"),
		modes:   get("modes"),
	}
	switch ev.Type {
	case agent.EventUserKicked:
		f.user = get("by")
		f.target = get("nick")
	case agent.EventUserNickChange:
		f.oldNick = get("old")
		f.newNick = get("new")
		f.user = get("old")
		f.target = get("new")
	case agent.EventTopicChanged, agent.EventModeChanged:
		f.user = get("by")
	default: // join / leave
		f.user = get("nick")
	}
	return f
}

func validEvent(name string) bool {
	switch name {
	case EventUserJoin, EventUserLeave, EventUserKicked,
		EventUserNickChange, EventTopicChanged, EventModeChanged:
		return true
	default:
		return false
	}
}

// channelSet normalizes a channel scope list into a lowercase lookup set, or
// nil when the scope is empty (all channels).
func channelSet(channels []string) map[string]struct{} {
	if len(channels) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(channels))
	for _, c := range channels {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			out[c] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func channelAllowed(set map[string]struct{}, channel string) bool {
	if set == nil {
		return true
	}
	_, ok := set[strings.ToLower(channel)]
	return ok
}
