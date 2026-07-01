package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/skill"
)

// maxSteps bounds how many nodes a single flow run may execute, so a malformed
// or cyclic graph can never run away.
const maxSteps = 256

// webhookTimeout bounds a single webhook node POST.
const webhookTimeout = 5 * time.Second

// Engine runs event/match-triggered flows: it subscribes to the agent's
// EventBus, seeds a data bag from each inbound message / lifecycle event, and
// walks every flow whose trigger matches. The flow set is hot-swappable
// (ReplaceFlows) under a write lock with a MaxFlows cap, mirroring the skill
// engine.
type Engine struct {
	actor    agent.Actor
	provider llm.Provider
	store    skill.Store
	post     PostFunc
	platform string
	owner    string
	log      *slog.Logger

	mu       sync.RWMutex
	maxFlows int
	events   []compiled
	matches  []compiled
	// webhooks indexes inbound-webhook-trigger flows by lowercased name so
	// FireWebhook can dispatch an external POST in O(1) without leaking the set.
	webhooks map[string]compiled
}

// compiled is a runtime-ready flow: the definition plus the compiled match
// regex (match trigger only) and a normalized channel scope.
type compiled struct {
	flow     Flow
	re       *regexp.Regexp
	channels map[string]struct{}
}

// Options configures an Engine.
type Options struct {
	Actor    agent.Actor
	Provider llm.Provider
	Store    skill.Store
	Platform string
	Owner    string
	MaxFlows int
	Post     PostFunc
	Log      *slog.Logger
}

// NewEngine builds a flow engine. It holds no flows until ReplaceFlows.
func NewEngine(o Options) *Engine {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	store := o.Store
	if store == nil {
		store = skill.NewMemoryStore()
	}
	post := o.Post
	if post == nil {
		post = defaultPost
	}
	return &Engine{
		actor:    o.Actor,
		provider: o.Provider,
		store:    store,
		post:     post,
		platform: o.Platform,
		owner:    o.Owner,
		log:      log.With("component", "flow-engine"),
		maxFlows: o.MaxFlows,
	}
}

func defaultPost(ctx context.Context, method, url string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, webhookTimeout)
	defer cancel()
	if method == "" {
		method = http.MethodPost
	}
	// A GET carries no request body; other verbs send the payload as JSON.
	var body io.Reader
	if method != http.MethodGet {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// SetMaxFlows caps how many flows ReplaceFlows installs: 0 = none (default),
// -1 = unrestricted, N = at most N.
func (e *Engine) SetMaxFlows(n int) {
	e.mu.Lock()
	e.maxFlows = n
	e.mu.Unlock()
}

// ReplaceFlows atomically swaps the engine's event/match flow set. A flow whose
// nodes are unknown, whose graph is invalid, or whose match regex is bad is
// dropped with a log line. The MaxFlows cap is enforced as a safety net.
func (e *Engine) ReplaceFlows(flows []Flow) {
	e.mu.RLock()
	limit := e.maxFlows
	e.mu.RUnlock()

	var events, matches []compiled
	webhooks := map[string]compiled{}
	installed := 0
	for _, f := range flows {
		if limit != -1 && installed >= limit {
			break
		}
		if err := validate(f); err != nil {
			e.log.Warn("skipping invalid flow", "flow", f.Name, "err", err)
			continue
		}
		switch f.triggerKind() {
		case skill.KindMatch:
			re, err := regexp.Compile(f.Trigger.Match)
			if err != nil {
				e.log.Warn("skipping match flow: bad regex", "flow", f.Name, "err", err)
				continue
			}
			matches = append(matches, compiled{flow: f, re: re, channels: channelSet(f.Trigger.Channels)})
			installed++
		case skill.KindEvent:
			events = append(events, compiled{flow: f, channels: channelSet(f.Trigger.Channels)})
			installed++
		case skill.KindWebhook:
			key := strings.ToLower(strings.TrimSpace(f.Name))
			if key == "" {
				e.log.Warn("skipping webhook flow: empty name")
				continue
			}
			webhooks[key] = compiled{flow: f, channels: channelSet(f.Trigger.Channels)}
			installed++
		default:
			// command / schedule flows are not the bus engine's concern.
		}
	}

	e.mu.Lock()
	e.events = events
	e.matches = matches
	e.webhooks = webhooks
	e.mu.Unlock()
}

// FireWebhook walks the flow whose webhook trigger matches name (case-
// insensitive), seeding the data bag from bag. It returns true when a matching
// webhook flow ran, false when no flow carries that name — the ingress handler
// maps false onto a 404 with no detail so a caller can't enumerate the set. The
// bag's "channel" is the trigger's first scoped channel when set, else whatever
// the caller seeded.
func (e *Engine) FireWebhook(name string, bag map[string]string) bool {
	e.mu.RLock()
	c, ok := e.webhooks[strings.ToLower(strings.TrimSpace(name))]
	e.mu.RUnlock()
	if !ok {
		return false
	}
	b := Bag{}
	for k, v := range bag {
		b[k] = v
	}
	if chans := c.flow.Trigger.Channels; len(chans) > 0 {
		b["channel"] = chans[0]
	}
	b["platform"] = e.platform
	b["owner"] = e.owner
	e.run(context.Background(), c.flow, b)
	return true
}

// validate checks a flow's node types are all registered. An empty graph is
// valid (it simply does nothing).
func validate(f Flow) error {
	for _, n := range f.Nodes {
		if _, ok := lookup(n.Type); !ok {
			return &unknownNodeError{n.Type}
		}
	}
	return nil
}

type unknownNodeError struct{ typ string }

func (e *unknownNodeError) Error() string { return "unknown node type " + e.typ }

// Subscribe wires the engine onto the agent's EventBus.
func (e *Engine) Subscribe(bus *agent.EventBus) {
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
	for _, m := range matches {
		if !channelAllowed(m.channels, env.Channel) {
			continue
		}
		if m.re != nil && !m.re.MatchString(env.Text) {
			continue
		}
		bag := Bag{
			"user": env.Sender, "channel": env.Channel, "text": env.Text,
			"platform": e.platform, "owner": e.owner,
		}
		e.run(ctx, m.flow, bag)
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
	bag := bagFromEvent(ev)
	bag["platform"] = e.platform
	bag["owner"] = e.owner
	channel := bag.str("channel")
	for _, m := range events {
		if m.flow.Trigger.Event != name {
			continue
		}
		if !channelAllowed(m.channels, channel) {
			continue
		}
		e.run(ctx, m.flow, cloneBag(bag))
	}
}

// run walks the flow graph from its entry nodes, executing each node and
// following the output port it returns. Execution is bounded by maxSteps.
func (e *Engine) run(ctx context.Context, f Flow, bag Bag) {
	nc := &nodeContext{
		actor: e.actor, provider: e.provider, store: e.store,
		post: e.post, flowName: f.Name, bag: bag,
	}
	queue := f.entryNodes()
	index := make(map[string]Node, len(f.Nodes))
	for _, n := range f.Nodes {
		index[n.ID] = n
	}
	steps := 0
	for len(queue) > 0 {
		if steps >= maxSteps {
			e.log.Warn("flow exceeded step ceiling", "flow", f.Name)
			return
		}
		steps++
		id := queue[0]
		queue = queue[1:]
		node, ok := index[id]
		if !ok {
			continue
		}
		nt, ok := lookup(node.Type)
		if !ok {
			continue
		}
		port, err := nt.Handler(ctx, node, nc)
		if err != nil {
			e.log.Warn("flow node failed", "flow", f.Name, "node", node.ID, "type", node.Type, "err", err)
			continue
		}
		if port == "\x00stop" {
			continue
		}
		queue = append(queue, f.successors(id, port)...)
	}
}

// RunOnce executes a flow against a caller-supplied bag, ignoring the trigger.
// It is the entry the command/schedule paths use once those wire flows in, and
// a test seam.
func (e *Engine) RunOnce(ctx context.Context, f Flow, bag Bag) {
	if bag == nil {
		bag = Bag{}
	}
	if bag["platform"] == nil {
		bag["platform"] = e.platform
	}
	if bag["owner"] == nil {
		bag["owner"] = e.owner
	}
	e.run(ctx, f, bag)
}

func bagFromEvent(ev *agent.Event) Bag {
	get := func(k string) string {
		v, _ := ev.Fields[k].(string)
		return v
	}
	bag := Bag{
		"channel": get("channel"), "reason": get("reason"),
		"topic": get("topic"), "modes": get("modes"),
	}
	switch ev.Type {
	case agent.EventUserKicked:
		bag["user"] = get("by")
		bag["target"] = get("nick")
	case agent.EventUserNickChange:
		bag["old"] = get("old")
		bag["new"] = get("new")
		bag["user"] = get("old")
	case agent.EventTopicChanged, agent.EventModeChanged:
		bag["user"] = get("by")
	default:
		bag["user"] = get("nick")
	}
	return bag
}

func cloneBag(b Bag) Bag {
	out := make(Bag, len(b))
	for k, v := range b {
		out[k] = v
	}
	return out
}

func channelSet(channels []string) map[string]struct{} {
	if len(channels) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(channels))
	for _, c := range channels {
		if c = strings.ToLower(strings.TrimSpace(c)); c != "" {
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

// jsonMarshal is a tiny indirection so the webhook node and tests share one
// encoder.
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
