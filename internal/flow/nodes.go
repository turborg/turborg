package flow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/skill"
)

// Bag is the JSON-friendly data envelope threaded through a flow's nodes. It is
// seeded from the trigger (user, channel, text, …) and mutated by nodes (set,
// llm, getvar, …) as execution proceeds.
type Bag map[string]any

func (b Bag) str(key string) string {
	switch v := b[key].(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

// nodeContext carries the shared dependencies and the live data bag to a node
// handler.
type nodeContext struct {
	actor    agent.Actor
	provider llm.Provider
	store    skill.Store
	post     PostFunc
	flowName string
	bag      Bag
	log      *slog.Logger
}

// PostFunc sends a payload to a URL with the given HTTP method and custom
// request headers, returning the (bounded) response body. It is the webhook
// node's egress seam; tests substitute a recorder.
type PostFunc func(ctx context.Context, method, url string, headers map[string]string, payload []byte) ([]byte, error)

// Handler runs one node. It mutates nc.bag and returns the output port to
// follow next ("" for the default single output).
type Handler func(ctx context.Context, n Node, nc *nodeContext) (port string, err error)

// NodeType is a registered activity kind: a stable name, the output ports it
// can emit (for the builder UI to render branches), a JSON-schema-ish config
// descriptor (for the UI to render fields), and the handler.
type NodeType struct {
	Name    string         `json:"name"`
	Ports   []string       `json:"ports"`
	Config  map[string]any `json:"config"`
	Handler Handler        `json:"-"`
}

// registry is the process-wide node catalog. It is populated by the package
// init below and is read-only thereafter, so concurrent flow execution needs
// no lock.
var registry = map[string]NodeType{}

// Register adds a node type to the catalog. Re-registering a name overwrites
// it. Intended for init-time wiring (the built-in catalog) and embedders that
// extend the catalog before any flow runs.
func Register(nt NodeType) { registry[nt.Name] = nt }

// Types returns the node catalog sorted by name, for UI introspection.
func Types() []NodeType {
	out := make([]NodeType, 0, len(registry))
	for _, nt := range registry {
		out = append(out, nt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func lookup(name string) (NodeType, bool) {
	nt, ok := registry[name]
	return nt, ok
}

func init() {
	Register(NodeType{Name: "set", Ports: []string{""}, Config: map[string]any{"fields": "map[string]string"}, Handler: nodeSet})
	Register(NodeType{Name: "if", Ports: []string{"true", "false"}, Config: map[string]any{"left": "string", "op": "eq|ne|contains|matches|gt|lt", "right": "string"}, Handler: nodeIf})
	Register(NodeType{Name: "switch", Ports: []string{"<case>", "default"}, Config: map[string]any{"value": "string", "cases": "[]string"}, Handler: nodeSwitch})
	Register(NodeType{Name: "stop", Ports: nil, Config: map[string]any{}, Handler: nodeStop})
	Register(NodeType{Name: "say", Ports: []string{""}, Config: map[string]any{"channel": "string", "text": "string"}, Handler: nodeSay})
	Register(NodeType{Name: "notice", Ports: []string{""}, Config: map[string]any{"target": "string", "text": "string"}, Handler: nodeNotice})
	Register(NodeType{Name: "effect", Ports: []string{""}, Config: map[string]any{"action": "kick|ban|mute|op|voice|mode|topic", "channel": "string", "target": "string", "modes": "string", "reason": "string"}, Handler: nodeEffect})
	Register(NodeType{Name: "llm", Ports: []string{""}, Config: map[string]any{"prompt": "string", "system": "string", "model": "string", "into": "string"}, Handler: nodeLLM})
	Register(NodeType{Name: "webhook", Ports: []string{""}, Config: map[string]any{"url": "string", "method": "GET|POST|PUT|PATCH|DELETE", "body": "string", "headers": "map[string]string", "into": "string", "retries": "int", "retry_backoff": "int"}, Handler: nodeWebhook})
	Register(NodeType{Name: "setvar", Ports: []string{""}, Config: map[string]any{"key": "string", "value": "string"}, Handler: nodeSetvar})
	Register(NodeType{Name: "getvar", Ports: []string{""}, Config: map[string]any{"key": "string", "into": "string"}, Handler: nodeGetvar})
	Register(NodeType{Name: "incr", Ports: []string{""}, Config: map[string]any{"key": "string", "by": "string", "into": "string"}, Handler: nodeIncr})
}

// cfg reads a string config value, rendered against the bag.
func cfg(n Node, key string, bag Bag) string {
	v, _ := n.Config[key].(string)
	return renderBag(v, bag)
}

func rawCfg(n Node, key string) string {
	v, _ := n.Config[key].(string)
	return v
}

// nodeSet renders each {fields} template and writes it into the bag.
func nodeSet(_ context.Context, n Node, nc *nodeContext) (string, error) {
	fields, _ := n.Config["fields"].(map[string]any)
	for k, raw := range fields {
		tmpl, _ := raw.(string)
		nc.bag[k] = renderBag(tmpl, nc.bag)
	}
	return "", nil
}

// nodeIf evaluates a condition and routes to the true/false port.
func nodeIf(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if evalCond(rawCfg(n, "op"), cfg(n, "left", nc.bag), cfg(n, "right", nc.bag)) {
		return "true", nil
	}
	return "false", nil
}

// nodeSwitch routes to the port named by the rendered value when it is one of
// the listed cases, else to "default".
func nodeSwitch(_ context.Context, n Node, nc *nodeContext) (string, error) {
	val := cfg(n, "value", nc.bag)
	cases, _ := n.Config["cases"].([]any)
	for _, c := range cases {
		if s, _ := c.(string); s == val {
			return val, nil
		}
	}
	return "default", nil
}

// nodeStop ends this branch (no outgoing ports).
func nodeStop(context.Context, Node, *nodeContext) (string, error) { return "\x00stop", nil }

func nodeSay(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.actor == nil {
		return "", nil
	}
	channel := cfg(n, "channel", nc.bag)
	if channel == "" {
		channel = nc.bag.str("channel")
	}
	text := cfg(n, "text", nc.bag)
	if channel == "" || text == "" {
		return "", nil
	}
	return "", nc.actor.Say(channel, text)
}

func nodeNotice(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.actor == nil {
		return "", nil
	}
	target := cfg(n, "target", nc.bag)
	if target == "" {
		target = nc.bag.str("channel")
	}
	text := cfg(n, "text", nc.bag)
	if target == "" || text == "" {
		return "", nil
	}
	return "", nc.actor.Notice(target, text)
}

func nodeEffect(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.actor == nil {
		return "", nil
	}
	channel := cfg(n, "channel", nc.bag)
	if channel == "" {
		channel = nc.bag.str("channel")
	}
	target := cfg(n, "target", nc.bag)
	if target == "" {
		target = nc.bag.str("user")
	}
	if channel == "" {
		return "", nil
	}
	reason := cfg(n, "reason", nc.bag)
	switch skill.EffectAction(rawCfg(n, "action")) {
	case skill.EffectKick:
		return "", nc.actor.Kick(channel, target, reason)
	case skill.EffectBan:
		return "", nc.actor.Ban(channel, target)
	case skill.EffectMute:
		return "", nc.actor.SetMode(channel, "+q", target)
	case skill.EffectOp:
		return "", nc.actor.Op(channel, target)
	case skill.EffectVoice:
		return "", nc.actor.Voice(channel, target)
	case skill.EffectMode:
		return "", nc.actor.SetMode(channel, cfg(n, "modes", nc.bag))
	case skill.EffectTopic:
		return "", nc.actor.Topic(channel, cfg(n, "reason", nc.bag))
	default:
		return "", nil
	}
}

func nodeLLM(ctx context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.provider == nil {
		return "", nil
	}
	opts := []llm.CallOption{llm.WithMaxTokens(512)}
	if sys := cfg(n, "system", nc.bag); sys != "" {
		opts = append(opts, llm.WithSystem(sys))
	}
	if model := rawCfg(n, "model"); model != "" {
		opts = append(opts, llm.WithModel(model))
	}
	answer, _, err := nc.provider.Ask(ctx, cfg(n, "prompt", nc.bag), opts...)
	if err != nil {
		return "", err
	}
	into := rawCfg(n, "into")
	if into == "" {
		into = "llm"
	}
	nc.bag[into] = strings.Join(strings.Fields(answer), " ")
	return "", nil
}

// webhook retry/backoff bounds. Config is clamped into these ranges.
const (
	maxWebhookRetries = 5
	defaultBackoffSec = 3
	minBackoffSec     = 1
	maxBackoffSec     = 30
	maxWebhookBackoff = maxBackoffSec * time.Second
)

func nodeWebhook(ctx context.Context, n Node, nc *nodeContext) (string, error) {
	url := cfg(n, "url", nc.bag)
	if url == "" || nc.post == nil {
		return "", nil
	}
	method := webhookMethod(cfg(n, "method", nc.bag))
	headers := webhookHeaders(n, nc.bag)
	// Optional custom body: a template rendered against the bag (e.g. your own
	// JSON with {user}/{channel}/… placeholders). Empty falls back to posting
	// the whole data bag as JSON.
	var payload []byte
	if body := strings.TrimSpace(cfg(n, "body", nc.bag)); body != "" {
		payload = []byte(body)
	} else {
		p, err := jsonMarshal(nc.bag)
		if err != nil {
			return "", err
		}
		payload = p
	}
	retries := clamp(cfgInt(n, "retries", 0), 0, maxWebhookRetries)
	backoff := clamp(cfgInt(n, "retry_backoff", defaultBackoffSec), minBackoffSec, maxBackoffSec)

	resp, err := postWithRetry(ctx, nc.post, method, url, headers, payload, retries, backoff)
	if err != nil {
		// A webhook failure is logged, not fatal: the flow continues so a
		// downstream node can degrade gracefully (an unset capture key renders
		// empty).
		webhookLog(nc).Warn("flow webhook failed", "flow", nc.flowName, "url", url, "retries", retries, "err", err)
		return "", nil
	}
	// Optional response capture: bound the body and stash it under the given
	// bag key so a later node can use it (e.g. a GET that fetches JSON).
	if into := rawCfg(n, "into"); into != "" {
		if len(resp) > maxWebhookBody {
			resp = resp[:maxWebhookBody]
		}
		nc.bag[into] = string(resp)
	}
	return "", nil
}

// maxWebhookBody bounds a captured webhook response body (belt-and-braces on
// top of the poster's own limit).
const maxWebhookBody = 64 << 10 // 64 KiB

// webhookHeaders renders each configured header value against the bag so a user
// can interpolate a secret (e.g. {apitoken}) into an Authorization header.
func webhookHeaders(n Node, bag Bag) map[string]string {
	raw, ok := n.Config["headers"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		s, _ := v.(string)
		out[k] = renderBag(s, bag)
	}
	return out
}

// cfgInt reads an int config value, tolerating JSON's float64 numbers and
// string digits, falling back to def when absent or unparseable.
func cfgInt(n Node, key string, def int) int {
	switch v := n.Config[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return i
		}
	}
	return def
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// postWithRetry calls post, retrying transport errors and 5xx/429 responses up
// to retries times with a linearly increasing backoff (base, 2·base, …) capped
// at maxWebhookBackoff. 4xx responses (other than 429) are permanent and are
// not retried. Backoff waits respect ctx cancellation.
func postWithRetry(ctx context.Context, post PostFunc, method, url string, headers map[string]string, payload []byte, retries, backoffSec int) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	for attempt := 0; ; attempt++ {
		resp, err = post(ctx, method, url, headers, payload)
		if err == nil {
			return resp, nil
		}
		if attempt >= retries || !retryable(err) {
			return resp, err
		}
		wait := time.Duration(backoffSec*(attempt+1)) * time.Second
		if wait > maxWebhookBackoff {
			wait = maxWebhookBackoff
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return resp, ctx.Err()
		case <-timer.C:
		}
	}
}

// retryable reports whether a webhook error warrants a retry: any transport-
// level error, or a 5xx / 429 HTTP status. Other 4xx statuses are permanent.
func retryable(err error) bool {
	var se *httpStatusError
	if errors.As(err, &se) {
		return se.status == http.StatusTooManyRequests || se.status >= 500
	}
	return true
}

// httpStatusError carries a non-2xx webhook response status so the retry loop
// can distinguish retryable (5xx / 429) from permanent (other 4xx) failures.
type httpStatusError struct{ status int }

func (e *httpStatusError) Error() string { return "webhook: HTTP " + strconv.Itoa(e.status) }

func webhookLog(nc *nodeContext) *slog.Logger {
	if nc.log != nil {
		return nc.log
	}
	return slog.Default()
}

// webhookMethod normalizes a configured HTTP method to upper case and
// validates it against the supported verbs, defaulting to POST for an empty or
// unrecognized value.
func webhookMethod(m string) string {
	switch strings.ToUpper(strings.TrimSpace(m)) {
	case "GET":
		return "GET"
	case "PUT":
		return "PUT"
	case "PATCH":
		return "PATCH"
	case "DELETE":
		return "DELETE"
	default:
		return "POST"
	}
}

func nodeSetvar(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.store == nil {
		return "", nil
	}
	nc.store.Set(nc.flowName, cfg(n, "key", nc.bag), cfg(n, "value", nc.bag), 0)
	return "", nil
}

func nodeGetvar(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.store == nil {
		return "", nil
	}
	v, _ := nc.store.Get(nc.flowName, cfg(n, "key", nc.bag))
	into := rawCfg(n, "into")
	if into == "" {
		into = "var"
	}
	nc.bag[into] = v
	return "", nil
}

// nodeIncr atomically-for-this-tenant increments a persisted integer counter
// (default step 1, or "by") and writes the new value into the bag under "into"
// (default the key). This is the counter/score/karma primitive that a linear
// graph can't express with set alone. Backed by the persistent Store, so the
// tally survives restarts.
func nodeIncr(_ context.Context, n Node, nc *nodeContext) (string, error) {
	if nc.store == nil {
		return "", nil
	}
	key := cfg(n, "key", nc.bag)
	if key == "" {
		return "", nil
	}
	cur, _ := nc.store.Get(nc.flowName, key)
	base, _ := strconv.Atoi(strings.TrimSpace(cur))
	by := 1
	if b := strings.TrimSpace(cfg(n, "by", nc.bag)); b != "" {
		if v, err := strconv.Atoi(b); err == nil {
			by = v
		}
	}
	next := strconv.Itoa(base + by)
	nc.store.Set(nc.flowName, key, next, 0)
	into := cfg(n, "into", nc.bag)
	if into == "" {
		into = key
	}
	nc.bag[into] = next
	return "", nil
}

// evalCond compares two rendered operands by op. Unknown ops are false.
func evalCond(op, left, right string) bool {
	switch op {
	case "eq":
		return left == right
	case "ne":
		return left != right
	case "contains":
		return strings.Contains(left, right)
	case "matches":
		re, err := regexp.Compile(right)
		return err == nil && re.MatchString(left)
	case "gt", "lt":
		l, e1 := strconv.ParseFloat(strings.TrimSpace(left), 64)
		r, e2 := strconv.ParseFloat(strings.TrimSpace(right), 64)
		if e1 != nil || e2 != nil {
			return false
		}
		if op == "gt" {
			return l > r
		}
		return l < r
	default:
		return false
	}
}

// bagToken matches a {key} placeholder.
var bagToken = regexp.MustCompile(`\{([a-zA-Z0-9_]+)\}`)

// renderBag substitutes {key} placeholders with the bag's stringified values.
// An unknown key renders empty.
func renderBag(tmpl string, bag Bag) string {
	if tmpl == "" || !strings.ContainsRune(tmpl, '{') {
		return tmpl
	}
	return bagToken.ReplaceAllStringFunc(tmpl, func(m string) string {
		key := bagToken.FindStringSubmatch(m)[1]
		if _, ok := bag[key]; !ok {
			return ""
		}
		return bag.str(key)
	})
}
