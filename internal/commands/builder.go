// Package commands turns data-driven command definitions into the
// handlers + guards the agent's CommandRegistry dispatches. A definition
// is a trigger name, a type (static or LLM-backed), a template, an access
// policy, and — for LLM commands — an optional model override.
//
// The package is transport-agnostic: definitions arrive as JSON from the
// tenant feed (pooled) or a TURBORG_COMMANDS env var (single-instance), are
// decoded into Definition values, and built into agent.DynamicCommand
// batches that the runtime swaps into a live registry via ReplaceDynamic.
// Per-command guards are injected by the runtime (which owns owner-trust
// config), so this package never imports the runtime — avoiding a cycle.
package commands

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
)

// Type is the command kind. Static commands render their template as the
// reply verbatim; LLM commands run the rendered template as a prompt.
type Type string

const (
	// TypeStatic renders the template (with placeholders substituted) as
	// the literal reply — a deterministic, zero-cost canned response.
	TypeStatic Type = "static"
	// TypeLLM runs the rendered template as a prompt against the
	// configured LLM provider and replies with the model's answer.
	TypeLLM Type = "llm"
)

// Access is the per-command trust policy deciding who may trigger it.
type Access string

const (
	// AccessEveryone lets any sender trigger the command (the registry-
	// wide ignore list + throttle still apply).
	AccessEveryone Access = "everyone"
	// AccessOwner restricts the command to the verified owner.
	AccessOwner Access = "owner"
	// AccessAllowlist allows the owner plus an explicit nick/account list.
	AccessAllowlist Access = "allowlist"
)

// Definition is one user-defined command. It is the byte-stable wire shape
// shared by the pooled feed and the single-instance spawn payload.
type Definition struct {
	Name     string `json:"name"`
	Type     Type   `json:"type"`
	Template string `json:"template"`
	// Instructions, when set, is the system prompt for an LLM command —
	// the persona / knowledge / behavior the model runs with. Empty falls
	// back to the default IRC-shaping prompt. Ignored for static commands.
	Instructions string   `json:"instructions,omitempty"`
	Model        string   `json:"model,omitempty"`
	Access       Access   `json:"access"`
	Allowlist    []string `json:"allowlist,omitempty"`
}

// llmSystemPrompt steers LLM-backed commands toward IRC-shaped replies.
// Kept as a const so the Anthropic provider's prompt-cache prefix stays
// stable across calls.
const llmSystemPrompt = "You are turborg, an IRC chatbot. Keep replies short and conversational — " +
	"most IRC clients show one line at a time. Avoid markdown."

// llmMaxTokens caps an LLM command's reply length — a handful of IRC
// lines after wrapping.
const llmMaxTokens = 512

// GuardFactory builds the per-command guard for a definition. The runtime
// supplies it (it owns owner-trust config); a nil factory or a nil return
// means "no per-command guard" (the registry-wide guard still runs).
type GuardFactory func(Definition) agent.CommandGuard

// renderCtx carries the per-instance template values that stay constant across
// a connector's lifetime: the platform label (the IRC server hostname today,
// later a Discord server / Slack workspace / "Web") and the agent owner's nick.
// Per-message values — sender, room, args, the UTC clock — come from the
// envelope at render time, not from here.
type renderCtx struct {
	platform string
	owner    string
}

// Build turns definitions into a ReplaceDynamic batch. LLM commands are
// skipped (with a log line) when no provider is configured; commands with
// an unknown type are skipped too. Order is preserved. platform/owner seed the
// connector-agnostic {platform}/{owner} template placeholders.
func Build(defs []Definition, provider llm.Provider, guardFor GuardFactory, platform, owner string, log *slog.Logger) []agent.DynamicCommand {
	if log == nil {
		log = slog.Default()
	}
	ctx := renderCtx{platform: platform, owner: owner}
	out := make([]agent.DynamicCommand, 0, len(defs))
	for _, d := range defs {
		var handler agent.CommandHandler
		switch d.Type {
		case TypeStatic:
			handler = staticHandler(d, ctx)
		case TypeLLM:
			if provider == nil {
				log.Warn("skipping llm command: no LLM provider configured", "command", d.Name)
				continue
			}
			handler = llmHandler(d, provider, ctx, log)
		default:
			log.Warn("skipping command with unknown type", "command", d.Name, "type", d.Type)
			continue
		}
		var guard agent.CommandGuard
		if guardFor != nil {
			guard = guardFor(d)
		}
		out = append(out, agent.DynamicCommand{Name: d.Name, Handler: handler, Guard: guard})
	}
	return out
}

func staticHandler(d Definition, ctx renderCtx) agent.CommandHandler {
	tmpl := d.Template
	return func(_ context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, render(tmpl, env, args, ctx)), nil
	}
}

func llmHandler(d Definition, provider llm.Provider, ctx renderCtx, log *slog.Logger) agent.CommandHandler {
	tmpl := d.Template
	model := d.Model
	name := d.Name
	// The command's own instructions become the system prompt; fall back to
	// the default IRC-shaping prompt when none were supplied.
	system := strings.TrimSpace(d.Instructions)
	if system == "" {
		system = llmSystemPrompt
	}
	return func(reqCtx context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		prompt := render(tmpl, env, args, ctx)
		opts := []llm.CallOption{llm.WithSystem(system), llm.WithMaxTokens(llmMaxTokens)}
		if model != "" {
			opts = append(opts, llm.WithModel(model))
		}
		answer, _, err := provider.Ask(reqCtx, prompt, opts...)
		if err != nil {
			if errors.Is(err, llm.ErrBudgetExhausted) {
				return agent.ReplyTo(env, "Daily AI token budget spent. Resets on a rolling 24h window."), nil
			}
			log.Warn("llm command failed", "command", name, "err", err)
			return agent.ReplyTo(env, "sorry, that broke: "+err.Error()), nil
		}
		return agent.ReplyTo(env, collapseWhitespace(answer)), nil
	}
}

// Helper-placeholder patterns. Each captures the comma-separated argument list
// (or the bound, for random) between the colon and the closing brace. `[^}]*`
// keeps them from spanning across a `}`.
var (
	choiceRe  = regexp.MustCompile(`\{choice:([^}]*)\}`)
	randomRe  = regexp.MustCompile(`\{random:([^}]*)\}`)
	shuffleRe = regexp.MustCompile(`\{shuffle:([^}]*)\}`)
)

// render substitutes the supported placeholders in a template.
//
// Connector-agnostic primary tokens: {user} (the sender), {args} (the
// whitespace-joined arguments), {room} (the originating channel, or the
// sender's nick for a DM), {platform} (the transport label — IRC server
// hostname today), {owner} (the agent owner's nick), and {date}/{time}/
// {datetime} (the UTC clock at reply time). The IRC-specific {nick}/{channel}/
// {network} names are retained as aliases of {user}/{room}/{platform} so
// existing skills keep working.
//
// The dynamic helpers — {choice:a,b,c}, {random:N}, {shuffle:a,b,c} — are
// expanded first, over the author-supplied template only, so a user-supplied
// {args} value substituted afterwards can never inject a helper.
func render(tmpl string, env *agent.InboundEnvelope, args []string, ctx renderCtx) string {
	tmpl = expandHelpers(tmpl)

	now := time.Now().UTC()
	return strings.NewReplacer(
		"{user}", env.Sender,
		"{nick}", env.Sender,
		"{args}", strings.TrimSpace(strings.Join(args, " ")),
		"{room}", env.Channel,
		"{channel}", env.Channel,
		"{platform}", ctx.platform,
		"{network}", ctx.platform,
		"{owner}", ctx.owner,
		"{date}", now.Format("2006-01-02"),
		"{time}", now.Format("15:04:05"),
		"{datetime}", now.Format("2006-01-02 15:04:05 UTC"),
	).Replace(tmpl)
}

// expandHelpers evaluates the random dynamic-placeholder helpers in a template.
// It runs before the simple substitution so only the author's template — never
// a user-supplied {args} value — can trigger one. A {random:N} with a missing
// or non-positive bound is left literal so a typo is visible rather than silent.
func expandHelpers(tmpl string) string {
	tmpl = choiceRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		opts := splitOptions(choiceRe.FindStringSubmatch(m)[1])
		if len(opts) == 0 {
			return ""
		}
		return opts[rand.Intn(len(opts))]
	})
	tmpl = shuffleRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		opts := splitOptions(shuffleRe.FindStringSubmatch(m)[1])
		rand.Shuffle(len(opts), func(i, j int) { opts[i], opts[j] = opts[j], opts[i] })
		return strings.Join(opts, ",")
	})
	tmpl = randomRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		n, err := strconv.Atoi(strings.TrimSpace(randomRe.FindStringSubmatch(m)[1]))
		if err != nil || n < 1 {
			return m
		}
		return strconv.Itoa(rand.Intn(n) + 1)
	})
	return tmpl
}

// splitOptions splits a comma-separated helper argument into trimmed, non-empty
// options.
func splitOptions(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
