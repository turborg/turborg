// Package commands turns data-driven command definitions into the
// handlers + guards the agent's CommandRegistry dispatches. A definition
// is a trigger name, a type (static or LLM-backed), a template, an access
// policy, and — for LLM commands — an optional model override.
//
// The package is transport-agnostic: definitions arrive as JSON from the
// tenant feed (pooled) or a TURBORG_COMMANDS env var (dedicated), are
// decoded into Definition values, and built into agent.DynamicCommand
// batches that the runtime swaps into a live registry via ReplaceDynamic.
// Per-command guards are injected by the runtime (which owns owner-trust
// config), so this package never imports the runtime — avoiding a cycle.
package commands

import (
	"context"
	"log/slog"
	"strings"

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
// shared by the pooled feed and the dedicated spawn payload.
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

// Build turns definitions into a ReplaceDynamic batch. LLM commands are
// skipped (with a log line) when no provider is configured; commands with
// an unknown type are skipped too. Order is preserved.
func Build(defs []Definition, provider llm.Provider, guardFor GuardFactory, log *slog.Logger) []agent.DynamicCommand {
	if log == nil {
		log = slog.Default()
	}
	out := make([]agent.DynamicCommand, 0, len(defs))
	for _, d := range defs {
		var handler agent.CommandHandler
		switch d.Type {
		case TypeStatic:
			handler = staticHandler(d)
		case TypeLLM:
			if provider == nil {
				log.Warn("skipping llm command: no LLM provider configured", "command", d.Name)
				continue
			}
			handler = llmHandler(d, provider, log)
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

func staticHandler(d Definition) agent.CommandHandler {
	tmpl := d.Template
	return func(_ context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, render(tmpl, env, args)), nil
	}
}

func llmHandler(d Definition, provider llm.Provider, log *slog.Logger) agent.CommandHandler {
	tmpl := d.Template
	model := d.Model
	name := d.Name
	// The command's own instructions become the system prompt; fall back to
	// the default IRC-shaping prompt when none were supplied.
	system := strings.TrimSpace(d.Instructions)
	if system == "" {
		system = llmSystemPrompt
	}
	return func(ctx context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		prompt := render(tmpl, env, args)
		opts := []llm.CallOption{llm.WithSystem(system), llm.WithMaxTokens(llmMaxTokens)}
		if model != "" {
			opts = append(opts, llm.WithModel(model))
		}
		answer, err := provider.Ask(ctx, prompt, opts...)
		if err != nil {
			log.Warn("llm command failed", "command", name, "err", err)
			return agent.ReplyTo(env, "sorry, that broke: "+err.Error()), nil
		}
		return agent.ReplyTo(env, collapseWhitespace(answer)), nil
	}
}

// render substitutes the supported placeholders in a template. {args} is
// the whitespace-joined command arguments; {nick} the sender; {channel}
// the originating channel (or the sender's nick for a DM).
func render(tmpl string, env *agent.InboundEnvelope, args []string) string {
	return strings.NewReplacer(
		"{nick}", env.Sender,
		"{args}", strings.TrimSpace(strings.Join(args, " ")),
		"{channel}", env.Channel,
	).Replace(tmpl)
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
