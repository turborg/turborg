// Package llm defines the provider abstraction every LLM backend
// implements. Concrete providers live under internal/llm/<name>. The
// agent and handlers talk to providers only through this interface so
// the backend can be swapped (Anthropic default, OpenAI, local models)
// without touching the rest of the codebase.
package llm

import (
	"context"
	"iter"
)

// Provider is the minimal surface every LLM backend exposes.
//
// Ask returns the assembled text response. Stream yields text deltas
// as iter.Seq2[string, error] — the second value carries any error
// encountered mid-stream; once non-nil, iteration ends.
//
// Both methods accept per-call CallOptions to override defaults (max
// tokens, system prompt, model) without rebuilding the provider.
type Provider interface {
	Model() string
	Ask(ctx context.Context, prompt string, opts ...CallOption) (string, error)
	Stream(ctx context.Context, prompt string, opts ...CallOption) iter.Seq2[string, error]
}

// CallOptions are populated by CallOption functions for a single Ask or
// Stream invocation. Zero values mean "use the provider's default".
type CallOptions struct {
	System    string
	MaxTokens int
	Model     string
}

// CallOption configures a single Ask / Stream call.
type CallOption func(*CallOptions)

// WithSystem overrides the provider's default system prompt for this
// call.
func WithSystem(prompt string) CallOption {
	return func(o *CallOptions) { o.System = prompt }
}

// WithMaxTokens overrides the provider's default max-tokens budget.
func WithMaxTokens(n int) CallOption {
	return func(o *CallOptions) { o.MaxTokens = n }
}

// WithModel overrides the model for a single call (e.g. switch to a
// reasoning model for a hard question).
func WithModel(model string) CallOption {
	return func(o *CallOptions) { o.Model = model }
}

// ApplyOptions evaluates the given options against an empty
// CallOptions. Concrete providers call this to resolve a per-call
// override-or-default for each field.
func ApplyOptions(opts []CallOption) CallOptions {
	var co CallOptions
	for _, o := range opts {
		o(&co)
	}
	return co
}
