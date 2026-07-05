package llm

import (
	"context"
	"iter"
	"strings"
)

// BudgetedProvider wraps a Provider with rolling-window token budget
// enforcement. Before each Ask call it checks the budget; after a
// successful call it records the consumption and fires the onUsage
// callback for external reporting (EventBus, structured logging).
type BudgetedProvider struct {
	inner     Provider
	budget    *TokenBudget
	inputCap  int
	outputCap int
	onUsage   func(Usage)
}

func NewBudgetedProvider(inner Provider, budget *TokenBudget, inputCap, outputCap int, onUsage func(Usage)) *BudgetedProvider {
	return &BudgetedProvider{
		inner:     inner,
		budget:    budget,
		inputCap:  inputCap,
		outputCap: outputCap,
		onUsage:   onUsage,
	}
}

func (p *BudgetedProvider) Model() string { return p.inner.Model() }

func (p *BudgetedProvider) Ask(ctx context.Context, prompt string, opts ...CallOption) (string, Usage, error) {
	if !p.budget.Allow(p.inputCap, p.outputCap) {
		return "", Usage{}, ErrBudgetExhausted
	}
	text, usage, err := p.inner.Ask(ctx, prompt, opts...)
	if err != nil {
		return "", usage, err
	}
	p.budget.Record(usage.InputTokens, usage.OutputTokens)
	if p.onUsage != nil {
		p.onUsage(usage)
	}
	return text, usage, nil
}

// Stream enforces the budget the same way Ask does, so streaming can't be used
// to bypass the daily cap. It refuses up front when the budget is spent
// (yielding ErrBudgetExhausted once), and records consumption after the stream
// ends. The streaming interface carries no Usage, so tokens are ESTIMATED from
// the prompt + accumulated output (~4 chars/token) — a soft budget tolerates the
// approximation, and it's far better than counting streamed replies as free.
func (p *BudgetedProvider) Stream(ctx context.Context, prompt string, opts ...CallOption) iter.Seq2[string, error] {
	if !p.budget.Allow(p.inputCap, p.outputCap) {
		return func(yield func(string, error) bool) {
			yield("", ErrBudgetExhausted)
		}
	}
	inner := p.inner.Stream(ctx, prompt, opts...)
	return func(yield func(string, error) bool) {
		var out strings.Builder
		for text, err := range inner {
			if err != nil {
				yield("", err)
				return
			}
			out.WriteString(text)
			if !yield(text, nil) {
				break // consumer stopped early; still record what streamed.
			}
		}
		usage := Usage{
			InputTokens:  estimateTokens(prompt),
			OutputTokens: estimateTokens(out.String()),
		}
		p.budget.Record(usage.InputTokens, usage.OutputTokens)
		if p.onUsage != nil {
			p.onUsage(usage)
		}
	}
}

// estimateTokens is a coarse token count (~4 chars/token) used to charge the
// budget for a streamed call, where the provider reports no exact Usage.
func estimateTokens(s string) int {
	if s == "" {
		return 0
	}
	return (len(s) + 3) / 4
}

// Budget returns the underlying TokenBudget so callers (e.g. the
// gateway's /tb usage handler) can read live totals.
func (p *BudgetedProvider) Budget() *TokenBudget { return p.budget }

// Caps returns the configured per-day caps.
func (p *BudgetedProvider) Caps() (inputCap, outputCap int) { return p.inputCap, p.outputCap }
