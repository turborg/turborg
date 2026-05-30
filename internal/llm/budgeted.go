package llm

import (
	"context"
	"iter"
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

func (p *BudgetedProvider) Stream(ctx context.Context, prompt string, opts ...CallOption) iter.Seq2[string, error] {
	return p.inner.Stream(ctx, prompt, opts...)
}

// Budget returns the underlying TokenBudget so callers (e.g. the
// gateway's /tb usage handler) can read live totals.
func (p *BudgetedProvider) Budget() *TokenBudget { return p.budget }

// Caps returns the configured per-day caps.
func (p *BudgetedProvider) Caps() (inputCap, outputCap int) { return p.inputCap, p.outputCap }
