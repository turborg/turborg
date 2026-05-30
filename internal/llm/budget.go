package llm

import (
	"sync"
	"time"
)

const defaultWindow = 24 * time.Hour

type tokenEntry struct {
	ts     time.Time
	input  int
	output int
}

// TokenBudget tracks LLM token consumption in a rolling time window.
// Goroutine-safe; entries older than the window are pruned on every
// mutating call.
type TokenBudget struct {
	mu      sync.Mutex
	entries []tokenEntry
	window  time.Duration
	now     func() time.Time // injectable clock for tests
}

func NewTokenBudget() *TokenBudget {
	return &TokenBudget{
		window: defaultWindow,
		now:    time.Now,
	}
}

// Record appends a token-consumption entry at the current time.
func (b *TokenBudget) Record(input, output int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune()
	b.entries = append(b.entries, tokenEntry{
		ts:     b.now(),
		input:  input,
		output: output,
	})
}

// Seed pre-loads consumption that happened before this budget existed — the
// totals an external authority (the control plane) reports for the rolling
// window across the whole account, including sibling and previously-destroyed
// agents. Without it, the window resets to zero every time an agent restarts
// or is recreated, so the cap would be enforced per agent-instance rather than
// per account/window: an operator could reset the budget at will by recreating
// the agent. Seeding closes that.
//
// The seed is anchored at the current time because the per-entry ages of the
// reported total aren't carried across, so it influences the budget for a full
// window from process start. That is deliberately conservative — it never
// under-counts prior usage. A zero/negative seed is a no-op.
func (b *TokenBudget) Seed(input, output int) {
	if input <= 0 && output <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, tokenEntry{
		ts:     b.now(),
		input:  max(0, input),
		output: max(0, output),
	})
}

// Totals returns the sum of input and output tokens consumed within
// the rolling window.
func (b *TokenBudget) Totals() (input, output int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune()
	for _, e := range b.entries {
		input += e.input
		output += e.output
	}
	return input, output
}

// Allow reports whether both input and output consumption are within
// the given caps. A cap of 0 means unrestricted for that dimension.
func (b *TokenBudget) Allow(inputCap, outputCap int) bool {
	input, output := b.Totals()
	if inputCap > 0 && input >= inputCap {
		return false
	}
	if outputCap > 0 && output >= outputCap {
		return false
	}
	return true
}

func (b *TokenBudget) prune() {
	cutoff := b.now().Add(-b.window)
	n := 0
	for _, e := range b.entries {
		if !e.ts.Before(cutoff) {
			b.entries[n] = e
			n++
		}
	}
	b.entries = b.entries[:n]
}
