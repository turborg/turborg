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
