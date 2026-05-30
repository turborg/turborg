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
//
// Enforcement is the sum of two parts:
//
//   - locally recorded entries — what THIS process has consumed since it
//     started (exact, in-memory).
//   - a baseline — consumption attributed to the rest of the account for the
//     same window (other agents, and this agent's own pre-restart usage),
//     supplied by an external authority and refreshed live via SetBaseline.
//
// Splitting the two is what makes the cap hold per account/window rather than
// per process: a fresh process starts with locals at zero but inherits the
// account baseline, so it can't reset the window by restarting or being
// recreated. The baseline is a replaceable snapshot (not pruned) — the
// authority recomputes it over the rolling window on each refresh.
type TokenBudget struct {
	mu             sync.Mutex
	entries        []tokenEntry
	baselineInput  int
	baselineOutput int
	window         time.Duration
	now            func() time.Time // injectable clock for tests
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

// SetBaseline replaces the account baseline — consumption attributed to the
// rest of the account (other agents + this agent's pre-restart usage) over the
// rolling window. The external authority recomputes it on each refresh; the
// caller arranges for the baseline to EXCLUDE what this process counts locally
// so the two halves never double-count. Negative values are clamped to zero.
//
// Called once at startup (the seed) and then periodically as the authority
// reports fresh account totals. Cheap and safe to call on every refresh tick.
func (b *TokenBudget) SetBaseline(input, output int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baselineInput = max(0, input)
	b.baselineOutput = max(0, output)
}

// Totals returns total input and output tokens for the window: the account
// baseline plus everything this process has recorded locally.
func (b *TokenBudget) Totals() (input, output int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune()
	input, output = b.baselineInput, b.baselineOutput
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
