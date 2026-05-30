package llm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenBudgetRecordAndTotals(t *testing.T) {
	b := NewTokenBudget()
	b.Record(100, 50)
	b.Record(200, 100)

	in, out := b.Totals()
	assert.Equal(t, 300, in)
	assert.Equal(t, 150, out)
}

func TestTokenBudgetPrunesOldEntries(t *testing.T) {
	b := NewTokenBudget()
	b.window = time.Second

	old := time.Now().Add(-2 * time.Second)
	b.now = func() time.Time { return old }
	b.Record(1000, 500)

	b.now = time.Now
	b.Record(10, 5)

	in, out := b.Totals()
	assert.Equal(t, 10, in)
	assert.Equal(t, 5, out)
}

func TestTokenBudgetAllowRespectsInputCap(t *testing.T) {
	b := NewTokenBudget()
	b.Record(90, 10)

	assert.True(t, b.Allow(100, 0))
	b.Record(10, 0)
	assert.False(t, b.Allow(100, 0))
}

func TestTokenBudgetAllowRespectsOutputCap(t *testing.T) {
	b := NewTokenBudget()
	b.Record(10, 90)

	assert.True(t, b.Allow(0, 100))
	b.Record(0, 10)
	assert.False(t, b.Allow(0, 100))
}

func TestTokenBudgetAllowZeroCapMeansUnrestricted(t *testing.T) {
	b := NewTokenBudget()
	b.Record(999999, 999999)
	assert.True(t, b.Allow(0, 0))
}

func TestTokenBudgetSeedCountsTowardCap(t *testing.T) {
	b := NewTokenBudget()
	b.Seed(90, 8)

	// Seed alone is under the caps.
	assert.True(t, b.Allow(100, 0))
	// Fresh consumption stacks on top of the seed.
	b.Record(10, 0)
	assert.False(t, b.Allow(100, 0), "seed + recorded usage must exhaust the cap")

	in, out := b.Totals()
	assert.Equal(t, 100, in)
	assert.Equal(t, 8, out)
}

func TestTokenBudgetSeedZeroIsNoop(t *testing.T) {
	b := NewTokenBudget()
	b.Seed(0, 0)
	b.Seed(-5, -1)

	in, out := b.Totals()
	assert.Equal(t, 0, in)
	assert.Equal(t, 0, out)
}

func TestTokenBudgetRollingWindow(t *testing.T) {
	b := NewTokenBudget()
	b.window = 100 * time.Millisecond

	b.Record(100, 50)
	assert.False(t, b.Allow(100, 0))

	time.Sleep(150 * time.Millisecond)
	assert.True(t, b.Allow(100, 0), "old entry should have expired")
}
