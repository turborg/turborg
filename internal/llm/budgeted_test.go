package llm

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeProvider struct {
	resp  string
	usage Usage
	err   error
}

func (f *fakeProvider) Model() string { return "fake" }
func (f *fakeProvider) Ask(_ context.Context, _ string, _ ...CallOption) (string, Usage, error) {
	return f.resp, f.usage, f.err
}
func (f *fakeProvider) Stream(_ context.Context, _ string, _ ...CallOption) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {}
}

func TestBudgetedProviderAllowsWithinBudget(t *testing.T) {
	inner := &fakeProvider{resp: "ok", usage: Usage{InputTokens: 10, OutputTokens: 5}}
	budget := NewTokenBudget()
	var reported Usage
	bp := NewBudgetedProvider(inner, budget, 100, 100, func(u Usage) { reported = u })

	got, usage, err := bp.Ask(context.Background(), "hi")
	require.NoError(t, err)
	assert.Equal(t, "ok", got)
	assert.Equal(t, 10, usage.InputTokens)
	assert.Equal(t, 5, usage.OutputTokens)
	assert.Equal(t, 10, reported.InputTokens)

	in, out := budget.Totals()
	assert.Equal(t, 10, in)
	assert.Equal(t, 5, out)
}

func TestBudgetedProviderRejectsWhenExhausted(t *testing.T) {
	inner := &fakeProvider{resp: "ok", usage: Usage{InputTokens: 10, OutputTokens: 5}}
	budget := NewTokenBudget()
	budget.Record(100, 0)
	bp := NewBudgetedProvider(inner, budget, 100, 100, nil)

	_, _, err := bp.Ask(context.Background(), "hi")
	require.ErrorIs(t, err, ErrBudgetExhausted)
}

func TestBudgetedProviderDoesNotRecordOnError(t *testing.T) {
	inner := &fakeProvider{err: assert.AnError}
	budget := NewTokenBudget()
	bp := NewBudgetedProvider(inner, budget, 100, 100, nil)

	_, _, err := bp.Ask(context.Background(), "hi")
	require.Error(t, err)

	in, out := budget.Totals()
	assert.Equal(t, 0, in)
	assert.Equal(t, 0, out)
}

func TestBudgetedProviderDelegatesModel(t *testing.T) {
	inner := &fakeProvider{resp: "ok"}
	bp := NewBudgetedProvider(inner, NewTokenBudget(), 0, 0, nil)
	assert.Equal(t, "fake", bp.Model())
}

func TestBudgetedProviderStreamDelegates(t *testing.T) {
	bp := NewBudgetedProvider(&fakeProvider{}, NewTokenBudget(), 0, 0, nil)
	seq := bp.Stream(context.Background(), "hi")
	assert.NotNil(t, seq)
}

func TestBudgetedProviderCapsAndBudgetAccessors(t *testing.T) {
	budget := NewTokenBudget()
	bp := NewBudgetedProvider(&fakeProvider{}, budget, 42, 21, nil)
	assert.Equal(t, budget, bp.Budget())
	inCap, outCap := bp.Caps()
	assert.Equal(t, 42, inCap)
	assert.Equal(t, 21, outCap)
}
