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
	return func(yield func(string, error) bool) {
		if f.err != nil {
			yield("", f.err)
			return
		}
		yield(f.resp, nil)
	}
}

func TestBudgetedProviderStreamRejectsWhenExhausted(t *testing.T) {
	inner := &fakeProvider{resp: "streamed reply"}
	budget := NewTokenBudget()
	budget.SetBaseline(1000, 1000) // already over the caps
	bp := NewBudgetedProvider(inner, budget, 100, 100, nil)

	var got string
	var gotErr error
	for text, err := range bp.Stream(context.Background(), "hi") {
		got, gotErr = text, err
	}
	assert.ErrorIs(t, gotErr, ErrBudgetExhausted)
	assert.Empty(t, got)
}

func TestBudgetedProviderStreamPropagatesInnerError(t *testing.T) {
	inner := &fakeProvider{err: assert.AnError} // budget allows; inner errors mid-stream
	bp := NewBudgetedProvider(inner, NewTokenBudget(), 1000, 1000, nil)

	var gotErr error
	for _, err := range bp.Stream(context.Background(), "hi") {
		gotErr = err
	}
	assert.ErrorIs(t, gotErr, assert.AnError)
}

func TestBudgetedProviderStreamStopsWhenConsumerBreaks(t *testing.T) {
	inner := &fakeProvider{resp: "partial reply"}
	budget := NewTokenBudget()
	bp := NewBudgetedProvider(inner, budget, 1000, 1000, nil)

	for _, err := range bp.Stream(context.Background(), "hi") {
		require.NoError(t, err)
		break // consumer stops early; producer must still record what streamed.
	}
	in, _ := budget.Totals()
	assert.Positive(t, in)
}

func TestBudgetedProviderStreamEmptyReply(t *testing.T) {
	inner := &fakeProvider{resp: ""} // exercises the zero-token estimate branch
	budget := NewTokenBudget()
	bp := NewBudgetedProvider(inner, budget, 1000, 1000, nil)

	for _, err := range bp.Stream(context.Background(), "p") {
		require.NoError(t, err)
	}
	_, out := budget.Totals()
	assert.Zero(t, out)
}

func TestBudgetedProviderStreamRecordsEstimatedUsage(t *testing.T) {
	inner := &fakeProvider{resp: "this is a streamed reply"}
	budget := NewTokenBudget()
	var reported Usage
	bp := NewBudgetedProvider(inner, budget, 1000, 1000, func(u Usage) { reported = u })

	var assembled string
	for text, err := range bp.Stream(context.Background(), "a prompt") {
		require.NoError(t, err)
		assembled += text
	}
	assert.Equal(t, "this is a streamed reply", assembled)
	// Estimated (~4 chars/token) usage was recorded + reported, so a streamed
	// call can't slip past the budget as free.
	in, out := budget.Totals()
	assert.Positive(t, in)
	assert.Positive(t, out)
	assert.Positive(t, reported.OutputTokens)
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
