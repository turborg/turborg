package irc

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
)

type fakeLLM struct {
	response string
	err      error
}

func (f *fakeLLM) Model() string { return "fake" }
func (f *fakeLLM) Ask(_ context.Context, _ string, _ ...llm.CallOption) (string, llm.Usage, error) {
	return f.response, llm.Usage{}, f.err
}
func (f *fakeLLM) Stream(_ context.Context, _ string, _ ...llm.CallOption) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {}
}

func TestTBSummarize(t *testing.T) {
	store := messages.NewMemoryStore(500)
	ctx := context.Background()
	for i := range 5 {
		_ = store.Submit(ctx, messages.Message{
			Channel: "#test",
			Nick:    fmt.Sprintf("user%d", i),
			Text:    fmt.Sprintf("message %d", i),
			TS:      time.Now().Add(time.Duration(i) * time.Second),
		})
	}

	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "  summary of chat  "})

	got, err := h.tbSummarize(ctx, "#test", 0, 200, store)
	require.NoError(t, err)
	assert.Equal(t, "summary of chat", got)
}

func TestTBSummarizeDefaultLimit(t *testing.T) {
	store := messages.NewMemoryStore(500)
	ctx := context.Background()
	for i := range 10 {
		_ = store.Submit(ctx, messages.Message{
			Channel: "#test",
			Nick:    "u",
			Text:    fmt.Sprintf("msg %d", i),
			TS:      time.Now().Add(time.Duration(i) * time.Second),
		})
	}

	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})

	got, err := h.tbSummarize(ctx, "#test", 0, 200, store)
	require.NoError(t, err)
	assert.Equal(t, "ok", got)
}

func TestTBSummarizeCapsN(t *testing.T) {
	store := messages.NewMemoryStore(500)
	ctx := context.Background()
	for i := range 10 {
		_ = store.Submit(ctx, messages.Message{
			Channel: "#test",
			Nick:    "u",
			Text:    fmt.Sprintf("msg %d", i),
			TS:      time.Now().Add(time.Duration(i) * time.Second),
		})
	}

	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})

	// Request 1000 but cap is 200 — should still work (capped to 200).
	got, err := h.tbSummarize(ctx, "#test", 1000, 200, store)
	require.NoError(t, err)
	assert.Equal(t, "ok", got)
}

func TestTBSummarizeNoProvider(t *testing.T) {
	h := newTBHandler(slog.Default())
	_, err := h.tbSummarize(context.Background(), "#test", 0, 200, messages.NewMemoryStore(100))
	assert.ErrorContains(t, err, "no LLM provider")
}

func TestTBSummarizeNoStore(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	_, err := h.tbSummarize(context.Background(), "#test", 0, 200, nil)
	assert.ErrorContains(t, err, "no message history")
}

func TestTBSummarizeZeroCap(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	_, err := h.tbSummarize(context.Background(), "#test", 0, 0, messages.NewMemoryStore(100))
	assert.ErrorContains(t, err, "not available on your plan")
}

func TestTBSummarizeEmptyChannel(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	_, err := h.tbSummarize(context.Background(), "#empty", 0, 200, messages.NewMemoryStore(100))
	assert.ErrorContains(t, err, "no messages")
}

func TestTBSummarizeLLMError(t *testing.T) {
	store := messages.NewMemoryStore(500)
	_ = store.Submit(context.Background(), messages.Message{
		Channel: "#test", Nick: "u", Text: "hi", TS: time.Now(),
	})

	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{err: fmt.Errorf("api down")})

	_, err := h.tbSummarize(context.Background(), "#test", 0, 200, store)
	assert.ErrorContains(t, err, "LLM request failed")
}

func TestTBHandlerSetLLM(t *testing.T) {
	h := newTBHandler(nil)
	assert.Nil(t, h.currentLLM())

	p := &fakeLLM{response: "ok"}
	h.setLLM(p)
	assert.Equal(t, p, h.currentLLM())
}
