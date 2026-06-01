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

func TestTBTLDR(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "  page summary  "})
	h.fetch = func(_ context.Context, url string) (string, error) {
		assert.Equal(t, "http://example.com/a", url)
		return "page body", nil
	}

	got, err := h.tbTLDR(context.Background(), "u1", "http://example.com/a")
	require.NoError(t, err)
	assert.Equal(t, "page summary", got)
}

func TestTBTLDRNoProvider(t *testing.T) {
	h := newTBHandler(slog.Default())
	_, err := h.tbTLDR(context.Background(), "u1", "http://example.com")
	assert.ErrorContains(t, err, "no LLM provider")
}

func TestTBTLDREmptyURL(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	_, err := h.tbTLDR(context.Background(), "u1", "   ")
	assert.ErrorContains(t, err, "usage:")
}

func TestTBTLDRRejectsBadScheme(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	// fetch must never be reached for a non-http(s) scheme.
	h.fetch = func(context.Context, string) (string, error) {
		t.Fatal("fetch should not be called for a bad scheme")
		return "", nil
	}
	_, err := h.tbTLDR(context.Background(), "u1", "ftp://example.com/x")
	assert.ErrorContains(t, err, "http and https")
}

func TestTBTLDRBlockedAddressMessage(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	h.fetch = func(context.Context, string) (string, error) { return "", errBlockedAddress }
	_, err := h.tbTLDR(context.Background(), "u1", "http://internal.local")
	assert.ErrorContains(t, err, "private or local address")
}

func TestTBTLDRFetchErrorRelayed(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	h.fetch = func(context.Context, string) (string, error) {
		return "", fmt.Errorf("fetch returned HTTP 404")
	}
	_, err := h.tbTLDR(context.Background(), "u1", "http://example.com")
	assert.ErrorContains(t, err, "HTTP 404")
}

func TestTBTLDRLLMError(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{err: fmt.Errorf("api down")})
	h.fetch = func(context.Context, string) (string, error) { return "body", nil }
	_, err := h.tbTLDR(context.Background(), "u1", "http://example.com")
	assert.ErrorContains(t, err, "LLM request failed")
}

func TestTBTLDRBudgetExhausted(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{err: llm.ErrBudgetExhausted})
	h.fetch = func(context.Context, string) (string, error) { return "body", nil }
	_, err := h.tbTLDR(context.Background(), "u1", "http://example.com")
	assert.ErrorContains(t, err, "daily AI token budget")
}

func TestTBTLDRRateLimit(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	h.fetch = func(context.Context, string) (string, error) { return "body", nil }

	// The throttle allows tbTLDRMaxCallsPerHour, then refuses.
	for i := 0; i < tbTLDRMaxCallsPerHour; i++ {
		_, err := h.tbTLDR(context.Background(), "owner", "http://example.com")
		require.NoErrorf(t, err, "call %d should be allowed", i)
	}
	_, err := h.tbTLDR(context.Background(), "owner", "http://example.com")
	require.Error(t, err)
	assert.ErrorContains(t, err, "rate limit")
}

func TestTBTLDRRateLimitPerKey(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	h.fetch = func(context.Context, string) (string, error) { return "body", nil }

	for i := 0; i < tbTLDRMaxCallsPerHour; i++ {
		_, err := h.tbTLDR(context.Background(), "alice", "http://example.com")
		require.NoError(t, err)
	}
	// A different user key has its own bucket.
	_, err := h.tbTLDR(context.Background(), "bob", "http://example.com")
	require.NoError(t, err)
}

func TestTBTLDRBadSchemeDoesNotConsumeQuota(t *testing.T) {
	h := newTBHandler(slog.Default())
	h.setLLM(&fakeLLM{response: "ok"})
	h.fetch = func(context.Context, string) (string, error) { return "body", nil }

	// A malformed URL is rejected before the throttle, so it must not burn
	// any of the hourly budget.
	for i := 0; i < 50; i++ {
		_, err := h.tbTLDR(context.Background(), "owner", "not-a-url")
		require.Error(t, err)
	}
	got, err := h.tbTLDR(context.Background(), "owner", "http://example.com")
	require.NoError(t, err)
	assert.Equal(t, "ok", got)
}

func TestTBHandlerSetLLM(t *testing.T) {
	h := newTBHandler(nil)
	assert.Nil(t, h.currentLLM())

	p := &fakeLLM{response: "ok"}
	h.setLLM(p)
	assert.Equal(t, p, h.currentLLM())
}
