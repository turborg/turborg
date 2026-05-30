package anthropic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/anthropic"
)

// sseStream writes a hand-crafted SSE response that emits message_start
// + N text deltas + message_stop. Mirrors the wire format the Anthropic
// streaming API produces and the SDK's stream decoder consumes.
func sseStream(w http.ResponseWriter, deltas []string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)

	send := func(event, data string) {
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		if fl != nil {
			fl.Flush()
		}
	}

	send("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
	send("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	for _, d := range deltas {
		payload := fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%q}}`, d)
		send("content_block_delta", payload)
	}
	send("content_block_stop", `{"type":"content_block_stop","index":0}`)
	send("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":7}}`)
	send("message_stop", `{"type":"message_stop"}`)
}

// captureRequest reads the inbound JSON body and parses it into a generic
// map so tests can assert on field shape without depending on the SDK
// param structs.
func captureRequest(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

// --- Construction ----------------------------------------------------------

func TestNewRejectsEmptyAPIKey(t *testing.T) {
	_, err := anthropic.New(anthropic.Settings{})
	require.Error(t, err)
}

func TestNewAppliesDefaults(t *testing.T) {
	p, err := anthropic.New(anthropic.Settings{APIKey: "test"})
	require.NoError(t, err)
	assert.Equal(t, anthropic.DefaultModel, p.Model())
}

func TestNewRespectsOverrides(t *testing.T) {
	p, err := anthropic.New(anthropic.Settings{
		APIKey: "test",
		Model:  "claude-opus-4-7",
	})
	require.NoError(t, err)
	assert.Equal(t, "claude-opus-4-7", p.Model())
}

// --- Ask path -------------------------------------------------------------

func TestAskAccumulatesStreamedDeltas(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sseStream(w, []string{"hello ", "world", "."})
	}))
	defer srv.Close()

	p, err := anthropic.New(anthropic.Settings{
		APIKey:  "test",
		BaseURL: srv.URL,
	})
	require.NoError(t, err)

	got, _, err := p.Ask(context.Background(), "say hi")
	require.NoError(t, err)
	assert.Equal(t, "hello world.", got)
}

func TestAskWrapsServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"oops"}}`))
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})
	_, _, err := p.Ask(context.Background(), "x")
	require.Error(t, err)
}

// --- Stream path ----------------------------------------------------------

func TestStreamYieldsDeltasInOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sseStream(w, []string{"a", "b", "c"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})

	var chunks []string
	for chunk, err := range p.Stream(context.Background(), "hi") {
		require.NoError(t, err)
		chunks = append(chunks, chunk)
	}
	assert.Equal(t, []string{"a", "b", "c"}, chunks)
}

func TestStreamEarlyTerminationStopsIteration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sseStream(w, []string{"a", "b", "c", "d", "e"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})

	var got []string
	for chunk, err := range p.Stream(context.Background(), "hi") {
		require.NoError(t, err)
		got = append(got, chunk)
		if len(got) == 2 {
			break
		}
	}
	assert.Equal(t, []string{"a", "b"}, got, "breaking out must stop iteration after the chunk we accepted")
}

// --- Request shape (prompt caching) ---------------------------------------

func TestAskSendsSystemBlockWithCacheControl(t *testing.T) {
	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = captureRequest(t, r)
		sseStream(w, []string{"ok"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{
		APIKey:            "test",
		BaseURL:           srv.URL,
		SystemPrompt:      "you are a helpful bot",
		CacheSystemPrompt: true,
	})
	_, _, err := p.Ask(context.Background(), "say hi")
	require.NoError(t, err)
	require.NotNil(t, captured)

	system, ok := captured["system"].([]any)
	require.True(t, ok, "expected system as []any, got %T", captured["system"])
	require.Len(t, system, 1)
	block := system[0].(map[string]any)
	assert.Equal(t, "you are a helpful bot", block["text"])
	cc, ok := block["cache_control"].(map[string]any)
	require.True(t, ok, "cache_control must be set on the system block")
	assert.Equal(t, "ephemeral", cc["type"])
}

func TestAskOmitsCacheControlWhenDisabled(t *testing.T) {
	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = captureRequest(t, r)
		sseStream(w, []string{"ok"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{
		APIKey:            "test",
		BaseURL:           srv.URL,
		SystemPrompt:      "short",
		CacheSystemPrompt: false,
	})
	_, _, err := p.Ask(context.Background(), "x")
	require.NoError(t, err)

	system := captured["system"].([]any)
	block := system[0].(map[string]any)
	_, hasCache := block["cache_control"]
	assert.False(t, hasCache, "cache_control must be absent when caching is disabled")
}

// --- Per-call options ----------------------------------------------------

func TestAskRespectsPerCallOverrides(t *testing.T) {
	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = captureRequest(t, r)
		sseStream(w, []string{"ok"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{
		APIKey:    "test",
		BaseURL:   srv.URL,
		Model:     "claude-sonnet-4-6",
		MaxTokens: 100,
	})
	_, _, err := p.Ask(context.Background(), "x",
		llm.WithSystem("override"),
		llm.WithMaxTokens(50),
		llm.WithModel("claude-opus-4-7"),
	)
	require.NoError(t, err)

	assert.Equal(t, "claude-opus-4-7", captured["model"])
	assert.Equal(t, float64(50), captured["max_tokens"])
	system := captured["system"].([]any)
	assert.Equal(t, "override", system[0].(map[string]any)["text"])
}

func TestAskOmitsSystemWhenEmpty(t *testing.T) {
	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = captureRequest(t, r)
		sseStream(w, []string{"ok"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})
	_, _, err := p.Ask(context.Background(), "x")
	require.NoError(t, err)
	_, ok := captured["system"]
	assert.False(t, ok, "system field must be omitted when no system prompt is configured")
}

// --- Authorization header -----------------------------------------------

func TestAskSendsAPIKeyHeader(t *testing.T) {
	var key string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key = r.Header.Get("x-api-key")
		sseStream(w, []string{"ok"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "sk-live-abc123", BaseURL: srv.URL})
	_, _, err := p.Ask(context.Background(), "x")
	require.NoError(t, err)
	assert.Equal(t, "sk-live-abc123", key, "SDK must propagate APIKey via x-api-key header")
}

// --- Streaming error path -----------------------------------------------

func TestStreamYieldsErrorOnServerFault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request","message":"bad"}}`))
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})
	var sawError bool
	for _, err := range p.Stream(context.Background(), "x") {
		if err != nil {
			sawError = true
			assert.Contains(t, strings.ToLower(err.Error()), "anthropic")
			break
		}
	}
	assert.True(t, sawError, "stream must surface server errors via the (chunk, err) tuple")
}

// --- SystemBlocks helper --------------------------------------------------

func TestSystemBlocksWithCache(t *testing.T) {
	blocks := anthropic.SystemBlocks("hello", true)
	require.Len(t, blocks, 1)
	assert.Equal(t, "hello", blocks[0].Text)
	assert.Equal(t, "ephemeral", string(blocks[0].CacheControl.Type))
}

func TestSystemBlocksWithoutCache(t *testing.T) {
	blocks := anthropic.SystemBlocks("hello", false)
	require.Len(t, blocks, 1)
	assert.Equal(t, "hello", blocks[0].Text)
	assert.Empty(t, string(blocks[0].CacheControl.Type),
		"caching disabled should leave the cache_control type unset")
}

// --- Stream non-text deltas skipped -------------------------------------

func TestStreamSkipsNonTextDeltas(t *testing.T) {
	// Some SDK events arrive that aren't content_block_delta with
	// text_delta — e.g. tool_use_delta, ping. The Stream path filters
	// these and only emits text chunks.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		send := func(event, data string) {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
			if fl != nil {
				fl.Flush()
			}
		}
		send("message_start", `{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		send("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		// An empty text delta — filtered out by the chunk == "" check.
		send("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":""}}`)
		// One real chunk through to confirm the iterator still runs.
		send("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`)
		send("content_block_stop", `{"type":"content_block_stop","index":0}`)
		send("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}`)
		send("message_stop", `{"type":"message_stop"}`)
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})
	var got []string
	for chunk, err := range p.Stream(context.Background(), "hi") {
		require.NoError(t, err)
		got = append(got, chunk)
	}
	assert.Equal(t, []string{"ok"}, got,
		"empty text deltas must be filtered out by the Stream chunk == \"\" check")
}

// --- Ask returns empty string when message has no text content ---------

func TestAskReturnsEmptyForBlankResponse(t *testing.T) {
	// Streams a message with NO content_block_delta — joinText sees no
	// "text"-type blocks and returns "".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		send := func(event, data string) {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
			if fl != nil {
				fl.Flush()
			}
		}
		send("message_start", `{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		send("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":0}}`)
		send("message_stop", `{"type":"message_stop"}`)
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{APIKey: "test", BaseURL: srv.URL})
	got, _, err := p.Ask(context.Background(), "x")
	require.NoError(t, err)
	assert.Equal(t, "", got, "joinText must return empty when the message has no text blocks")
}

// --- Settings.MaxTokens defaults when zero --------------------------------

func TestNewDefaultsMaxTokensOnZero(t *testing.T) {
	// MaxTokens=0 must take DefaultMaxTokens. Verified indirectly via
	// the request body of an Ask call.
	var captured map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = captureRequest(t, r)
		sseStream(w, []string{"ok"})
	}))
	defer srv.Close()

	p, _ := anthropic.New(anthropic.Settings{
		APIKey:    "test",
		BaseURL:   srv.URL,
		MaxTokens: 0,
	})
	_, _, err := p.Ask(context.Background(), "x")
	require.NoError(t, err)
	assert.Equal(t, float64(anthropic.DefaultMaxTokens), captured["max_tokens"],
		"MaxTokens=0 must fall back to DefaultMaxTokens")
}
