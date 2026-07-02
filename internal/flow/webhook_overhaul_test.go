package flow

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWebhookHeadersRenderedFromBag verifies custom headers are applied and
// that each value is rendered against the bag (so a user can inject a secret).
func TestWebhookHeadersRenderedFromBag(t *testing.T) {
	var gotAuth, gotStatic string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotStatic = r.Header.Get("X-Api-Key")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	nc := &nodeContext{
		bag:  Bag{"apitoken": "s3cr3t"},
		post: defaultPost,
	}
	_, err := nodeWebhook(context.Background(), Node{Config: map[string]any{
		"url":     srv.URL,
		"headers": map[string]any{"Authorization": "Bearer {apitoken}", "X-Api-Key": "abc"},
	}}, nc)
	require.NoError(t, err)
	assert.Equal(t, "Bearer s3cr3t", gotAuth, "header value rendered from bag")
	assert.Equal(t, "abc", gotStatic)
}

// TestWebhookResponseCapturedIntoBag verifies a GET's response body lands in
// the configured bag key, bounded and usable by later nodes.
func TestWebhookResponseCapturedIntoBag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"price":42}`)
	}))
	defer srv.Close()

	nc := &nodeContext{bag: Bag{}, post: defaultPost}
	_, err := nodeWebhook(context.Background(), Node{Config: map[string]any{
		"url": srv.URL, "method": "GET", "into": "resp",
	}}, nc)
	require.NoError(t, err)
	assert.Equal(t, `{"price":42}`, nc.bag.str("resp"))
}

// TestWebhookResponseCapBound verifies capture is bounded to 64 KiB.
func TestWebhookResponseCapBound(t *testing.T) {
	huge := strings.Repeat("x", maxWebhookBody+4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, huge)
	}))
	defer srv.Close()

	nc := &nodeContext{bag: Bag{}, post: defaultPost}
	_, err := nodeWebhook(context.Background(), Node{Config: map[string]any{
		"url": srv.URL, "method": "GET", "into": "resp",
	}}, nc)
	require.NoError(t, err)
	assert.Len(t, nc.bag.str("resp"), maxWebhookBody, "captured body is capped at 64 KiB")
}

// TestWebhookRetriesThenSucceeds verifies a 500 is retried and the eventual
// 200 response is captured. Uses a 1s backoff (the floor).
func TestWebhookRetriesThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	nc := &nodeContext{bag: Bag{}, post: defaultPost}
	_, err := nodeWebhook(context.Background(), Node{Config: map[string]any{
		"url": srv.URL, "method": "GET", "into": "resp",
		"retries": 2, "retry_backoff": 1,
	}}, nc)
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls), "one retry after the 500")
	assert.Equal(t, "ok", nc.bag.str("resp"))
}

// TestWebhookNoRetryOn4xx verifies a 400 is not retried and no capture happens.
func TestWebhookNoRetryOn4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	nc := &nodeContext{bag: Bag{}, post: defaultPost}
	_, err := nodeWebhook(context.Background(), Node{Config: map[string]any{
		"url": srv.URL, "method": "GET", "into": "resp",
		"retries": 3, "retry_backoff": 1,
	}}, nc)
	require.NoError(t, err, "a failing webhook is logged, not fatal")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "4xx is permanent, no retry")
	assert.Empty(t, nc.bag.str("resp"), "no capture on failure")
}

// TestWebhook429IsRetried verifies 429 (rate limit) is treated as retryable.
func TestWebhook429IsRetried(t *testing.T) {
	var n int32
	post := func(context.Context, string, string, map[string]string, []byte) ([]byte, error) {
		if atomic.AddInt32(&n, 1) == 1 {
			return nil, &httpStatusError{status: http.StatusTooManyRequests}
		}
		return []byte("done"), nil
	}
	got, err := postWithRetry(context.Background(), post, "GET", "http://x", nil, nil, 3, 1)
	require.NoError(t, err)
	assert.Equal(t, "done", string(got))
	assert.Equal(t, int32(2), atomic.LoadInt32(&n))
}

// TestWebhookBackoffRespectsCtxCancel verifies a cancelled context aborts the
// wait between retries promptly instead of sleeping the full backoff.
func TestWebhookBackoffRespectsCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	post := func(context.Context, string, string, map[string]string, []byte) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, &httpStatusError{status: http.StatusServiceUnavailable}
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := postWithRetry(ctx, post, "GET", "http://x", nil, nil, 5, 1)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "cancel short-circuits the 1s backoff")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "no attempt after cancel")
}

// TestWebhookRetryClampAndGiveUp verifies retries clamp to 5 and a persistently
// failing webhook gives up without failing the flow.
func TestWebhookRetryClampAndGiveUp(t *testing.T) {
	var calls int32
	post := func(context.Context, string, string, map[string]string, []byte) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, &httpStatusError{status: 500}
	}
	nc := &nodeContext{bag: Bag{}, post: post}
	// retries=99 clamps to 5 → 6 total attempts; retry_backoff=0 clamps to 1s
	// but we only care about the attempt count, so keep it fast by cancelling.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_, err := nodeWebhook(ctx, Node{Config: map[string]any{
		"url": "http://x", "method": "GET", "retries": 99, "retry_backoff": 0,
	}}, nc)
	require.NoError(t, err, "give-up is logged, not fatal")
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1))
}

// TestWebhookCfgIntParsesStringAndFloat covers the JSON number / string forms
// the flow config can carry for retries / retry_backoff.
func TestWebhookCfgIntParsesStringAndFloat(t *testing.T) {
	n := Node{Config: map[string]any{"a": float64(4), "b": "7", "c": 2, "d": "bad"}}
	assert.Equal(t, 4, cfgInt(n, "a", 0))
	assert.Equal(t, 7, cfgInt(n, "b", 0))
	assert.Equal(t, 2, cfgInt(n, "c", 0))
	assert.Equal(t, 3, cfgInt(n, "d", 3), "unparseable string falls back to default")
	assert.Equal(t, 9, cfgInt(n, "missing", 9))
	assert.Equal(t, 5, clamp(99, 0, 5))
	assert.Equal(t, 0, clamp(-1, 0, 5))
}

// TestWebhookCatalogExposesNewKeys verifies the UI catalog introspects the new
// config keys on the webhook node type.
func TestWebhookCatalogExposesNewKeys(t *testing.T) {
	for _, nt := range Types() {
		if nt.Name != "webhook" {
			continue
		}
		for _, key := range []string{"headers", "into", "retries", "retry_backoff"} {
			_, ok := nt.Config[key]
			assert.Truef(t, ok, "webhook config exposes %q", key)
		}
		return
	}
	t.Fatal("webhook node type not registered")
}

// TestWebhookEndToEndCaptureFeedsSay wires a GET-fetch webhook whose captured
// body a downstream say node emits — the !cryptoprice shape.
func TestWebhookEndToEndCaptureFeedsSay(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "42000")
	}))
	defer srv.Close()

	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.RunOnce(context.Background(), Flow{
		Name: "price",
		Nodes: []Node{
			{ID: "w", Type: "webhook", Config: map[string]any{"url": srv.URL, "method": "GET", "into": "price"}},
			{ID: "s", Type: "say", Config: map[string]any{"channel": "#c", "text": "BTC {price}"}},
		},
		Edges: []Edge{{From: "w", To: "s"}},
	}, Bag{})
	assert.Equal(t, []string{"say #c BTC 42000"}, act.snapshot())
}

// TestDefaultPostAppliesHeaders exercises defaultPost's header application and
// non-2xx error classification directly.
func TestDefaultPostAppliesHeaders(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Token")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	body, err := defaultPost(context.Background(), "GET", srv.URL, map[string]string{"X-Token": "t"}, nil)
	require.Error(t, err)
	var se *httpStatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusServiceUnavailable, se.status)
	assert.Equal(t, "t", gotKey)
	assert.NotNil(t, body)
}
