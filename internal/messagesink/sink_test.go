package messagesink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSinkWireShape pins the contract with sidecar:
//   - POST to the configured endpoint, no path mutation
//   - Authorization: Bearer <token>
//   - Content-Type: application/json
//   - Body: {"messages":[{msg_id, channel, nick, text, ts, kind?}, ...]}
//
// Any drift here breaks turborg → sidecar; the corresponding test in
// sidecar/messages_sink_test.go pins the other end of the same wire.
func TestSinkWireShape(t *testing.T) {
	var (
		mu      sync.Mutex
		got     []*http.Request
		bodies  [][]byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r)
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sink := New(srv.URL, "test-bearer", nil)
	if sink == nil {
		t.Fatal("sink should not be nil with valid config")
	}

	rec := NewRecorder(sink)
	ts := time.Date(2026, 6, 1, 12, 0, 0, 123456000, time.UTC)
	rec.Submit("#xshellz-test", "alice", "hello world", ts)

	// Close to drain.
	sink.Close(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("expected 1 POST, got %d", len(got))
	}
	req := got[0]
	if req.Method != http.MethodPost {
		t.Errorf("method = %q, want POST", req.Method)
	}
	if h := req.Header.Get("Authorization"); h != "Bearer test-bearer" {
		t.Errorf("auth = %q, want bearer", h)
	}
	if h := req.Header.Get("Content-Type"); h != "application/json" {
		t.Errorf("content-type = %q", h)
	}

	var body struct {
		Messages []Entry `json:"messages"`
	}
	if err := json.Unmarshal(bodies[0], &body); err != nil {
		t.Fatalf("body decode: %v", err)
	}
	if len(body.Messages) != 1 {
		t.Fatalf("entries = %d, want 1", len(body.Messages))
	}
	e := body.Messages[0]
	if e.Channel != "#xshellz-test" || e.Nick != "alice" || e.Text != "hello world" {
		t.Errorf("entry fields wrong: %+v", e)
	}
	if e.Kind != "message" {
		t.Errorf("kind = %q, want message", e.Kind)
	}
	if e.Ts != "2026-06-01T12:00:00.123456Z" {
		t.Errorf("ts = %q, want 2026-06-01T12:00:00.123456Z", e.Ts)
	}
	if len(e.MsgID) != 26 {
		t.Errorf("msg_id length = %d, want 26 (ULID)", len(e.MsgID))
	}
}

// TestSinkNilWhenUnconfigured matches the cap_hit + statepush
// convention: missing env = nil constructor = silent no-op upstream.
// Lets self-host turborg run without a SaaS backplane.
func TestSinkNilWhenUnconfigured(t *testing.T) {
	if got := New("", "tok", nil); got != nil {
		t.Error("empty endpoint should yield nil sink")
	}
	if got := New("http://x", "", nil); got != nil {
		t.Error("empty token should yield nil sink")
	}
}

// TestRecorderNilSafeOnNilSink documents that callers can hold a nil
// recorder and Submit unconditionally — useful for the gateway's
// non-nil-interface assignment trick in runtime.
func TestRecorderNilSafeOnNilSink(t *testing.T) {
	r := NewRecorder(nil)
	if r != nil {
		t.Fatal("recorder with nil sink should itself be nil so the gateway's interface check works")
	}
	// And the receiver-nil case is also safe to call.
	var typed *Recorder
	typed.Submit("#x", "n", "t", time.Now())
}

// TestSizeTriggersFlushAheadOfTicker confirms the 10-entry threshold
// fires a flush before the 1s interval. Bursty channels get their
// batches delivered with bounded latency.
func TestSizeTriggersFlushAheadOfTicker(t *testing.T) {
	var got int
	var wg sync.WaitGroup
	wg.Add(1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		if got == 0 {
			wg.Done()
		}
		got++
	}))
	defer srv.Close()

	sink := New(srv.URL, "tok", nil)
	rec := NewRecorder(sink)

	for i := 0; i < flushBatchSize; i++ {
		rec.Submit("#x", "n", strings.Repeat("a", 4), time.Now())
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("flush didn't fire on size trigger; ticker would take ~1s")
	}
	sink.Close(context.Background())
}
