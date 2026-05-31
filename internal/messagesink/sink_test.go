package messagesink

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// quietLogger drops logs so error-path tests don't spam test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

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
		mu     sync.Mutex
		got    []*http.Request
		bodies [][]byte
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

// TestNon2xxResponseDoesNotPanic exercises the error branch in post():
// the sink-side endpoint can answer 4xx/5xx (accounts-api outage,
// auth drift, etc.) and we must absorb it best-effort instead of
// crashing the gateway. msg_id idempotency on the receiving side
// makes a future retry harmless so we don't bother with one here.
func TestNon2xxResponseDoesNotPanic(t *testing.T) {
	var hits int32
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sink := New(srv.URL, "tok", nil)
	for i := 0; i < flushBatchSize; i++ {
		sink.Submit(Entry{
			MsgID:   "01HX0000000000000000000000",
			Channel: "#x", Nick: "n", Text: "t",
			Ts: "2026-06-01T00:00:00.000000Z",
		})
	}
	sink.Close(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if hits == 0 {
		t.Fatal("upstream wasn't hit at all — flush didn't fire")
	}
}

// TestPostNetworkErrorIsBestEffort covers the http.Client.Do error
// branch in post(): upstream closed mid-flush, the sink logs + drops
// the batch instead of bubbling the error to the gateway.
func TestPostNetworkErrorIsBestEffort(t *testing.T) {
	// Capture a real httptest server URL, then close it so the sink
	// dials a port nothing's listening on.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadURL := srv.URL
	srv.Close()

	sink := New(deadURL, "tok", nil)
	sink.Submit(Entry{
		MsgID:   "01HX0000000000000000000000",
		Channel: "#x", Nick: "n", Text: "t",
		Ts: "2026-06-01T00:00:00.000000Z",
	})
	// Must complete within the per-request 5s timeout; not panic.
	done := make(chan struct{})
	go func() {
		sink.Close(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(7 * time.Second):
		t.Fatal("Close hung — error path didn't recover")
	}
}

// TestEmptyBufferFlushIsNoOp covers the early-return in flushOnce
// when nothing's pending — exercised on every ticker fire in steady
// state and in Close() on an idle sink.
func TestEmptyBufferFlushIsNoOp(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := New(srv.URL, "tok", nil)
	sink.Close(context.Background())
	if hits != 0 {
		t.Errorf("idle sink hit upstream %d times, want 0", hits)
	}
}

// TestBufferCapDropsOldest exercises the backpressure path: push
// enough entries that the buffer would exceed cap, confirm only the
// tail (last cap entries) survives. Construct the sink without
// starting the run() goroutine so the buffer actually accumulates
// instead of draining each tick.
func TestBufferCapDropsOldest(t *testing.T) {
	s := &Sink{
		endpoint: "http://example.invalid",
		token:    "tok",
		client:   &http.Client{},
		log:      quietLogger(),
		flushCh:  make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
	}
	pushN := bufferCap + 100
	for i := 0; i < pushN; i++ {
		s.Submit(Entry{
			MsgID:   "01HX0000000000000000000000",
			Channel: "#x", Nick: "n", Text: "t",
			Ts: "2026-06-01T00:00:00.000000Z",
		})
		// Drain the trigger channel so it doesn't block subsequent
		// Submits when the buffer keeps hitting the size threshold.
		select {
		case <-s.flushCh:
		default:
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bufferSize != bufferCap {
		t.Errorf("buffer size = %d, want %d (drop-oldest backpressure)", s.bufferSize, bufferCap)
	}
	if len(s.buffer) != bufferCap {
		t.Errorf("buffer len = %d, want %d", len(s.buffer), bufferCap)
	}
}

// TestSubmitOnNilSinkIsNoOp covers the typed-nil-receiver branch of
// Sink.Submit. Lets the gateway hold a nil *Sink in its Options
// without a guard at the call site.
func TestSubmitOnNilSinkIsNoOp(t *testing.T) {
	var s *Sink
	s.Submit(Entry{
		MsgID:   "01HX0000000000000000000000",
		Channel: "#x", Nick: "n", Text: "t",
		Ts: "2026-06-01T00:00:00.000000Z",
	})
}

// TestCloseOnNilSinkIsNoOp covers the matching typed-nil branch in
// Close. The runtime guards New() returning nil but a future caller
// holding the result by interface could still hit this.
func TestCloseOnNilSinkIsNoOp(t *testing.T) {
	var s *Sink
	s.Close(context.Background())
}

// TestRequestBuildErrorIsAbsorbed exercises post()'s
// http.NewRequestWithContext error branch via a URL with control
// characters that the stdlib rejects before any network I/O.
func TestRequestBuildErrorIsAbsorbed(t *testing.T) {
	s := &Sink{
		endpoint: "http://example.com/\x7f\x00bad",
		token:    "tok",
		client:   &http.Client{},
		log:      quietLogger(),
		flushCh:  make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
	}
	// Push directly into the buffer and call flushOnce — bypasses the
	// goroutine so the test stays deterministic.
	s.buffer = []Entry{{
		MsgID:   "01HX0000000000000000000000",
		Channel: "#x", Nick: "n", Text: "t",
		Ts: "2026-06-01T00:00:00.000000Z",
	}}
	s.bufferSize = 1
	s.flushOnce(context.Background())
	// flushOnce always clears the buffer (the entries are "spent"
	// either way — best-effort delivery).
	if s.bufferSize != 0 {
		t.Errorf("buffer size after flush = %d, want 0", s.bufferSize)
	}
}
