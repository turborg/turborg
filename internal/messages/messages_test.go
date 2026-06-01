package messages_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/messagesink"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func mkMsg(channel, nick, text string, ts time.Time) messages.Message {
	return messages.Message{Channel: channel, Nick: nick, Text: text, TS: ts}
}

func TestMemoryStoreEmptyChannelNoop(t *testing.T) {
	s := messages.NewMemoryStore(0)
	require.NoError(t, s.Submit(context.Background(), mkMsg("", "a", "x", time.Now())))
	got, err := s.Recent(context.Background(), "", time.Now(), 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMemoryStoreRecentUnknownChannelEmpty(t *testing.T) {
	s := messages.NewMemoryStore(0)
	require.NoError(t, s.Submit(context.Background(), mkMsg("#a", "alice", "hi", time.Now())))
	// A channel name that was never written to has no bucket; Recent
	// returns nil rather than panicking on the missing map entry.
	got, err := s.Recent(context.Background(), "#never", time.Time{}, 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMemoryStoreSubmitAndRecent(t *testing.T) {
	s := messages.NewMemoryStore(0)
	ctx := context.Background()
	base := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.Submit(ctx, mkMsg("#a", "alice", "hi", base)))
	require.NoError(t, s.Submit(ctx, mkMsg("#a", "bob", "yo", base.Add(time.Second))))
	require.NoError(t, s.Submit(ctx, mkMsg("#b", "cara", "elsewhere", base.Add(2*time.Second))))

	t.Run("recent #a returns newest-first", func(t *testing.T) {
		got, err := s.Recent(ctx, "#a", time.Time{}, 10)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "yo", got[0].Text)
		assert.Equal(t, "hi", got[1].Text)
	})

	t.Run("recent #b is isolated from #a", func(t *testing.T) {
		got, err := s.Recent(ctx, "#b", time.Time{}, 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "elsewhere", got[0].Text)
	})

	t.Run("limit caps result", func(t *testing.T) {
		got, err := s.Recent(ctx, "#a", time.Time{}, 1)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "yo", got[0].Text)
	})

	t.Run("before filter excludes equal and newer", func(t *testing.T) {
		// `before` is exclusive — passing the ts of the newer message
		// must omit it and return only the older one.
		got, err := s.Recent(ctx, "#a", base.Add(time.Second), 10)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "hi", got[0].Text)
	})
}

func TestMemoryStoreRingCap(t *testing.T) {
	s := messages.NewMemoryStore(3)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, s.Submit(ctx, mkMsg("#c", "n", "m"+strconv.Itoa(i),
			time.Date(2026, 5, 21, 12, 0, i, 0, time.UTC))))
	}
	assert.Equal(t, 3, s.Len("#c"))
	got, err := s.Recent(ctx, "#c", time.Time{}, 100)
	require.NoError(t, err)
	require.Len(t, got, 3)
	// Oldest survivor is m7 (m0..m6 dropped); newest is m9.
	assert.Equal(t, "m9", got[0].Text)
	assert.Equal(t, "m7", got[2].Text)
}

func TestMemoryStoreZeroLimit(t *testing.T) {
	s := messages.NewMemoryStore(0)
	require.NoError(t, s.Submit(context.Background(), mkMsg("#a", "n", "m", time.Now())))
	got, err := s.Recent(context.Background(), "#a", time.Time{}, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMemoryStoreConcurrentSubmit(t *testing.T) {
	s := messages.NewMemoryStore(0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Submit(context.Background(), mkMsg("#c", "n", "m",
				time.Now().Add(time.Duration(i)*time.Millisecond)))
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 50, s.Len("#c"))
}

// --- HTTPStore -------------------------------------------------------------

func TestHTTPStoreNilOnMissingConfig(t *testing.T) {
	assert.Nil(t, messages.NewHTTPStore("", "tok", nil, nil))
	assert.Nil(t, messages.NewHTTPStore("http://x", "", nil, nil))
}

func TestHTTPStoreSubmitNilReceiverNoop(t *testing.T) {
	var hs *messages.HTTPStore
	// Calling Submit on a nil receiver must not panic — keeps the
	// caller's "did we configure HTTPStore?" check optional.
	assert.NoError(t, hs.Submit(context.Background(), mkMsg("#a", "n", "t", time.Now())))
}

func TestHTTPStoreSubmitNilSinkNoop(t *testing.T) {
	// HTTPStore with nil sink (a configuration the constructor
	// rejects but defensible against external composition).
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	require.NotNil(t, hs)
	assert.NoError(t, hs.Submit(context.Background(), mkMsg("#a", "n", "t", time.Now())))
}

func TestHTTPStoreRecentRejectsEmptyChannelAndZeroLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Errorf("server should not be hit for empty channel / zero limit")
	}))
	defer srv.Close()
	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	got, err := hs.Recent(context.Background(), "", time.Time{}, 10)
	require.NoError(t, err)
	assert.Empty(t, got)
	got, err = hs.Recent(context.Background(), "#a", time.Time{}, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestHTTPStoreRecentNilReceiverNoop(t *testing.T) {
	var hs *messages.HTTPStore
	got, err := hs.Recent(context.Background(), "#a", time.Time{}, 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestHTTPStoreRecentMalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"messages": "this is not an array"}`))
	}))
	defer srv.Close()
	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	got, err := hs.Recent(context.Background(), "#a", time.Time{}, 10)
	// Decode error is downgraded to empty result, not surfaced.
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestMemoryStoreLenWithUnknownChannel(t *testing.T) {
	s := messages.NewMemoryStore(0)
	assert.Equal(t, 0, s.Len("#never-existed"))
}

func TestHTTPStoreRecentRoundtrip(t *testing.T) {
	// Fake receiver: serves a small history payload, recording the
	// query params it received so we can pin the wire contract.
	var gotPath, gotQuery, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]any{
				{"msg_id": "01H1", "channel": "#a", "nick": "alice", "text": "hi", "ts": "2026-05-21T12:00:00.000000Z"},
				{"msg_id": "01H2", "channel": "#a", "nick": "bob", "text": "yo", "ts": "2026-05-21T12:00:01.000000Z"},
			},
		})
	}))
	defer srv.Close()

	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	require.NotNil(t, hs)

	before := time.Date(2026, 5, 21, 12, 0, 5, 0, time.UTC)
	got, err := hs.Recent(context.Background(), "#a", before, 50)
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, "/", gotPath)
	assert.Equal(t, "Bearer tok", gotAuth)
	q, _ := url.ParseQuery(gotQuery)
	assert.Equal(t, "#a", q.Get("channel"))
	assert.Equal(t, "50", q.Get("limit"))
	assert.Equal(t, "2026-05-21T12:00:05.000000Z", q.Get("before"))

	assert.Equal(t, "alice", got[0].Nick)
	assert.Equal(t, "01H1", got[0].ID)
	assert.True(t, got[0].TS.Equal(time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)))
}

func TestHTTPStoreRecentBeforeOmittedWhenZero(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"messages": []}`))
	}))
	defer srv.Close()

	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	_, err := hs.Recent(context.Background(), "#a", time.Time{}, 10)
	require.NoError(t, err)
	q, _ := url.ParseQuery(gotQuery)
	assert.Empty(t, q.Get("before"), "before must be omitted when zero")
}

func TestHTTPStoreRecentNetworkErrorReturnsEmpty(t *testing.T) {
	// Endpoint that immediately closes — emulates a dead receiver.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	}))
	defer srv.Close()

	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	got, err := hs.Recent(context.Background(), "#a", time.Time{}, 10)
	// Best-effort: no hard error returned, just an empty slice.
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestHTTPStoreRecentBadStatusReturnsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	got, err := hs.Recent(context.Background(), "#a", time.Time{}, 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestHTTPStoreRecentBadTimestampSkipsEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"messages": [
			{"msg_id":"a","channel":"#a","nick":"a","text":"good","ts":"2026-05-21T12:00:00.000000Z"},
			{"msg_id":"b","channel":"#a","nick":"b","text":"bad","ts":"NOT-A-TIMESTAMP"}
		]}`))
	}))
	defer srv.Close()

	hs := messages.NewHTTPStore(srv.URL, "tok", nil, nil)
	got, err := hs.Recent(context.Background(), "#a", time.Time{}, 10)
	require.NoError(t, err)
	require.Len(t, got, 1, "malformed entry must be dropped, not whole reply")
	assert.Equal(t, "good", got[0].Text)
}

func TestHTTPStoreSubmitDelegatesToSink(t *testing.T) {
	// Capture POSTed payload so we know HTTPStore.Submit really
	// pushed through messagesink.Sink. The sink does batching, so we
	// wait briefly after Submit for the flush.
	var (
		mu       sync.Mutex
		captured []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		mu.Lock()
		captured = buf[:n]
		mu.Unlock()
	}))
	defer srv.Close()

	sink := messagesink.New(srv.URL, "tok", nil)
	require.NotNil(t, sink)

	hs := messages.NewHTTPStore(srv.URL, "tok", sink, nil)
	require.NoError(t, hs.Submit(context.Background(), mkMsg("#a", "alice", "hi",
		time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))))

	// Sink batches with a 1s ticker; Close drains synchronously so we
	// don't sleep here. Single Close per Sink — the channel-close inside
	// would panic on a second call.
	sink.Close(context.Background())
	mu.Lock()
	defer mu.Unlock()
	assert.Contains(t, string(captured), `"channel":"#a"`)
	assert.Contains(t, string(captured), `"nick":"alice"`)
	assert.Contains(t, string(captured), `"text":"hi"`)
}

// TestHTTPStoreSubmitMintsULIDWhenIDMissing pins the receiver-contract
// invariant: when Submit is called with an empty Message.ID, the POSTed
// payload still carries a 26-char (Crockford base32) ULID as msg_id.
// Regression guard for the message-history-scrollback refactor where
// the Recorder's ULID minting was dropped and the receiving end's
// "msg_id must be 26 chars" validator silently 422'd every batch.
func TestHTTPStoreSubmitMintsULIDWhenIDMissing(t *testing.T) {
	var (
		mu        sync.Mutex
		bodyBytes []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		mu.Lock()
		bodyBytes = buf[:n]
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sink := messagesink.New(srv.URL, "tok", nil)
	require.NotNil(t, sink)
	hs := messages.NewHTTPStore(srv.URL, "tok", sink, nil)
	require.NotNil(t, hs)

	// Caller passes an empty ID — same shape runtime.makeStoreSubmitter
	// uses for IRC inbound messages.
	require.NoError(t, hs.Submit(context.Background(), mkMsg("#a", "alice", "hi",
		time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))))
	sink.Close(context.Background())

	mu.Lock()
	body := string(bodyBytes)
	mu.Unlock()

	var decoded struct {
		Messages []struct {
			MsgID string `json:"msg_id"`
		} `json:"messages"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &decoded))
	require.Len(t, decoded.Messages, 1)
	gotID := decoded.Messages[0].MsgID
	assert.Len(t, gotID, 26, "receiver validator requires 26-char ULID; empty / short id causes 422")
	// Crockford base32: A-Z (no I, L, O, U) + 0-9. Pin the alphabet so
	// a future refactor that swaps to a different scheme trips this test.
	for _, r := range gotID {
		assert.True(t,
			(r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z' && r != 'I' && r != 'L' && r != 'O' && r != 'U'),
			"msg_id %q contains non-Crockford-base32 char %q", gotID, r)
	}
}

// TestHTTPStoreSubmitPreservesCallerSuppliedID confirms the "explicit
// id wins" branch — a caller that already has a stable ULID (e.g.
// replaying a known-id message from another source) gets it through
// untouched.
func TestHTTPStoreSubmitPreservesCallerSuppliedID(t *testing.T) {
	const presetID = "01HXJZN1H7G0M2K9PQR3S4T5V6"
	var (
		mu        sync.Mutex
		bodyBytes []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		mu.Lock()
		bodyBytes = buf[:n]
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sink := messagesink.New(srv.URL, "tok", nil)
	hs := messages.NewHTTPStore(srv.URL, "tok", sink, nil)
	m := mkMsg("#a", "alice", "hi", time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC))
	m.ID = presetID
	require.NoError(t, hs.Submit(context.Background(), m))
	sink.Close(context.Background())

	mu.Lock()
	body := string(bodyBytes)
	mu.Unlock()
	assert.Contains(t, body, `"msg_id":"`+presetID+`"`)
}
