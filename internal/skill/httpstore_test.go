package skill

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPStoreNilWhenUnconfigured(t *testing.T) {
	assert.Nil(t, NewHTTPStore("", "tok", nil), "empty base → nil")
	assert.Nil(t, NewHTTPStore("http://x", "", nil), "empty token → nil")
	assert.NotNil(t, NewHTTPStore("http://x", "tok", nil), "both set → non-nil")
}

// stateBackend is a minimal in-memory stand-in for the HTTP state service,
// exercising the GET/PUT/DELETE wire contract HTTPStore speaks.
type stateBackend struct {
	mu       sync.Mutex
	values   map[string]string
	lastAuth string
	lastTTL  int
}

func newStateBackend() *stateBackend {
	return &stateBackend{values: map[string]string{}}
}

func (b *stateBackend) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.lastAuth = r.Header.Get("Authorization")

		// Path shape: /state/{ns}/{key}
		trimmed := strings.TrimPrefix(r.URL.Path, "/state/")
		require.NotEqual(t, r.URL.Path, trimmed, "path must be under /state/")
		key := trimmed // ns/key together identify the row for this test

		switch r.Method {
		case http.MethodGet:
			v, ok := b.values[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"value": v})
		case http.MethodPut:
			var body struct {
				Value      string `json:"value"`
				TTLSeconds int    `json:"ttl_seconds"`
			}
			raw, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(raw, &body))
			b.lastTTL = body.TTLSeconds
			b.values[key] = body.Value
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(b.values, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func TestHTTPStoreSetGetDelete(t *testing.T) {
	b := newStateBackend()
	srv := httptest.NewServer(b.handler(t))
	defer srv.Close()

	s := NewHTTPStore(srv.URL, "sekret", nil)
	require.NotNil(t, s)

	// Missing key → not found.
	_, ok := s.Get("quiz", "score")
	assert.False(t, ok)

	// Set then Get round-trips the value.
	s.Set("quiz", "score", "42", 0)
	got, ok := s.Get("quiz", "score")
	require.True(t, ok)
	assert.Equal(t, "42", got)

	// Bearer token forwarded.
	b.mu.Lock()
	assert.Equal(t, "Bearer sekret", b.lastAuth)
	assert.Equal(t, 0, b.lastTTL, "no ttl → ttl_seconds omitted (0)")
	b.mu.Unlock()

	// Delete removes it.
	s.Delete("quiz", "score")
	_, ok = s.Get("quiz", "score")
	assert.False(t, ok)
}

func TestHTTPStoreSetSendsTTL(t *testing.T) {
	b := newStateBackend()
	srv := httptest.NewServer(b.handler(t))
	defer srv.Close()
	s := NewHTTPStore(srv.URL, "tok", nil)

	s.Set("ns", "k", "v", 90*time.Second)
	b.mu.Lock()
	assert.Equal(t, 90, b.lastTTL)
	b.mu.Unlock()

	// Sub-second positive TTL rounds up to 1 rather than collapsing to "no expiry".
	s.Set("ns", "k2", "v", 500*time.Millisecond)
	b.mu.Lock()
	assert.Equal(t, 1, b.lastTTL)
	b.mu.Unlock()
}

func TestHTTPStoreNamespaceAndKeyEscaped(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	s := NewHTTPStore(srv.URL, "tok", nil)

	s.Set("my skill", "a/b c", "v", 0)
	// Both segments must be percent-escaped so the slash in the key is not read
	// as an extra path segment.
	assert.Equal(t, "/state/"+url.PathEscape("my skill")+"/"+url.PathEscape("a/b c"), gotPath)
}

func TestHTTPStoreTrailingSlashBaseTrimmed(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	s := NewHTTPStore(srv.URL+"/", "tok", nil)
	s.Set("ns", "k", "v", 0)
	assert.Equal(t, "/state/ns/k", gotPath, "no doubled slash from trailing-slash base")
}

func TestHTTPStoreGetDegradesOnErrors(t *testing.T) {
	// Non-200 (e.g. 500) → not found.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	s := NewHTTPStore(srv.URL, "tok", nil)
	_, ok := s.Get("ns", "k")
	assert.False(t, ok)
	srv.Close()

	// Malformed JSON body on 200 → not found.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv2.Close()
	s2 := NewHTTPStore(srv2.URL, "tok", nil)
	_, ok = s2.Get("ns", "k")
	assert.False(t, ok)
}

func TestHTTPStoreWritesDropOnNetworkError(t *testing.T) {
	// Point at a closed server so every request fails at the transport layer.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := srv.URL
	srv.Close()

	s := NewHTTPStore(base, "tok", nil)
	// None of these should panic or block; a state outage is silently degraded.
	s.Set("ns", "k", "v", 0)
	s.Delete("ns", "k")
	_, ok := s.Get("ns", "k")
	assert.False(t, ok)
}

func TestHTTPStoreSetNon2xxLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	s := NewHTTPStore(srv.URL, "tok", nil)
	// Best-effort: a non-2xx is dropped without surfacing an error.
	s.Set("ns", "k", "v", 0)
	s.Delete("ns", "k")
}

func TestHTTPStoreIncrUsesInProcessCounter(t *testing.T) {
	// Incr must never hit the network; a nil/unreachable backend still counts.
	s := NewHTTPStore("http://127.0.0.1:1", "tok", nil)
	n, err := s.Incr("ns", "user", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, err = s.Incr("ns", "user", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

// HTTPStore must satisfy the Store interface.
var _ Store = (*HTTPStore)(nil)
