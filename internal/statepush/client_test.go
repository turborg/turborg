package statepush_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/turborg/turborg/internal/statepush"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestClient_PutSendsJSONWithBearer(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies [][]byte
		auths  []string
		paths  []string
		ctypes []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, body)
		auths = append(auths, r.Header.Get("Authorization"))
		paths = append(paths, r.URL.Path)
		ctypes = append(ctypes, r.Header.Get("Content-Type"))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL+"/state", "secret", nil)
	err := c.Put(context.Background(), map[string]string{"hello": "world"})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, bodies, 1)
	assert.Equal(t, "Bearer secret", auths[0])
	assert.Equal(t, "/state", paths[0])
	assert.Equal(t, "application/json", ctypes[0])
	var got map[string]string
	require.NoError(t, json.Unmarshal(bodies[0], &got))
	assert.Equal(t, "world", got["hello"])
}

func TestClient_PutNoAuthHeaderWhenTokenEmpty(t *testing.T) {
	var auth atomic.Value
	auth.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL, "", nil)
	require.NoError(t, c.Put(context.Background(), map[string]string{"x": "y"}))
	assert.Empty(t, auth.Load().(string))
}

func TestClient_PutIsNoOpWhenURLEmpty(t *testing.T) {
	c := statepush.NewClient("", "", nil)
	// Must not error, must not panic.
	require.NoError(t, c.Put(context.Background(), struct{}{}))
}

func TestClient_PutIsNoOpOnNilReceiver(t *testing.T) {
	var c *statepush.Client
	require.NoError(t, c.Put(context.Background(), struct{}{}))
	assert.Empty(t, c.URL())
}

// stubDoer captures every Do attempt and returns canned responses.
type stubDoer struct {
	mu       sync.Mutex
	calls    int
	statuses []int // pulled in order; last value persists once exhausted
	errs     []error
	lastBody []byte
}

func (s *stubDoer) Do(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if req.Body != nil {
		s.lastBody, _ = io.ReadAll(req.Body)
	}
	var err error
	if len(s.errs) > 0 {
		err = s.errs[0]
		if len(s.errs) > 1 {
			s.errs = s.errs[1:]
		}
	}
	if err != nil {
		return nil, err
	}
	status := http.StatusOK
	if len(s.statuses) > 0 {
		status = s.statuses[0]
		if len(s.statuses) > 1 {
			s.statuses = s.statuses[1:]
		}
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(http.NoBody),
	}, nil
}

func (s *stubDoer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestClient_PutRetriesTransientFailures(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	doer := &stubDoer{statuses: []int{500, 500, 204}}
	c := statepush.NewClient("http://obs/state", "", nil)
	c.SetHTTPClient(doer)

	require.NoError(t, c.Put(context.Background(), struct{}{}))
	assert.Equal(t, 3, doer.count())
}

func TestClient_PutGivesUpAfterTwoRetries(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	doer := &stubDoer{statuses: []int{500, 502, 503}}
	c := statepush.NewClient("http://obs/state", "", nil)
	c.SetHTTPClient(doer)

	err := c.Put(context.Background(), struct{}{})
	require.Error(t, err)
	assert.Equal(t, 3, doer.count())
}

func TestClient_PutDoesNotRetry4xx(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	doer := &stubDoer{statuses: []int{http.StatusBadRequest}}
	c := statepush.NewClient("http://obs/state", "", nil)
	c.SetHTTPClient(doer)

	err := c.Put(context.Background(), struct{}{})
	require.Error(t, err)
	assert.Equal(t, 1, doer.count())
}

func TestClient_PutDoesNotRetryAuthFailure(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	doer := &stubDoer{statuses: []int{http.StatusUnauthorized}}
	c := statepush.NewClient("http://obs/state", "stale", nil)
	c.SetHTTPClient(doer)

	err := c.Put(context.Background(), struct{}{})
	require.Error(t, err)
	assert.Equal(t, 1, doer.count())
}

func TestClient_PutRetriesTransportErrors(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	doer := &stubDoer{
		errs:     []error{errStub("transport down"), errStub("transport down"), nil},
		statuses: []int{200, 200, 204},
	}
	c := statepush.NewClient("http://obs/state", "", nil)
	c.SetHTTPClient(doer)

	require.NoError(t, c.Put(context.Background(), struct{}{}))
	assert.Equal(t, 3, doer.count())
}

func TestClient_PutHonorsContextCancel(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{
		50 * time.Millisecond,
		50 * time.Millisecond,
	})()

	doer := &stubDoer{statuses: []int{500, 500, 500}}
	c := statepush.NewClient("http://obs/state", "", nil)
	c.SetHTTPClient(doer)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := c.Put(ctx, struct{}{})
	require.Error(t, err)
	// Should bail after the first 500 instead of finishing all 3 attempts.
	assert.LessOrEqual(t, doer.count(), 2)
}

func TestClient_SetHTTPClientOnNilReceiverIsSafe(t *testing.T) {
	var c *statepush.Client
	c.SetHTTPClient(&stubDoer{})
}

func TestClient_AuthErrorLogRateLimited(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	// Two consecutive auth rejections within a minute should log only
	// once (the second falls under the per-minute rate limit). The test
	// asserts the second call still returns an error and doesn't crash.
	doer := &stubDoer{statuses: []int{http.StatusUnauthorized, http.StatusForbidden}}
	c := statepush.NewClient("http://obs/state", "stale", nil)
	c.SetHTTPClient(doer)

	require.Error(t, c.Put(context.Background(), struct{}{}))
	require.Error(t, c.Put(context.Background(), struct{}{}))
	assert.Equal(t, 2, doer.count())
}

func TestClient_PutContextCancelDuringTransport(t *testing.T) {
	defer statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})()

	doer := &stubDoer{errs: []error{context.Canceled}}
	c := statepush.NewClient("http://obs/state", "", nil)
	c.SetHTTPClient(doer)

	err := c.Put(context.Background(), struct{}{})
	require.Error(t, err)
	// Context-canceled mid-transport is treated as a terminal cancel,
	// not a retryable transport blip — exactly one attempt fires.
	assert.Equal(t, 1, doer.count())
}

type errStub string

func (e errStub) Error() string { return string(e) }
