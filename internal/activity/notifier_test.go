package activity_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/turborg/turborg/internal/activity"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNotifier_DisabledWhenURLEmpty(t *testing.T) {
	n := activity.New("", "", nil)
	assert.False(t, n.Enabled())
	// Calling Mark on a disabled notifier is safe and spawns no goroutine.
	n.Mark(context.Background(), activity.ReasonWSMessage)
	n.Wait()
}

func TestNotifier_NilReceiverIsSafe(t *testing.T) {
	var n *activity.Notifier
	assert.False(t, n.Enabled())
	n.Mark(context.Background(), activity.ReasonWSMessage)
	n.Wait()
}

func TestNotifier_PostsReasonAndBearerToken(t *testing.T) {
	var (
		mu       sync.Mutex
		received []map[string]string
		auth     string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var got map[string]string
		_ = json.Unmarshal(body, &got)
		mu.Lock()
		received = append(received, got)
		auth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	n := activity.New(server.URL, "secret-token", nil)
	require.True(t, n.Enabled())
	n.Mark(context.Background(), activity.ReasonBouncerAttach)
	n.Mark(context.Background(), activity.ReasonTBCommand)
	n.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 2)
	assert.Equal(t, "Bearer secret-token", auth)
	reasons := []string{received[0]["reason"], received[1]["reason"]}
	assert.Contains(t, reasons, activity.ReasonBouncerAttach)
	assert.Contains(t, reasons, activity.ReasonTBCommand)
}

func TestNotifier_NoAuthHeaderWhenTokenEmpty(t *testing.T) {
	var auth atomic.Value
	auth.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	n := activity.New(server.URL, "", nil)
	n.Mark(context.Background(), activity.ReasonWSMessage)
	n.Wait()

	assert.Empty(t, auth.Load().(string))
}

// stubDoer captures the request without sending it, used to verify the
// notifier doesn't error out when the remote returns non-2xx.
type stubDoer struct {
	mu       sync.Mutex
	calls    int
	status   int
	err      error
	lastBody []byte
}

func (s *stubDoer) Do(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if req.Body != nil {
		s.lastBody, _ = io.ReadAll(req.Body)
	}
	if s.err != nil {
		return nil, s.err
	}
	status := s.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(http.NoBody),
	}, nil
}

func TestNotifier_LogsAndSwallowsNon2xx(t *testing.T) {
	doer := &stubDoer{status: http.StatusInternalServerError}
	n := activity.New("http://example.invalid/", "", nil)
	n.SetHTTPClient(doer)
	n.Mark(context.Background(), activity.ReasonWSMessage)
	n.Wait()

	doer.mu.Lock()
	defer doer.mu.Unlock()
	assert.Equal(t, 1, doer.calls)
	assert.Contains(t, string(doer.lastBody), activity.ReasonWSMessage)
}

func TestNotifier_SwallowsTransportError(t *testing.T) {
	doer := &stubDoer{err: assertNetErr{}}
	n := activity.New("http://example.invalid/", "", nil)
	n.SetHTTPClient(doer)
	// Must not panic or block.
	n.Mark(context.Background(), activity.ReasonWSMessage)
	n.Wait()
}

type assertNetErr struct{}

func (assertNetErr) Error() string { return "stub transport failure" }

func TestNotifier_SwallowsBadRequestURL(t *testing.T) {
	// A URL with a control character (\n) fails http.NewRequestWithContext
	// before any transport is consulted; the notifier must log + swallow.
	n := activity.New("http://bad\nurl/", "", nil)
	doer := &stubDoer{}
	n.SetHTTPClient(doer)
	n.Mark(context.Background(), activity.ReasonWSMessage)
	n.Wait()
	// The request never reached the doer because NewRequest failed.
	doer.mu.Lock()
	defer doer.mu.Unlock()
	assert.Equal(t, 0, doer.calls)
}

func TestNotifier_HookMarksWithBackgroundContext(t *testing.T) {
	doer := &stubDoer{status: http.StatusNoContent}
	n := activity.New("http://example.invalid/", "", nil)
	n.SetHTTPClient(doer)
	// Hook is the closure-free entry point the runtime wires into the
	// connector/gateway attach callbacks; it must post the reason just
	// like Mark with a Background context.
	n.Hook(activity.ReasonBouncerAttach)
	n.Wait()

	doer.mu.Lock()
	defer doer.mu.Unlock()
	require.Equal(t, 1, doer.calls)
	assert.Contains(t, string(doer.lastBody), activity.ReasonBouncerAttach)
}

func TestNotifier_SetHTTPClientOnNilReceiverIsSafe(t *testing.T) {
	var n *activity.Notifier
	// Must not panic.
	n.SetHTTPClient(&stubDoer{})
}
