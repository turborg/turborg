package web_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/web"
)

// captureFire records the last (name, bag) FireWebhook was called with and
// returns a configurable result.
type captureFire struct {
	mu     sync.Mutex
	calls  int
	name   string
	bag    map[string]string
	result bool
}

func (c *captureFire) fn(name string, bag map[string]string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.name = name
	c.bag = bag
	return c.result
}

func (c *captureFire) snapshot() (int, string, map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.name, c.bag
}

// newHookGateway builds a gateway whose /hook route dispatches to fire, without
// binding a listener — tests drive Handler() directly with httptest.
func newHookGateway(t *testing.T, password string, fire func(string, map[string]string) bool) (*web.Gateway, *agent.Agent) {
	t.Helper()
	v, err := web.NewStaticPasswordVerifier(password)
	require.NoError(t, err)
	g, err := web.New(newFakeBridge("bot"), &fakeSender{}, web.Options{
		Verifier:    v,
		WebhookFire: fire,
	})
	require.NoError(t, err)
	a := agent.New(nil)
	g.Subscribe(a.Events)
	return g, a
}

func postHook(g *web.Gateway, target string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)
	return rec
}

func TestHookRejectsMissingToken(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	rec := postHook(g, "/hook/deploy", `{"text":"hi"}`)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	calls, _, _ := cap.snapshot()
	assert.Zero(t, calls, "dispatch must not run for an unauthenticated request")
}

func TestHookRejectsBadToken(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	rec := postHook(g, "/hook/deploy?token=wrong", `{"text":"hi"}`)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.NotContains(t, rec.Body.String(), "deploy", "401 body must not leak the flow name")
	calls, _, _ := cap.snapshot()
	assert.Zero(t, calls)
}

func TestHookAcceptsBearerToken(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	req := httptest.NewRequest(http.MethodPost, "/hook/deploy", strings.NewReader(`{"text":"hi"}`))
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	calls, name, _ := cap.snapshot()
	assert.Equal(t, 1, calls)
	assert.Equal(t, "deploy", name)
}

func TestHookUnknownNameReturns404(t *testing.T) {
	cap := &captureFire{result: false} // no matching trigger
	g, _ := newHookGateway(t, "secret", cap.fn)

	rec := postHook(g, "/hook/ghost?token=secret", `{"text":"hi"}`)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.NotContains(t, rec.Body.String(), "ghost", "404 body must not echo the requested name")
	calls, name, _ := cap.snapshot()
	assert.Equal(t, 1, calls)
	assert.Equal(t, "ghost", name)
}

func TestHookDisabledWhenNoDispatcher(t *testing.T) {
	g, _ := newHookGateway(t, "secret", nil) // WebhookFire nil = ingress disabled

	rec := postHook(g, "/hook/deploy?token=secret", `{"text":"hi"}`)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHookFiresWithBodyInBag(t *testing.T) {
	cap := &captureFire{result: true}
	g, a := newHookGateway(t, "secret", cap.fn)

	// Audit event should fire exactly once on a successful dispatch.
	var audit int
	var auditMu sync.Mutex
	a.Events.Subscribe(agent.EventWebhookReceived, func(_ context.Context, ev *agent.Event) {
		auditMu.Lock()
		defer auditMu.Unlock()
		audit++
		assert.Equal(t, "deploy", ev.Fields["name"])
	})

	body := `{"text":"ship it","from":"ci","channel":"#ops","count":3,"ok":true}`
	rec := postHook(g, "/hook/deploy?token=secret", body)

	assert.Equal(t, http.StatusOK, rec.Code)
	calls, name, bag := cap.snapshot()
	assert.Equal(t, 1, calls)
	assert.Equal(t, "deploy", name)
	// Top-level scalars become string bag vars.
	assert.Equal(t, "ship it", bag["text"])
	assert.Equal(t, "ci", bag["from"])
	assert.Equal(t, "#ops", bag["channel"])
	assert.Equal(t, "3", bag["count"])
	assert.Equal(t, "true", bag["ok"])
	// Raw JSON preserved under "body".
	assert.Equal(t, body, bag["body"])
	// {user} seeded from "from" when no explicit "user".
	assert.Equal(t, "ci", bag["user"])

	// Give the async bus publish a beat to land.
	require.Eventually(t, func() bool {
		auditMu.Lock()
		defer auditMu.Unlock()
		return audit == 1
	}, time.Second, 5*time.Millisecond)
}

func TestHookUserFieldWins(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	rec := postHook(g, "/hook/x?token=secret", `{"user":"alice","from":"bob"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
	_, _, bag := cap.snapshot()
	assert.Equal(t, "alice", bag["user"], "explicit user field takes precedence over from")
}

func TestHookNonObjectBodyStillDispatches(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	rec := postHook(g, "/hook/x?token=secret", `"just a string"`)
	assert.Equal(t, http.StatusOK, rec.Code)
	_, _, bag := cap.snapshot()
	assert.Equal(t, `"just a string"`, bag["body"])
}

func TestHookRejectsOversizedBody(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	// 64 KiB + slack of JSON padding — well over the cap.
	big := `{"text":"` + strings.Repeat("a", 70<<10) + `"}`
	rec := postHook(g, "/hook/deploy?token=secret", big)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	calls, _, _ := cap.snapshot()
	assert.Zero(t, calls, "an over-cap body must never reach dispatch")
}

func TestHookGetMethodNotRouted(t *testing.T) {
	cap := &captureFire{result: true}
	g, _ := newHookGateway(t, "secret", cap.fn)

	// GET is not the hook method; it falls through to the static file server,
	// which has no such file — never a 200 dispatch.
	req := httptest.NewRequest(http.MethodGet, "/hook/deploy?token=secret", nil)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	assert.NotEqual(t, http.StatusOK, rec.Code)
	calls, _, _ := cap.snapshot()
	assert.Zero(t, calls)
}
