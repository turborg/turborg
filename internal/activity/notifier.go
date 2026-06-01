// Package activity exposes a tiny fire-and-forget webhook client the agent
// uses to signal meaningful runtime events to a remote observer.
//
// Activity means genuine owner presence, never "the bot is doing work"
// or "messages are flowing through the channel". Call sites fire Mark
// only for owner-driven signals: a bouncer client attaching
// ("bouncer_attach"), an owner message through the web chat
// ("ws_message"), an owner /tb command ("tb_command"), and a periodic
// presence heartbeat while a bouncer client is attached or a web session
// is engaged ("presence"). Inbound channel traffic, the bot's own
// replies, and bare idle dashboard tabs deliberately do NOT fire Mark.
// Each Mark is a single POST with a small JSON body; failures are logged
// at debug and never block the caller — the notifier is observability,
// not control.
//
// When TURBORG_ACTIVITY_URL is unset the package returns a Notifier
// whose Mark is a no-op, so self-host operators who don't run an
// observer see no behavior change.
package activity

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Reason identifiers for the owner-presence call sites. Stable values
// the observer can dispatch on without parsing free-form text.
const (
	ReasonBouncerAttach = "bouncer_attach"
	ReasonWSMessage     = "ws_message"
	ReasonTBCommand     = "tb_command"
	ReasonPresence      = "presence"
)

// defaultTimeout caps individual HTTP POSTs so a wedged observer cannot
// stall the surrounding hot path.
const defaultTimeout = 3 * time.Second

// HTTPDoer is the minimal slice of *http.Client the notifier consumes.
// Pulled out so tests can install a stub without spinning a real server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Notifier posts activity-signal events to a configured webhook URL.
// A Notifier with an empty URL is a no-op — every Mark returns
// immediately without firing any HTTP.
type Notifier struct {
	url    string
	token  string
	client HTTPDoer
	log    *slog.Logger
	wg     sync.WaitGroup
}

// New returns a Notifier wired to url. When url is empty, Mark is a
// no-op and no goroutine is ever spawned. token is sent as a bearer
// authorization header when non-empty.
func New(url, token string, log *slog.Logger) *Notifier {
	if log == nil {
		log = slog.Default()
	}
	return &Notifier{
		url:    url,
		token:  token,
		client: &http.Client{Timeout: defaultTimeout},
		log:    log,
	}
}

// Enabled reports whether the notifier is wired to a real URL. Callers
// use it to skip work (e.g. building a payload) when no observer is
// configured.
func (n *Notifier) Enabled() bool {
	return n != nil && n.url != ""
}

// Mark fires a fire-and-forget POST with the given reason. Safe to call
// on a nil receiver and on a Notifier with no URL — both are no-ops.
// The call returns immediately; the actual HTTP runs in a background
// goroutine bounded by defaultTimeout. Use Wait to drain in-flight
// posts before shutdown (tests rely on this for goleak).
func (n *Notifier) Mark(_ context.Context, reason string) {
	if !n.Enabled() {
		return
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.fire(reason)
	}()
}

// Hook returns a context-free callback shape suitable for handing to
// surfaces that emit a stable reason string (the IRC connector's
// SetBouncerAttachHook and the WS gateway's OnActivity). Equivalent to
// calling Mark with a Background context; exists so the runtime wires
// the notifier without an in-line closure that the cover gate has to
// chase.
func (n *Notifier) Hook(reason string) {
	n.Mark(context.Background(), reason)
}

// Wait blocks until every Mark spawned before the call has returned.
// Idempotent — callers can issue further Marks after Wait returns; they
// register on the same WaitGroup and Wait can be called again. Intended
// for graceful shutdown and for deterministic tests.
func (n *Notifier) Wait() {
	if n == nil {
		return
	}
	n.wg.Wait()
}

func (n *Notifier) fire(reason string) {
	// Payload is a fixed-shape one-field object. Building it with a
	// quoted reason via strconv.Quote is cheaper than json.Marshal and
	// removes an unreachable error branch from this hot path.
	body := []byte(fmt.Sprintf(`{"reason":%s}`, strconv.Quote(reason)))
	reqCtx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		n.log.Debug("activity request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if n.token != "" {
		req.Header.Set("Authorization", "Bearer "+n.token)
	}
	resp, err := n.client.Do(req)
	if err != nil {
		n.log.Debug("activity post", "url", n.url, "reason", reason, "err", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		n.log.Debug("activity non-2xx", "status", resp.StatusCode, "reason", reason)
	}
}

// SetHTTPClient swaps the underlying HTTP doer. Used by tests; production
// callers leave the default *http.Client in place.
func (n *Notifier) SetHTTPClient(c HTTPDoer) {
	if n == nil {
		return
	}
	n.client = c
}
