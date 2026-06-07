package statepush

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// defaultDebounce is the debounce window applied to bursty state
// changes — a /join #a,#b,#c,#d,#e fans into one PUT, not five. 0
// disables the debounce (every NotifyChange fires immediately, useful
// in tests).
const defaultDebounce = 250 * time.Millisecond

// SnapshotBuilder returns the authoritative state snapshot to send.
// Called inside the emitter goroutine immediately before the PUT —
// the returned value is the latest known truth at that moment.
type SnapshotBuilder func() Snapshot

// Snapshot is the per-emit payload shape. Keys in the connectors map
// are connector names ("irc"); values are the per-connector state.
//
// Wire shape:
//
//	{
//	  "connectors": {
//	    "irc": {
//	      "state": "registered",
//	      "since": "2026-05-17T15:30:00Z",
//	      "channels": [{"name": "#x", "key": null}, ...],
//	      "nick": "stefan",
//	      "reason": ""
//	    }
//	  }
//	}
type Snapshot struct {
	Connectors map[string]ConnectorSnapshot `json:"connectors"`
}

// ConnectorSnapshot describes one connector's authoritative state.
// Channels is always non-nil on the wire (empty slice → []), nick is
// always a string (empty when neither preferred nor live is known).
// state is the string form of the connector's state machine value.
// since is an ISO-8601 UTC timestamp.
type ConnectorSnapshot struct {
	State    string            `json:"state"`
	Since    time.Time         `json:"since"`
	Channels []ChannelSnapshot `json:"channels"`
	Nick     string            `json:"nick"`
	// DesiredNick is the nick the user wants (the reclaim target), which
	// differs from Nick when the server forced a "_" fallback. Receivers
	// persist this as the desired/intent nick and Nick as the observed one,
	// so a fallback never overwrites the user's saved nick.
	DesiredNick string `json:"desired_nick"`
	// Reason is the last server-supplied human-readable explanation for
	// the current state — e.g. the disconnect/ban text an upstream sent.
	// Empty for healthy states or when the server gave no reason.
	Reason string `json:"reason"`
}

// ChannelSnapshot is one wanted-channel entry. Key is nullable on the
// wire so receivers can distinguish "no key" (channel has no +k) from
// "explicit empty key" — encoded as *string so empty becomes JSON
// null rather than "".
type ChannelSnapshot struct {
	Name string  `json:"name"`
	Key  *string `json:"key"`
}

// NewChannelSnapshot is a helper that constructs a ChannelSnapshot
// with the JSON-null semantics: empty key → Key field is nil → encoded
// as `"key": null`. Non-empty key → encoded as `"key": "<value>"`.
func NewChannelSnapshot(name, key string) ChannelSnapshot {
	out := ChannelSnapshot{Name: name}
	if key != "" {
		k := key
		out.Key = &k
	}
	return out
}

// Emitter coalesces a stream of NotifyChange calls into debounced
// snapshot PUTs against a Client. Construct one per agent and wire
// NotifyChange to the connector's state-machine subscription,
// wanted-channels OnChange, and preferred-nick setter.
//
// The emitter owns one background goroutine which runs the debounce
// timer and fires the PUT. Stop terminates it and blocks until the
// goroutine returns — call from the agent's shutdown path so goleak
// stays clean.
//
// All methods are safe on a nil receiver and on an emitter built
// against a disabled (empty-URL) client — both are no-ops.
type Emitter struct {
	client   *Client
	build    SnapshotBuilder
	debounce time.Duration
	log      *slog.Logger

	notify chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewEmitter wires an Emitter to the given client and snapshot
// builder. When client is nil or its URL is empty, the returned
// emitter is a working but inert no-op — calls to NotifyChange and
// Stop are safe but produce no HTTP traffic.
//
// debounce <= 0 → no debounce; every NotifyChange call fires a PUT
// directly. Values larger than a few seconds defeat the "real-time
// status dot" intent and are clamped by the operator-policy caller,
// not here.
func NewEmitter(client *Client, build SnapshotBuilder, debounce time.Duration, log *slog.Logger) *Emitter {
	if log == nil {
		log = slog.Default()
	}
	if debounce < 0 {
		debounce = defaultDebounce
	}
	e := &Emitter{
		client:   client,
		build:    build,
		debounce: debounce,
		log:      log,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	if !e.enabled() {
		// Inert emitter: leave the goroutine unstarted. Stop and
		// NotifyChange both short-circuit via the enabled() check.
		return e
	}
	e.wg.Add(1)
	go e.run()
	return e
}

// enabled reports whether the emitter has a real destination to PUT
// to. False for nil receivers, missing clients, or clients with empty
// URLs (the operator didn't wire a webhook).
func (e *Emitter) enabled() bool {
	return e != nil && e.client != nil && e.client.url != "" && e.build != nil
}

// NotifyChange marks the emitter dirty and (re)arms the debounce
// timer. Safe to call from any goroutine and on a nil/disabled
// emitter. The actual PUT runs after the debounce window elapses
// without another NotifyChange.
func (e *Emitter) NotifyChange() {
	if !e.enabled() {
		return
	}
	// Non-blocking send: an already-queued tick coalesces this call.
	select {
	case e.notify <- struct{}{}:
	default:
	}
}

// Stop terminates the emitter goroutine and blocks until it returns.
// Safe to call on a nil or disabled emitter. After Stop, NotifyChange
// remains safe but is a no-op (the goroutine is gone).
func (e *Emitter) Stop() {
	if !e.enabled() {
		return
	}
	// close(done) is the cancel signal; idempotent guard handles the
	// case where Stop is called twice on a real shutdown path.
	select {
	case <-e.done:
		// already stopped
	default:
		close(e.done)
	}
	e.wg.Wait()
}

func (e *Emitter) run() {
	defer e.wg.Done()

	for {
		select {
		case <-e.done:
			return
		case <-e.notify:
			if e.debounce <= 0 {
				e.send()
				continue
			}
			if !e.collect() {
				return
			}
			e.send()
		}
	}
}

// collect waits out the debounce window after the first notify, eating
// any further notifies and re-arming the timer each time. Returns
// false when done was closed (caller should return), true when the
// timer fired clean.
func (e *Emitter) collect() bool {
	timer := time.NewTimer(e.debounce)
	for {
		select {
		case <-e.done:
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-e.notify:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(e.debounce)
		case <-timer.C:
			return true
		}
	}
}

// Flush synchronously builds and PUTs the current snapshot, bypassing the
// debounce. Call it on teardown so a terminal state that arrives just before
// Stop (e.g. disconnected_banned ~1ms before the tenant's work returns) is
// still delivered — Stop cancels the debounce timer, so the pending PUT would
// otherwise be dropped and the control plane left at the last state (the
// classic "stuck at registering" symptom). Safe on a nil/disabled emitter.
func (e *Emitter) Flush(ctx context.Context) {
	if !e.enabled() {
		return
	}
	if err := e.client.Put(ctx, e.build()); err != nil {
		e.log.Debug("state webhook flush PUT failed", "err", err)
	}
}

func (e *Emitter) send() {
	snapshot := e.build()
	// Bound the whole PUT (including retries) by a generous ceiling so
	// a chronically wedged webhook cannot stall NotifyChange callers
	// indefinitely. Each individual attempt is already capped by
	// defaultHTTPTimeout in Client; this is a belt-and-braces parent
	// deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.client.Put(ctx, snapshot); err != nil {
		e.log.Debug("state webhook PUT failed", "err", err)
	}
}
