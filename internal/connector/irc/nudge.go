package irc

import (
	"fmt"
	"sync"
	"time"
)

// OwnerNudge periodically DMs the configured owner with a usage summary.
// Operators wire it via Connector.SetOwnerNudge to surface "the bot's
// been busy" feedback without requiring a separate dashboard.
//
// Note() is called after every successful PRIVMSG the bot sends; when
// the running count hits a multiple of EveryN, the next call emits a
// single DM line to the owner. The day boundary resets the count at UTC
// midnight so a long-running bot doesn't accumulate forever.
type OwnerNudge struct {
	ownerNick string
	everyN    int

	mu    sync.Mutex
	count int
	today string // YYYY-MM-DD in UTC; "" before the first Note()
	now   func() time.Time
}

// NewOwnerNudge returns nil when the configuration is incomplete
// (no owner nick or non-positive everyN) — the caller treats nil as
// "no nudge configured" and skips the wiring.
func NewOwnerNudge(ownerNick string, everyN int) *OwnerNudge {
	if ownerNick == "" || everyN <= 0 {
		return nil
	}
	return &OwnerNudge{
		ownerNick: ownerNick,
		everyN:    everyN,
		now:       time.Now,
	}
}

// Note increments the daily counter; when count is a positive multiple
// of EveryN, sends a single DM line to the owner via the supplied
// writer. Day boundary reset uses UTC.
//
// The writer should bypass any counter that would otherwise re-enter
// Note (the connector's Send path counts; pass the raw client write
// here, never the Send wrapper).
func (n *OwnerNudge) Note(send func(line string) error) {
	if n == nil {
		return
	}

	n.mu.Lock()
	today := n.now().UTC().Format("2006-01-02")
	if today != n.today {
		n.today = today
		n.count = 0
	}
	n.count++
	count := n.count
	shouldFire := count%n.everyN == 0
	n.mu.Unlock()

	if !shouldFire || send == nil {
		return
	}
	_ = send(fmt.Sprintf("PRIVMSG %s :[turborg] you've sent %d messages today", n.ownerNick, count))
}
