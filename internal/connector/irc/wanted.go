package irc

import (
	"strings"
	"sync"
)

// WantedChannel is one entry in the wanted-channels set. Name is the
// case-preserved channel name as the user / server spelled it. Key is
// the channel password (for +k channels) — empty when no key applies.
//
// The reconnect supervisor replays JOIN for every wanted channel on
// transition to `registered`, attaching the cached key when present.
// Without that the supervisor would issue a bare JOIN against a +k
// channel and get 475 ERR_BADCHANNELKEY back.
type WantedChannel struct {
	Name string
	Key  string
}

// WantedChannels is the per-connector "channels I want to be in"
// bookkeeping. Seeded from the operator-configured channel list at
// startup, then mutated by runtime events:
//
//   - upstream self-JOIN echo → Add(name, "")  (server doesn't echo keys)
//   - client-originated JOIN  → Add(name, key)
//   - upstream self-PART echo → Remove(name)
//   - client-originated PART  → Remove(name)
//
// Safe for concurrent use by the connector's read loop, the bouncer's
// forward path, and the supervisor's reconnect replay.
type WantedChannels struct {
	mu       sync.RWMutex
	channels map[string]WantedChannel // lowercased name → entry
	order    []string                 // insertion order for deterministic replay
}

// NewWantedChannels returns a set seeded with the given channel list.
// Each seed entry starts with an empty key — the operator-configured
// list never carries credentials. Pass nil/empty for an empty starting
// set.
func NewWantedChannels(seed []string) *WantedChannels {
	w := &WantedChannels{channels: map[string]WantedChannel{}}
	for _, name := range seed {
		w.Add(name, "")
	}
	return w
}

// Add inserts or updates an entry. The "update" semantics are biased
// toward preserving credentials: when the entry already exists, a
// non-empty key overwrites the stored key, but a *new* empty-key Add
// (e.g. an upstream JOIN echo coming through after the client stored
// a key) does not clobber the existing key.
//
// First insert wins on case preservation — repeated Adds keep the
// originally-cased Name so display matches what the user first typed.
func (w *WantedChannels) Add(name, key string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	lc := strings.ToLower(name)
	if existing, ok := w.channels[lc]; ok {
		if key != "" {
			existing.Key = key
			w.channels[lc] = existing
		}
		return
	}
	w.channels[lc] = WantedChannel{Name: name, Key: key}
	w.order = append(w.order, lc)
}

// Remove drops an entry. No-op when the entry isn't present.
func (w *WantedChannels) Remove(name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	lc := strings.ToLower(name)
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.channels[lc]; !ok {
		return
	}
	delete(w.channels, lc)
	for i, k := range w.order {
		if k == lc {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
}

// Snapshot returns the wanted channels in insertion order. Callers may
// mutate the returned slice safely — it's a copy.
func (w *WantedChannels) Snapshot() []WantedChannel {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]WantedChannel, 0, len(w.order))
	for _, lc := range w.order {
		out = append(out, w.channels[lc])
	}
	return out
}

// Get returns the entry for the named channel, or (zero, false) when
// it isn't in the wanted-set.
func (w *WantedChannels) Get(name string) (WantedChannel, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	entry, ok := w.channels[strings.ToLower(strings.TrimSpace(name))]
	return entry, ok
}

// Len returns the number of wanted channels — cheap, no allocation.
func (w *WantedChannels) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.order)
}

// parseJoinLine extracts channel names and matching keys from an IRC
// JOIN line's params. Standard JOIN syntax is:
//
//	JOIN #a,#b,#c key1,key2,key3
//
// where keys are positionally aligned with channels and a missing key
// is represented by an empty slot. Returns parallel slices; the keys
// slice is padded to len(channels) with empty strings when fewer keys
// were supplied.
//
// Used by both the bouncer's forward path (to record keys it sees in
// client-originated JOINs) and the detached JOIN-queue handler.
func parseJoinLine(params []string, trailing string) (channels, keys []string) {
	target := ""
	keyList := ""
	switch len(params) {
	case 0:
		target = strings.TrimSpace(trailing)
	case 1:
		target = strings.TrimSpace(params[0])
		keyList = strings.TrimSpace(trailing)
	default:
		target = strings.TrimSpace(params[0])
		keyList = strings.TrimSpace(params[1])
	}
	if target == "" {
		return nil, nil
	}
	channels = splitAndTrim(target, ",")
	keys = splitPositional(keyList, ",")
	for len(keys) < len(channels) {
		keys = append(keys, "")
	}
	if len(keys) > len(channels) {
		keys = keys[:len(channels)]
	}
	return channels, keys
}

// splitAndTrim splits a comma-list, trims each element, and drops
// empties. Right for channel names (no IRC channel is the empty
// string) — wrong for positional key lists, where an empty slot is
// meaningful (means "this channel has no key, but the next does").
func splitAndTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitPositional splits a comma-list, trims each element, but
// preserves empty slots so the resulting slice retains its positional
// mapping. Used for JOIN key lists where "k1,,k3" must yield
// ["k1", "", "k3"] so #b picks up "" and #c picks up "k3".
func splitPositional(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = strings.TrimSpace(p)
	}
	return out
}
