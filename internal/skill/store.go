package skill

import (
	"sync"
	"time"
)

// Store is the small key/value + counter backend skills use for state that
// must survive between fires: a value with an optional TTL (setvar/getvar) and
// a per-key sliding-window counter (flood detection). Keys are namespaced by
// skill name so two skills can't collide. Implementations must be safe for
// concurrent use.
type Store interface {
	// Get returns the stored value for (skill,key) and whether it was present
	// and unexpired.
	Get(skill, key string) (string, bool)
	// Set stores val for (skill,key). A positive ttl expires it after that
	// duration; a non-positive ttl stores it without expiry.
	Set(skill, key, val string, ttl time.Duration)
	// Incr records one hit for (skill,key) in a sliding window and returns the
	// number of hits within the window (including this one).
	Incr(skill, key string, window time.Duration) (int, error)
}

// clock abstracts the time source so tests can drive TTLs and windows
// deterministically.
type clock func() time.Time

type memValue struct {
	val       string
	expiresAt time.Time // zero = no expiry
}

// MemoryStore is the default in-process Store. Values support TTL expiry and
// counters use a sliding window pruned on each Incr, mirroring the IRC
// throttle's window bookkeeping.
type MemoryStore struct {
	now clock

	mu     sync.Mutex
	values map[string]memValue
	counts map[string][]time.Time
}

// NewMemoryStore returns an empty in-process store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		now:    time.Now,
		values: map[string]memValue{},
		counts: map[string][]time.Time{},
	}
}

func storeKey(skill, key string) string { return skill + "\x00" + key }

func (m *MemoryStore) Get(skill, key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.values[storeKey(skill, key)]
	if !ok {
		return "", false
	}
	if !v.expiresAt.IsZero() && !m.now().Before(v.expiresAt) {
		delete(m.values, storeKey(skill, key))
		return "", false
	}
	return v.val, true
}

func (m *MemoryStore) Set(skill, key, val string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var exp time.Time
	if ttl > 0 {
		exp = m.now().Add(ttl)
	}
	m.values[storeKey(skill, key)] = memValue{val: val, expiresAt: exp}
}

func (m *MemoryStore) Incr(skill, key string, window time.Duration) (int, error) {
	if window <= 0 {
		window = time.Minute
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := storeKey(skill, key)
	now := m.now()
	cutoff := now.Add(-window)
	bucket := m.counts[k]
	pruned := bucket[:0]
	for _, t := range bucket {
		if !t.Before(cutoff) {
			pruned = append(pruned, t)
		}
	}
	pruned = append(pruned, now)
	m.counts[k] = pruned
	return len(pruned), nil
}
