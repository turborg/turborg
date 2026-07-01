package skill

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreSetGetNamespaced(t *testing.T) {
	s := NewMemoryStore()
	s.Set("a", "k", "v1", 0)
	s.Set("b", "k", "v2", 0)

	got, ok := s.Get("a", "k")
	require.True(t, ok)
	assert.Equal(t, "v1", got)
	got, ok = s.Get("b", "k")
	require.True(t, ok)
	assert.Equal(t, "v2", got, "keys are namespaced by skill")

	_, ok = s.Get("a", "missing")
	assert.False(t, ok)
}

func TestStoreTTLExpiry(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(1000, 0)
	s.now = func() time.Time { return now }

	s.Set("a", "k", "v", time.Minute)
	got, ok := s.Get("a", "k")
	require.True(t, ok)
	assert.Equal(t, "v", got)

	now = now.Add(2 * time.Minute)
	_, ok = s.Get("a", "k")
	assert.False(t, ok, "value past its TTL is gone")
}

func TestStoreIncrSlidingWindow(t *testing.T) {
	s := NewMemoryStore()
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }

	for i := 1; i <= 3; i++ {
		n, err := s.Incr("flood", "alice", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, i, n)
	}
	// Advance past the window — the count resets.
	now = now.Add(11 * time.Second)
	n, err := s.Incr("flood", "alice", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "events older than the window are pruned")
}

func TestStoreIncrDefaultsWindow(t *testing.T) {
	s := NewMemoryStore()
	n, err := s.Incr("flood", "k", 0)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestStoreDelete(t *testing.T) {
	s := NewMemoryStore()
	s.Set("ns", "k", "v", 0)
	if _, ok := s.Get("ns", "k"); !ok {
		t.Fatal("expected value present before delete")
	}
	s.Delete("ns", "k")
	if _, ok := s.Get("ns", "k"); ok {
		t.Fatal("expected value gone after delete")
	}
	// Deleting an absent key is a no-op (must not panic).
	s.Delete("ns", "missing")
}
