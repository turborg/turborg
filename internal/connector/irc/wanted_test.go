package irc_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestWantedChannelsSeedPreservesOrder(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels([]string{"#a", "#b", "#c"})
	snap := w.Snapshot()
	require.Len(t, snap, 3)
	assert.Equal(t, "#a", snap[0].Name)
	assert.Equal(t, "#b", snap[1].Name)
	assert.Equal(t, "#c", snap[2].Name)
	for _, e := range snap {
		assert.Empty(t, e.Key, "seed entries must have empty keys — the operator-configured list never carries credentials")
	}
}

func TestWantedChannelsAddIsCaseInsensitiveOnLookupCasePreservingOnDisplay(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)
	w.Add("#XSHELLZ-Test", "")
	w.Add("#xshellz-test", "") // dup lookup, different case

	snap := w.Snapshot()
	require.Len(t, snap, 1, "case-insensitive dedupe must collapse duplicates")
	assert.Equal(t, "#XSHELLZ-Test", snap[0].Name,
		"first-insert case wins for display")
}

func TestWantedChannelsKeyOverwriteSemantics(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)

	// First insert from upstream echo — no key.
	w.Add("#private", "")
	got, ok := w.Get("#private")
	require.True(t, ok)
	assert.Empty(t, got.Key)

	// Client subsequently joins with a key — the cached entry picks up
	// the credential.
	w.Add("#private", "hunter2")
	got, ok = w.Get("#private")
	require.True(t, ok)
	assert.Equal(t, "hunter2", got.Key)

	// Another upstream echo with no key must NOT clobber the stored
	// credential — otherwise the next reconnect would issue a bare
	// JOIN and trip 475 ERR_BADCHANNELKEY.
	w.Add("#private", "")
	got, ok = w.Get("#private")
	require.True(t, ok)
	assert.Equal(t, "hunter2", got.Key, "empty-key Add must not clobber a stored key")
}

func TestWantedChannelsKeyUpdateAllowsRotation(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)
	w.Add("#private", "old")
	w.Add("#private", "new")
	got, _ := w.Get("#private")
	assert.Equal(t, "new", got.Key,
		"non-empty Add must overwrite — operator rotated the key")
}

func TestWantedChannelsRemove(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels([]string{"#a", "#b", "#c"})
	w.Remove("#b")
	snap := w.Snapshot()
	require.Len(t, snap, 2)
	assert.Equal(t, "#a", snap[0].Name)
	assert.Equal(t, "#c", snap[1].Name, "removal must preserve relative order of survivors")
	assert.Equal(t, 2, w.Len())
}

func TestWantedChannelsRemoveMissingIsNoOp(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels([]string{"#a"})
	w.Remove("#nothere") // no panic, no state change
	assert.Equal(t, 1, w.Len())
}

func TestWantedChannelsOnChangeFiresOnInsertAndRemove(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)
	var calls int
	w.SetOnChange(func() { calls++ })

	w.Add("#a", "")
	w.Add("#b", "")
	w.Remove("#a")
	assert.Equal(t, 3, calls, "fresh insert + insert + remove = 3 fires")
}

func TestWantedChannelsOnChangeFiresOnKeyRotationOnly(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)
	var calls int
	w.SetOnChange(func() { calls++ })

	w.Add("#a", "old") // fresh insert → fires
	w.Add("#a", "old") // same key → no fire
	w.Add("#a", "")    // empty key with existing entry → no fire (preserves stored key)
	w.Add("#a", "new") // key rotation → fires
	assert.Equal(t, 2, calls)
}

func TestWantedChannelsOnChangeNotFiredForSeedEntries(t *testing.T) {
	t.Parallel()
	calls := 0
	// Seed during construction — callback isn't installed yet, so
	// seed entries don't fire. Install the callback *after*
	// construction (the documented contract).
	w := irc.NewWantedChannels([]string{"#a", "#b"})
	w.SetOnChange(func() { calls++ })
	assert.Equal(t, 0, calls)
	// Sanity: a post-seed mutation still fires.
	w.Add("#c", "")
	assert.Equal(t, 1, calls)
}

func TestWantedChannelsOnChangeNotFiredOnNoOpRemove(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)
	var calls int
	w.SetOnChange(func() { calls++ })
	w.Remove("#nothere")
	assert.Equal(t, 0, calls)
}

func TestWantedChannelsConcurrentMutationIsSafe(t *testing.T) {
	t.Parallel()
	w := irc.NewWantedChannels(nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				name := []string{"#a", "#b", "#c", "#d"}[(i+j)%4]
				w.Add(name, "")
				if j%5 == 0 {
					w.Remove(name)
				}
				_ = w.Snapshot()
			}
		}(i)
	}
	wg.Wait()
	// Just want the race detector to confirm no concurrent map writes.
	assert.LessOrEqual(t, w.Len(), 4)
}
