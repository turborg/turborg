package irc_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestChannelStateSelfJoinIsIdempotent(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#test")
	s.OnSelfJoin("#test")
	s.OnSelfJoin("#TEST") // case-insensitive

	channels := s.JoinedChannels()
	require.Len(t, channels, 1)
	assert.Equal(t, "#test", channels[0].Name, "Name preserves the original casing of the first join")
}

func TestChannelStatePreservesOriginalCasing(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#XSHELLZ-Test2")
	info := s.Get("#xshellz-test2")
	require.NotNil(t, info)
	assert.Equal(t, "#XSHELLZ-Test2", info.Name)
}

func TestChannelStateSelfPart(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#a")
	s.OnSelfJoin("#b")
	s.OnSelfPart("#a")

	channels := s.JoinedChannels()
	require.Len(t, channels, 1)
	assert.Equal(t, "#b", channels[0].Name)
	assert.Nil(t, s.Get("#a"))
}

func TestChannelStateInsertionOrder(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#z")
	s.OnSelfJoin("#a")
	s.OnSelfJoin("#m")

	channels := s.JoinedChannels()
	names := []string{channels[0].Name, channels[1].Name, channels[2].Name}
	assert.Equal(t, []string{"#z", "#a", "#m"}, names)
}

func TestChannelStateTopic(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.SetTopic("#ch", "hello world")
	s.SetTopicMeta("#ch", "alice", 1700000000)

	info := s.Get("#ch")
	require.NotNil(t, info)
	assert.Equal(t, "hello world", info.Topic)
	assert.True(t, info.TopicSet)
	assert.Equal(t, "alice", info.TopicSetBy)
	assert.Equal(t, int64(1700000000), info.TopicSetAt)
}

func TestChannelStateClearTopic(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.SetTopic("#ch", "something")
	s.ClearTopic("#ch")

	info := s.Get("#ch")
	require.NotNil(t, info)
	assert.Empty(t, info.Topic)
	assert.True(t, info.TopicSet, "ClearTopic records that a topic was observed (empty)")
}

func TestChannelStateTopicOnUnjoinedChannelNoop(t *testing.T) {
	s := irc.NewChannelState()
	s.SetTopic("#nope", "hi")
	s.SetTopicMeta("#nope", "alice", 1)
	s.ClearTopic("#nope")
	assert.Nil(t, s.Get("#nope"))
}

func TestChannelStateNamesReply(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnNamesReply("#ch", []string{"@alice", "+bob", "carol", "%dave", "&eve", "~fred"})
	s.OnNamesEnd("#ch")

	info := s.Get("#ch")
	require.NotNil(t, info)
	assert.True(t, info.NamesComplete)
	assert.Equal(t, "@", info.Members["alice"])
	assert.Equal(t, "+", info.Members["bob"])
	assert.Equal(t, "", info.Members["carol"])
	assert.Equal(t, "%", info.Members["dave"])
	assert.Equal(t, "&", info.Members["eve"])
	assert.Equal(t, "~", info.Members["fred"])
}

// Reproduces the post-netsplit ghost-member bug: a NAMES burst that
// arrives AFTER a previous cycle has already completed must REPLACE
// the member set, not merge into it. Pre-fix, members who QUIT
// during the bot's upstream outage stayed ghosted forever because
// OnNamesReply appended to the stale set.
func TestChannelStateNamesReplyReplacesAfterCycleComplete(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")

	// First NAMES cycle: alice, bob, carol present.
	s.OnNamesReply("#ch", []string{"@alice", "bob", "carol"})
	s.OnNamesEnd("#ch")
	require.True(t, s.Get("#ch").NamesComplete)

	// Simulate the bot losing/rejoining upstream — bob has quit while
	// the bot was disconnected. The new NAMES burst only contains
	// alice + carol.
	s.OnNamesReply("#ch", []string{"@alice", "carol"})
	s.OnNamesEnd("#ch")

	info := s.Get("#ch")
	require.NotNil(t, info)
	assert.True(t, info.NamesComplete)
	assert.Contains(t, info.Members, "alice")
	assert.Contains(t, info.Members, "carol")
	assert.NotContains(t, info.Members, "bob", "bob must be evicted; NAMES is a full snapshot")
}

// ChannelsContaining is used by the connector's QUIT handler to fan
// out a per-channel EventUserLeave before OnMemberQuit wipes the
// nick everywhere. Without it the gateway emits op:part events with
// a nil channel and the SPA's per-channel state can't apply them,
// leaving QUIT'd members ghosted in every channel they were in.
func TestChannelStateChannelsContaining(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#one")
	s.OnSelfJoin("#two")
	s.OnSelfJoin("#three")
	s.OnNamesReply("#one", []string{"alice", "bob"})
	s.OnNamesEnd("#one")
	s.OnNamesReply("#two", []string{"alice"})
	s.OnNamesEnd("#two")
	s.OnNamesReply("#three", []string{"bob"})
	s.OnNamesEnd("#three")

	got := s.ChannelsContaining("alice")
	require.Len(t, got, 2)
	assert.ElementsMatch(t, []string{"#one", "#two"}, got)

	got = s.ChannelsContaining("bob")
	assert.ElementsMatch(t, []string{"#one", "#three"}, got)

	got = s.ChannelsContaining("carol")
	assert.Empty(t, got)

	got = s.ChannelsContaining("")
	assert.Empty(t, got, "empty nick is a no-op")
}

func TestChannelStateSetMemberPrefix(t *testing.T) {
	// Mirrors the bot-side ApplyPrefixModes path: when a live MODE
	// line changes a member's displayed prefix, we need to keep the
	// in-memory ChannelState aligned so the next sendState (fresh WS
	// attach) reflects reality without waiting for NAMES.
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnNamesReply("#ch", []string{"alice", "bob"})
	s.OnNamesEnd("#ch")

	// Promote bob to op.
	s.SetMemberPrefix("#ch", "bob", "@")
	assert.Equal(t, "@", s.Get("#ch").Members["bob"])

	// Demote bob.
	s.SetMemberPrefix("#ch", "bob", "")
	assert.Equal(t, "", s.Get("#ch").Members["bob"])

	// Unknown nick is a no-op (no phantom members invented).
	s.SetMemberPrefix("#ch", "phantom", "@")
	assert.NotContains(t, s.Get("#ch").Members, "phantom")

	// Unknown channel is a no-op.
	s.SetMemberPrefix("#nope", "alice", "@")
	assert.Nil(t, s.Get("#nope"))

	// Empty nick is a no-op (defensive against malformed MODE args).
	s.SetMemberPrefix("#ch", "", "@")
	assert.Equal(t, 2, len(s.Get("#ch").Members))
}

func TestChannelStateNamesAutoJoinIfMissing(t *testing.T) {
	s := irc.NewChannelState()
	s.OnNamesReply("#auto", []string{"alice"})
	info := s.Get("#auto")
	require.NotNil(t, info, "NAMES on unknown channel should auto-create it")
	assert.Equal(t, "", info.Members["alice"])
}

func TestChannelStateMemberJoinPartKick(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnMemberJoin("#ch", "alice")
	s.OnMemberJoin("#ch", "alice") // dup is no-op
	assert.Contains(t, s.Get("#ch").Members, "alice")

	s.OnMemberPart("#ch", "alice")
	assert.NotContains(t, s.Get("#ch").Members, "alice")

	s.OnMemberJoin("#ch", "bob")
	s.OnMemberKick("#ch", "bob")
	assert.NotContains(t, s.Get("#ch").Members, "bob")
}

func TestChannelStateMemberJoinEmptyNickNoop(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnMemberJoin("#ch", "")
	assert.Empty(t, s.Get("#ch").Members)
}

func TestChannelStateMemberQuitNetworkWide(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#a")
	s.OnSelfJoin("#b")
	s.OnMemberJoin("#a", "alice")
	s.OnMemberJoin("#b", "alice")
	s.OnMemberJoin("#a", "bob")

	s.OnMemberQuit("alice")

	assert.NotContains(t, s.Get("#a").Members, "alice")
	assert.NotContains(t, s.Get("#b").Members, "alice")
	assert.Contains(t, s.Get("#a").Members, "bob")
}

func TestChannelStateNickChange(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#a")
	s.OnSelfJoin("#b")
	s.OnNamesReply("#a", []string{"@alice"})
	s.OnNamesReply("#b", []string{"+alice"})
	s.OnMemberJoin("#a", "bob")

	s.OnNickChange("alice", "alice2")

	a := s.Get("#a")
	assert.Equal(t, "@", a.Members["alice2"], "mode prefix carries over the rename")
	assert.NotContains(t, a.Members, "alice")
	b := s.Get("#b")
	assert.Equal(t, "+", b.Members["alice2"])
	assert.Contains(t, a.Members, "bob")
}

func TestChannelStateNickChangeUntouchedIfNoMatch(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnMemberJoin("#ch", "alice")

	s.OnNickChange("carol", "dave")

	info := s.Get("#ch")
	assert.Contains(t, info.Members, "alice")
	assert.NotContains(t, info.Members, "dave")
}

func TestChannelStateGetReturnsSnapshotCopy(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnMemberJoin("#ch", "alice")

	snap := s.Get("#ch")
	snap.Members["mutated"] = "@" // try to corrupt
	snap.Topic = "tampered"

	fresh := s.Get("#ch")
	assert.NotContains(t, fresh.Members, "mutated", "snapshots must be deep copies")
	assert.Empty(t, fresh.Topic)
}

func TestChannelStateConcurrentReadersAndWriters(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.OnMemberJoin("#ch", "user")
				s.OnMemberPart("#ch", "user")
			}
			_ = i
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.JoinedChannels()
				_ = s.Get("#ch")
			}
		}()
	}
	wg.Wait()
}
