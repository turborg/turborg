package skill

import (
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPlaceholders(t *testing.T) {
	f := renderFields{
		user: "alice", channel: "#room", text: "hello", target: "bob",
		reason: "spam", topic: "welcome", modes: "+o", oldNick: "a", newNick: "b",
		platform: "irc.example", owner: "stefan", count: 4,
	}
	got := render("{user}/{nick} {channel}/{room} {text}/{message} {target} {reason} {topic} {modes} {old}>{new} {platform}/{network} {owner} {count}", f)
	assert.Equal(t, "alice/alice #room/#room hello/hello bob spam welcome +o a>b irc.example/irc.example stefan 4", got)
}

func TestRenderClockTokens(t *testing.T) {
	got := render("{date} | {time} | {datetime}", renderFields{})
	parts := strings.Split(got, " | ")
	require.Len(t, parts, 3)
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, parts[0])
	assert.Regexp(t, `^\d{2}:\d{2}:\d{2}$`, parts[1])
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$`, parts[2])
}

func TestExpandHelpers(t *testing.T) {
	assert.Contains(t, []string{"red", "green"}, render("{choice:red,green}", renderFields{}))
	assert.Equal(t, "", render("{choice:}", renderFields{}), "empty choice list yields empty")

	for range 30 {
		n, err := strconv.Atoi(render("{random:6}", renderFields{}))
		require.NoError(t, err)
		assert.GreaterOrEqual(t, n, 1)
		assert.LessOrEqual(t, n, 6)
	}
	assert.Equal(t, "{random:0}", render("{random:0}", renderFields{}), "bad bound left literal")
	assert.Equal(t, "{random:x}", render("{random:x}", renderFields{}))

	shuffled := render("{shuffle:a,b,c}", renderFields{})
	got := strings.Split(shuffled, ",")
	sort.Strings(got)
	assert.Equal(t, []string{"a", "b", "c"}, got)
}
