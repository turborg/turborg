package irc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestParsePlainCommand(t *testing.T) {
	m := irc.Parse("PING server")
	assert.Equal(t, "PING", m.Command)
	assert.Equal(t, []string{"server"}, m.Params)
	assert.Empty(t, m.Trailing)
	assert.Empty(t, m.Prefix)
	assert.Empty(t, m.Tags)
}

func TestParseUppercasesCommand(t *testing.T) {
	m := irc.Parse("privmsg #ch :hi")
	assert.Equal(t, "PRIVMSG", m.Command)
}

func TestParseTrimsCRLF(t *testing.T) {
	m := irc.Parse("PING server\r\n")
	assert.Equal(t, "PING server", m.Raw)
}

func TestParseWithPrefix(t *testing.T) {
	m := irc.Parse(":nick!user@host PRIVMSG #ch :hello world")
	assert.Equal(t, "nick!user@host", m.Prefix)
	assert.Equal(t, "PRIVMSG", m.Command)
	assert.Equal(t, []string{"#ch"}, m.Params)
	assert.Equal(t, "hello world", m.Trailing)
}

func TestParseNumericReply(t *testing.T) {
	m := irc.Parse(":server 376 nick :End of MOTD")
	assert.Equal(t, "376", m.Command)
	assert.Equal(t, "server", m.Prefix)
	assert.Equal(t, []string{"nick"}, m.Params)
	assert.Equal(t, "End of MOTD", m.Trailing)
}

func TestParseTrailingWithSpaces(t *testing.T) {
	m := irc.Parse(":nick PRIVMSG #ch :hello   :colon :and more")
	assert.Equal(t, "hello   :colon :and more", m.Trailing,
		"trailing must preserve internal spaces and colons verbatim")
}

func TestParseNoTrailing(t *testing.T) {
	m := irc.Parse("JOIN #ch")
	assert.Equal(t, []string{"#ch"}, m.Params)
	assert.Empty(t, m.Trailing)
}

func TestParseMultipleParams(t *testing.T) {
	m := irc.Parse("KICK #ch alice :bye")
	assert.Equal(t, []string{"#ch", "alice"}, m.Params)
	assert.Equal(t, "bye", m.Trailing)
}

func TestParseTags(t *testing.T) {
	m := irc.Parse("@time=2026-05-13T19:00:00Z;account=alice :nick PRIVMSG #ch :hi")
	assert.Equal(t, "2026-05-13T19:00:00Z", m.Tags["time"])
	assert.Equal(t, "alice", m.Tags["account"])
	assert.Equal(t, "PRIVMSG", m.Command)
}

func TestParseTagWithoutValue(t *testing.T) {
	m := irc.Parse("@solo;keyed=v PRIVMSG #ch :x")
	v, ok := m.Tags["solo"]
	assert.True(t, ok)
	assert.Equal(t, "", v)
	assert.Equal(t, "v", m.Tags["keyed"])
}

func TestParseEmptyTagEntries(t *testing.T) {
	m := irc.Parse("@;a=b; PRIVMSG #ch :x")
	assert.Equal(t, "b", m.Tags["a"])
	assert.Len(t, m.Tags, 1, "empty tag entries must be skipped")
}

func TestParseTagEscapeSequences(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`\:`, ";"},
		{`\s`, " "},
		{`\\`, `\`},
		{`\r`, "\r"},
		{`\n`, "\n"},
		{`\x`, "x"},
		{`a\sb\:c`, "a b;c"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			m := irc.Parse("@k=" + tc.raw + " PING server")
			assert.Equal(t, tc.want, m.Tags["k"])
		})
	}
}

func TestParseTagBackslashAtEnd(t *testing.T) {
	m := irc.Parse(`@k=value\ PING server`)
	assert.Equal(t, `value\`, m.Tags["k"],
		"trailing backslash with no following byte stays literal")
}

func TestParseMalformedReturnsEmptyCommand(t *testing.T) {
	cases := []string{
		"",
		"@",
		":",
		":prefix-only",
		"   ",
		"@tag-only",
	}
	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			m := irc.Parse(tc)
			assert.Empty(t, m.Command,
				"malformed input must yield Command=\"\" (line: %q)", tc)
		})
	}
}

func TestParseSourceNick(t *testing.T) {
	cases := []struct {
		prefix string
		want   string
	}{
		{"alice!~user@host.example", "alice"},
		{"server.example.net", "server.example.net"},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			m := irc.Message{Prefix: tc.prefix}
			assert.Equal(t, tc.want, m.SourceNick())
		})
	}
}

func TestNickFunction(t *testing.T) {
	assert.Equal(t, "alice", irc.Nick("alice!~user@host"))
	assert.Equal(t, "server", irc.Nick("server"))
	assert.Equal(t, "", irc.Nick(""))
}

func TestFormatCommand(t *testing.T) {
	cases := []struct {
		command     string
		params      []string
		trailing    string
		hasTrailing bool
		want        string
	}{
		{"PING", []string{"server"}, "", false, "PING server"},
		{"PRIVMSG", []string{"#ch"}, "hello world", true, "PRIVMSG #ch :hello world"},
		{"QUIT", nil, "bye", true, "QUIT :bye"},
		{"NICK", []string{"alice"}, "", false, "NICK alice"},
		{"USER", []string{"user", "0", "*"}, "real name", true, "USER user 0 * :real name"},
	}
	for _, tc := range cases {
		t.Run(tc.command, func(t *testing.T) {
			got := irc.FormatCommand(tc.command, tc.params, tc.trailing, tc.hasTrailing)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsHandshakeComplete(t *testing.T) {
	assert.True(t, irc.IsHandshakeComplete(irc.RplEndOfMOTD))
	assert.True(t, irc.IsHandshakeComplete(irc.ErrNoMOTD))
	assert.False(t, irc.IsHandshakeComplete(irc.RplWelcome))
	assert.False(t, irc.IsHandshakeComplete(""))
}

func TestParsePrefixOnlyLineAfterTags(t *testing.T) {
	m := irc.Parse("@k=v :prefix")
	assert.Equal(t, "v", m.Tags["k"])
	assert.Equal(t, "prefix", m.Prefix)
	assert.Empty(t, m.Command)
}

func TestParseDegenerateColonAfterPrefix(t *testing.T) {
	// Per RFC, trailing must be introduced by " :" (space-colon). A bare
	// ":trailing" right after the prefix is therefore parsed as a command
	// with one param, not as trailing. The test pins this behavior so a
	// future "cleanup" that rescues the degenerate form gets flagged.
	m := irc.Parse(":prefix :weird args")
	assert.Equal(t, "prefix", m.Prefix)
	assert.Equal(t, ":WEIRD", m.Command, "degenerate input is forgiven, not rescued")
	assert.Equal(t, []string{"args"}, m.Params)
	assert.Empty(t, m.Trailing)
}
