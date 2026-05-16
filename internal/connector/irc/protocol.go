// Package irc implements an RFC 1459 / 2812 client with IRCv3 tag support.
//
// The protocol parser is forgiving: a malformed line yields a Message with
// Command="" rather than raising, so a single bad input does not crash the
// connector. Callers should treat empty Command as "unparseable, drop".
package irc

import "strings"

// Message is a parsed IRC line.
//
// Wire format: [@tags ][:prefix ]command[ params...][ :trailing]
type Message struct {
	Raw      string
	Tags     map[string]string
	Prefix   string
	Command  string
	Params   []string
	Trailing string
}

// SourceNick returns the nickname portion of Prefix ("nick!user@host" →
// "nick"). For server-origin lines (prefix has no "!"), the full prefix is
// returned unchanged.
func (m Message) SourceNick() string {
	if i := strings.IndexByte(m.Prefix, '!'); i >= 0 {
		return m.Prefix[:i]
	}
	return m.Prefix
}

// IRCv3 tag-value escape sequences. See
// https://ircv3.net/specs/extensions/message-tags#escaping-values
var tagUnescape = map[byte]byte{
	':':  ';',
	's':  ' ',
	'\\': '\\',
	'r':  '\r',
	'n':  '\n',
}

func unescapeTagValue(value string) string {
	if !strings.ContainsRune(value, '\\') {
		return value
	}
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) {
			if r, ok := tagUnescape[value[i+1]]; ok {
				b.WriteByte(r)
			} else {
				b.WriteByte(value[i+1])
			}
			i++
			continue
		}
		b.WriteByte(value[i])
	}
	return b.String()
}

// Parse turns a raw IRC line into a Message. Trailing CR/LF is stripped.
// Malformed input (empty command after prefix, lone "@" or ":", etc.)
// yields a Message with Command="" — callers check that and skip dispatch.
func Parse(line string) Message {
	raw := strings.TrimRight(line, "\r\n")
	remaining := raw
	tags := map[string]string{}
	prefix := ""

	if strings.HasPrefix(remaining, "@") {
		tagSegment := remaining
		if sp := strings.IndexByte(remaining, ' '); sp >= 0 {
			tagSegment = remaining[:sp]
			remaining = strings.TrimLeft(remaining[sp+1:], " ")
		} else {
			remaining = ""
		}
		for _, tag := range strings.Split(tagSegment[1:], ";") {
			if tag == "" {
				continue
			}
			if eq := strings.IndexByte(tag, '='); eq >= 0 {
				tags[tag[:eq]] = unescapeTagValue(tag[eq+1:])
			} else {
				tags[tag] = ""
			}
		}
	}

	if strings.HasPrefix(remaining, ":") {
		if sp := strings.IndexByte(remaining, ' '); sp >= 0 {
			prefix = remaining[1:sp]
			remaining = strings.TrimLeft(remaining[sp+1:], " ")
		} else {
			prefix = remaining[1:]
			remaining = ""
		}
	}

	if remaining == "" {
		return Message{Raw: raw, Tags: tags, Prefix: prefix}
	}

	head := remaining
	trailing := ""
	if idx := strings.Index(remaining, " :"); idx >= 0 {
		head = remaining[:idx]
		trailing = remaining[idx+2:]
	}
	parts := strings.Fields(head)
	if len(parts) == 0 {
		return Message{Raw: raw, Tags: tags, Prefix: prefix, Trailing: trailing}
	}
	return Message{
		Raw:      raw,
		Tags:     tags,
		Prefix:   prefix,
		Command:  strings.ToUpper(parts[0]),
		Params:   parts[1:],
		Trailing: trailing,
	}
}

// FormatCommand assembles a wire-ready line from command + params +
// optional trailing argument. The caller is responsible for not embedding
// CR/LF in any field (IRC has no escape mechanism for those in normal
// commands).
func FormatCommand(command string, params []string, trailing string, hasTrailing bool) string {
	pieces := make([]string, 0, len(params)+2)
	pieces = append(pieces, command)
	pieces = append(pieces, params...)
	if hasTrailing {
		pieces = append(pieces, ":"+trailing)
	}
	return strings.Join(pieces, " ")
}

// Nick extracts the nickname portion of a "nick!user@host" prefix. Kept
// as a top-level function for callers (the IRC connector) that work with
// raw prefix strings without constructing a Message.
func Nick(prefix string) string {
	if i := strings.IndexByte(prefix, '!'); i >= 0 {
		return prefix[:i]
	}
	return prefix
}

// splitIdentHost parses "nick!ident@host" into (ident, host, true). For
// server-origin or malformed prefixes ("server.example.net", "nick",
// "nick!ident-no-host") it returns ("", "", false).
func splitIdentHost(prefix string) (ident, host string, ok bool) {
	bang := strings.IndexByte(prefix, '!')
	if bang < 0 {
		return "", "", false
	}
	rest := prefix[bang+1:]
	at := strings.IndexByte(rest, '@')
	if at < 0 {
		return "", "", false
	}
	return rest[:at], rest[at+1:], true
}
