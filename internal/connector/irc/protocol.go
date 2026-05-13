package irc

import "strings"

// Message is a minimal IRC line — enough for the PoC's PING/376/PRIVMSG
// dispatch. The full protocol parser (IRCv3 tags, escape sequences, the
// canonical numeric set) lands in Phase 2 of the port plan.
type Message struct {
	Prefix   string
	Command  string
	Params   []string
	Trailing string
}

func Parse(line string) Message {
	var msg Message
	if strings.HasPrefix(line, ":") {
		sp := strings.SplitN(line[1:], " ", 2)
		msg.Prefix = sp[0]
		if len(sp) < 2 {
			return msg
		}
		line = sp[1]
	}
	if idx := strings.Index(line, " :"); idx >= 0 {
		msg.Trailing = line[idx+2:]
		line = line[:idx]
	}
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return msg
	}
	msg.Command = parts[0]
	msg.Params = parts[1:]
	return msg
}

// Nick extracts the nickname portion of a "nick!user@host" prefix. Returns
// the prefix unchanged if no "!" is present (e.g. server-origin messages).
func Nick(prefix string) string {
	if i := strings.IndexByte(prefix, '!'); i >= 0 {
		return prefix[:i]
	}
	return prefix
}
