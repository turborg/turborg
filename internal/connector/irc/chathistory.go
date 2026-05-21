package irc

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/messages"
)

// chathistoryMaxLimit caps how many entries a single CHATHISTORY
// query can return. Mirrors common IRCv3 server behavior (Soju /
// ergo default to 100; we go to 200 to match the welcome-replay
// depth — same conceptual "one screen of backfill" unit).
const chathistoryMaxLimit = 200

// chathistorySubBefore returns messages strictly older than the
// selector; chathistoryLatest returns the latest N when the selector
// is `*` (or older-than selector when a real selector is given,
// equivalent to BEFORE in that case).
//
// Subcommands we don't implement yet — AFTER, AROUND, BETWEEN,
// TARGETS — return FAIL CHATHISTORY UNKNOWN_COMMAND so clients fall
// back gracefully. The set we ship covers the scroll-up case all
// known clients actually use.
const (
	chathistorySubBefore = "BEFORE"
	chathistorySubLatest = "LATEST"
)

// handleChathistory parses and dispatches an IRCv3
// `CHATHISTORY <sub> <target> <selector> <limit>` command.
//
// Selectors supported: `timestamp=<ISO8601>` and `*` (LATEST only).
// `msgid=<id>` is rejected with FAIL INVALID_PARAMS — the store
// doesn't yet expose msgid-keyed lookups; adding them is a follow-up
// once a consumer needs it.
//
// Response shape per IRCv3:
//
//	BATCH +<id> chathistory <target>
//	  @batch=<id>;time=<ISO> :nick!u@h PRIVMSG <target> :text
//	  ...
//	BATCH -<id>
//
// On error: `FAIL CHATHISTORY <code> [<context...>] :<message>`.
func (b *Bouncer) handleChathistory(client *BouncerClient, msg Message) {
	if len(msg.Params) < 4 {
		sendChathistoryFail(client, "NEED_MORE_PARAMS", msg.Command,
			"Need <subcommand> <target> <selector> <limit>")
		return
	}
	sub := strings.ToUpper(msg.Params[0])
	target := msg.Params[1]
	selector := msg.Params[2]
	limitStr := msg.Params[3]

	if target == "" || !startsWithChannelSigil(target) {
		sendChathistoryFail(client, "INVALID_TARGET", target,
			"Channel target required for CHATHISTORY")
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		sendChathistoryFail(client, "INVALID_PARAMS", target,
			"Limit must be a positive integer")
		return
	}
	if limit > chathistoryMaxLimit {
		limit = chathistoryMaxLimit
	}

	var before time.Time
	switch sub {
	case chathistorySubBefore:
		ts, ok := parseChathistorySelector(selector)
		if !ok {
			sendChathistoryFail(client, "INVALID_PARAMS", target,
				"Selector must be timestamp=<ISO8601>")
			return
		}
		before = ts
	case chathistorySubLatest:
		// `*` selector means "no lower bound, last N" — equivalent to
		// passing the zero time to store.Recent. A concrete selector
		// behaves like BEFORE for ordering purposes (older-than).
		if selector != "*" {
			ts, ok := parseChathistorySelector(selector)
			if !ok {
				sendChathistoryFail(client, "INVALID_PARAMS", target,
					"Selector must be * or timestamp=<ISO8601>")
				return
			}
			before = ts
		}
	default:
		sendChathistoryFail(client, "UNKNOWN_COMMAND", sub,
			"Subcommand not implemented")
		return
	}

	store := b.currentMessageStore()
	if store == nil {
		// Empty result is a valid response — equivalent to "no history
		// available". Don't FAIL just because the operator hasn't
		// configured a store.
		b.writeChathistoryBatch(client, target, nil)
		return
	}

	out, err := store.Recent(context.Background(), target, before, limit)
	if err != nil {
		sendChathistoryFail(client, "MESSAGE_ERROR", target,
			"history lookup failed")
		return
	}
	// store.Recent returns newest-first; CHATHISTORY responses are
	// emitted oldest-first inside the batch so clients render the
	// conversation in chronological order.
	reverseMessages(out)
	b.writeChathistoryBatch(client, target, out)
}

func (b *Bouncer) writeChathistoryBatch(client *BouncerClient, target string, msgs []messages.Message) {
	useBatch := client.hasCap("batch")
	tagTime := client.hasCap("server-time")

	var batchID string
	if useBatch {
		batchID = newBatchID()
		_ = client.sendLine("BATCH +" + batchID + " chathistory " + target)
	}
	for _, m := range msgs {
		line := b.formatMessageForReplay(m)
		_ = client.sendLine(decorateReplayLine(loggedLine{line: line, ts: m.TS}, tagTime, batchID))
	}
	if useBatch {
		_ = client.sendLine("BATCH -" + batchID)
	}
}

// parseChathistorySelector accepts `timestamp=<ISO8601>` and returns
// the parsed time. Future selector forms (msgid=, etc.) plug in here.
func parseChathistorySelector(selector string) (time.Time, bool) {
	const prefix = "timestamp="
	if !strings.HasPrefix(selector, prefix) {
		return time.Time{}, false
	}
	raw := selector[len(prefix):]
	// IRCv3 says ISO8601; in practice that's RFC3339(Nano).
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02T15:04:05.000Z", raw); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02T15:04:05Z", raw); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func sendChathistoryFail(client *BouncerClient, code, context, message string) {
	if context == "" {
		context = "*"
	}
	_ = client.sendLine("FAIL CHATHISTORY " + code + " " + context + " :" + message)
}
