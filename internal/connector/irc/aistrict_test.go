package irc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/messages"
)

func TestChannelStateIsOperator(t *testing.T) {
	s := irc.NewChannelState()
	s.OnSelfJoin("#ch")
	s.OnNamesReply("#ch", []string{"@alice", "+bob", "carol", "%dave", "&eve", "~fred"})
	s.OnNamesEnd("#ch")

	// op (@), admin (&), owner (~) all count as operator-or-higher.
	for _, nick := range []string{"alice", "eve", "fred"} {
		assert.Truef(t, s.IsOperator("#ch", nick), "%q should be operator", nick)
	}
	// voice (+), halfop (%), and plain members do not.
	for _, nick := range []string{"bob", "dave", "carol"} {
		assert.Falsef(t, s.IsOperator("#ch", nick), "%q should NOT be operator", nick)
	}

	// Channel and nick comparison both fold case.
	assert.True(t, s.IsOperator("#CH", "ALICE"))

	// Unknown nick / channel / empty nick / nil receiver are all false.
	assert.False(t, s.IsOperator("#ch", "nobody"))
	assert.False(t, s.IsOperator("#nope", "alice"))
	assert.False(t, s.IsOperator("#ch", ""))
	var nilState *irc.ChannelState
	assert.False(t, nilState.IsOperator("#ch", "alice"))
}

func TestClientLimitsAIStrictDenyMessage(t *testing.T) {
	// No override → the neutral built-in default.
	var l irc.ClientLimits
	assert.Equal(t, irc.DefaultAIStrictMessage, l.AIStrictDenyMessage())

	// Override wins (lets the control plane supply network-specific wording).
	l.AIStrictMessage = "op consent required here"
	assert.Equal(t, "op consent required here", l.AIStrictDenyMessage())
}

// strictBouncerLimits is the policy the gate tests install: the AI gate on,
// a recognisable notice, and a non-zero summarize cap so the only thing
// standing between the request and the LLM is the +o check.
func strictBouncerLimits() irc.ClientLimits {
	return irc.ClientLimits{
		AIStrict:               true,
		AIStrictMessage:        "op consent required here",
		TBSummarizeMaxMessages: 200,
	}
}

func TestBouncerTBSummarizeAIStrictDeniesNonOp(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	// Bot is present but NOT opped.
	state.OnNamesReply("#test", []string{"turborg", "alice"})
	state.OnNamesEnd("#test")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(strictBouncerLimits())

	store := messages.NewMemoryStore(0)
	_ = store.Submit(context.Background(), messages.Message{
		Channel: "#test", Nick: "alice", Text: "hi", TS: time.Now(),
	})
	b.AttachMessageStore(store)

	conn, r := bouncerClient(t, addr)
	defer conn.Close()
	authSimple(t, conn, r, "hunter2")

	writeLine(t, conn, "TB SUMMARIZE #test")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var gotDeny, gotSummarizing bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "op consent required here") {
			gotDeny = true
		}
		if strings.Contains(line, "Summarizing") {
			gotSummarizing = true
		}
	}
	assert.True(t, gotDeny, "a non-op must be denied with the policy notice")
	assert.False(t, gotSummarizing, "a denied request must not start summarizing")
}

func TestBouncerTBSummarizeAIStrictAllowsOp(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	// Bot holds +o → an op opped it, which is the consent signal.
	state.OnNamesReply("#test", []string{"@turborg", "alice"})
	state.OnNamesEnd("#test")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(strictBouncerLimits())
	b.AttachMessageStore(messages.NewMemoryStore(0))

	conn, r := bouncerClient(t, addr)
	defer conn.Close()
	authSimple(t, conn, r, "hunter2")

	writeLine(t, conn, "TB SUMMARIZE #test")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var gotDeny, gotSummarizing bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "op consent required here") {
			gotDeny = true
		}
		// No LLM is attached, so the goroutine will error afterwards — but
		// "Summarizing" is emitted only once the +o gate has passed.
		if strings.Contains(line, "Summarizing") {
			gotSummarizing = true
			break
		}
	}
	assert.False(t, gotDeny, "an opped bot must not be denied")
	assert.True(t, gotSummarizing, "an opped bot should proceed past the gate")
}
