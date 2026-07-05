package web

import (
	"context"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
)

// stubProvider is a minimal llm.Provider for console tests: Ask returns a canned
// reply (or a canned error, e.g. ErrBudgetExhausted) and counts invocations.
type stubProvider struct {
	mu    sync.Mutex
	calls int
	reply string
	err   error
}

func (s *stubProvider) Model() string { return "stub" }

func (s *stubProvider) Ask(context.Context, string, ...llm.CallOption) (string, llm.Usage, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.reply, llm.Usage{}, s.err
}

func (s *stubProvider) Stream(context.Context, string, ...llm.CallOption) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

func (s *stubProvider) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// readUntil reads frames until one has op == want (or times out).
func readUntil(t *testing.T, conn *websocket.Conn, want string) map[string]any {
	t.Helper()
	for i := 0; i < 8; i++ {
		f := readFrame(t, conn)
		if f["op"] == want {
			return f
		}
	}
	t.Fatalf("did not receive an %q frame", want)
	return nil
}

// TestConsoleAnswersFreeTextViaLLM: a non-command message is answered by the
// assistant as a bot message.
func TestConsoleAnswersFreeTextViaLLM(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	// Seed prior turns so buildChatPrompt feeds real conversation memory
	// (a user line + a bot line → both role branches).
	ctx := context.Background()
	require.NoError(t, c.store.Submit(ctx, storeMsg("owner", "hi")))
	require.NoError(t, c.store.Submit(ctx, storeMsg("helper", "hello, how can I help?")))
	prov := &stubProvider{reply: "I'm your turborg assistant."}
	c.SetLLMProvider(prov)
	c.SetCommandPrefix("!")

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"what are you"}`)))

	// Skip the replayed history frames and the user echo; land on the live bot
	// reply from the LLM.
	bot := readUntil(t, conn, "message")
	for bot["kind"] != "bot" || bot["replayed"] == true {
		bot = readUntil(t, conn, "message")
	}
	assert.Equal(t, "I'm your turborg assistant.", bot["text"])
	assert.Equal(t, "helper", bot["sender"])
	assert.Equal(t, 1, prov.callCount())
}

// TestConsoleCommandNotSentToLLM: a `!command` message is NOT answered by the
// LLM (it goes to the agent's command dispatch instead).
func TestConsoleCommandNotSentToLLM(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	prov := &stubProvider{reply: "should not be called"}
	c.SetLLMProvider(prov)
	c.SetCommandPrefix("!")

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"!btc"}`)))

	// Give any (erroneous) async LLM call time to land, then assert none did.
	assert.Never(t, func() bool { return prov.callCount() > 0 }, 300*time.Millisecond, 30*time.Millisecond)
	<-c.Inbound() // drain the command envelope for a clean teardown
}

// TestConsoleBudgetExhaustedSignal: a spent daily LLM budget surfaces as a
// distinct budget_exhausted frame the client can act on.
func TestConsoleBudgetExhaustedSignal(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	prov := &stubProvider{err: llm.ErrBudgetExhausted}
	c.SetLLMProvider(prov)

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"hello"}`)))

	frame := readUntil(t, conn, "budget_exhausted")
	assert.Contains(t, frame["message"], "budget")
}

// TestConsoleGenericLLMErrorFrame: a non-budget error surfaces a generic error
// frame, not the budget-exhausted signal.
func TestConsoleGenericLLMErrorFrame(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	c.SetLLMProvider(&stubProvider{err: assert.AnError})

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"hello"}`)))

	frame := readUntil(t, conn, "error")
	assert.Contains(t, frame["message"], "unavailable")
}

func storeMsg(nick, text string) messages.Message {
	return messages.Message{Channel: "console", Nick: nick, Text: text, TS: time.Now()}
}
