package web

import (
	"context"
	"iter"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
)

// stubProvider is a minimal llm.Provider for console tests. Stream yields the
// canned reply (or a canned error, e.g. ErrBudgetExhausted) and counts
// invocations across Ask + Stream.
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
	s.mu.Lock()
	s.calls++
	reply, err := s.reply, s.err
	s.mu.Unlock()
	return func(yield func(string, error) bool) {
		if err != nil {
			yield("", err)
			return
		}
		// Stream in two chunks to exercise multi-delta assembly.
		mid := len(reply) / 2
		for _, chunk := range []string{reply[:mid], reply[mid:]} {
			if chunk == "" {
				continue
			}
			if !yield(chunk, nil) {
				return
			}
		}
	}
}

func (s *stubProvider) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// readUntil reads frames until one has op == want (or times out).
func readUntil(t *testing.T, conn *websocket.Conn, want string) map[string]any {
	t.Helper()
	for i := 0; i < 10; i++ {
		f := readFrame(t, conn)
		if f["op"] == want {
			return f
		}
	}
	t.Fatalf("did not receive an %q frame", want)
	return nil
}

// readStreamedReply assembles a streamed assistant message: it waits for
// message_start, concatenates message_delta text, and returns at message_end.
func readStreamedReply(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	_ = readUntil(t, conn, "message_start")
	var sb strings.Builder
	for i := 0; i < 20; i++ {
		f := readFrame(t, conn)
		switch f["op"] {
		case "message_delta":
			if s, ok := f["text"].(string); ok {
				sb.WriteString(s)
			}
		case "message_end":
			return sb.String()
		}
	}
	t.Fatal("no message_end frame")
	return ""
}

// TestConsoleStreamsFreeTextViaLLM: a non-command message is answered by the
// assistant, streamed token-by-token.
func TestConsoleStreamsFreeTextViaLLM(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	// Seed prior turns so buildChatPrompt feeds real conversation memory.
	ctx := context.Background()
	require.NoError(t, c.store.Submit(ctx, storeMsg("owner", "hi")))
	require.NoError(t, c.store.Submit(ctx, storeMsg("helper", "hello, how can I help?")))
	prov := &stubProvider{reply: "I'm your turborg assistant."}
	c.SetLLMProvider(prov)
	c.SetCommandPrefix("!")

	base := serveConn(t, c, "tenant-a")
	conn := dial(t, base, validToken(t, "tenant-a", "console", "owner"))
	_ = readFrame(t, conn) // state
	_ = readFrame(t, conn) // replay: "hi"
	_ = readFrame(t, conn) // replay: "hello, how can I help?"

	require.NoError(t, conn.Write(context.Background(), websocket.MessageText,
		[]byte(`{"op":"say","text":"what are you"}`)))
	_ = readUntil(t, conn, "message") // user echo (kind user)

	assert.Equal(t, "I'm your turborg assistant.", readStreamedReply(t, conn))
	assert.Equal(t, 1, prov.callCount())
}

// TestConsoleCommandNotSentToLLM: a `!command` message is not answered by the
// LLM (it routes to the agent's command dispatch instead).
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

	assert.Never(t, func() bool { return prov.callCount() > 0 }, 300*time.Millisecond, 30*time.Millisecond)
	<-c.Inbound() // drain the command envelope for a clean teardown
}

// TestConsoleBudgetExhaustedSignal: a spent daily LLM budget surfaces as a
// distinct budget_exhausted frame the client can act on.
func TestConsoleBudgetExhaustedSignal(t *testing.T) {
	c, _ := newTestConn(t, Settings{BotNick: "helper", Room: "console"})
	c.SetLLMProvider(&stubProvider{err: llm.ErrBudgetExhausted})

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
