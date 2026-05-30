package irc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
)

const (
	tbSummarizeDefaultLimit = 200
	tbSummarizeSystemPrompt = "Summarize the following IRC conversation concisely. Output a short, readable summary suitable for a single IRC message (max ~400 chars). No markdown."
	tbSummarizeTimeout      = 30 * time.Second
)

// tbHandler holds the dependencies for /tb subcommands. Shared by the
// bouncer (IRC clients) and the web gateway (WS clients).
type tbHandler struct {
	llmMu sync.RWMutex
	llm   llm.Provider

	log *slog.Logger
}

func newTBHandler(log *slog.Logger) *tbHandler {
	if log == nil {
		log = slog.Default()
	}
	return &tbHandler{log: log}
}

func (h *tbHandler) setLLM(p llm.Provider) {
	h.llmMu.Lock()
	defer h.llmMu.Unlock()
	h.llm = p
}

func (h *tbHandler) currentLLM() llm.Provider {
	h.llmMu.RLock()
	defer h.llmMu.RUnlock()
	return h.llm
}

// tbSummarize fetches the last N messages from channel and asks the LLM
// to summarize them. Returns the summary text or an error string for the
// caller to relay to the requesting client.
func (h *tbHandler) tbSummarize(ctx context.Context, channel string, n int, cap int, store messages.Store) (string, error) {
	provider := h.currentLLM()
	if provider == nil {
		return "", fmt.Errorf("no LLM provider configured")
	}
	if store == nil {
		return "", fmt.Errorf("no message history available")
	}
	if cap <= 0 {
		return "", fmt.Errorf("/tb summarize is not available on your plan")
	}
	if n <= 0 {
		n = tbSummarizeDefaultLimit
	}
	if n > cap {
		n = cap
	}

	msgs, err := store.Recent(ctx, channel, time.Time{}, n)
	if err != nil {
		return "", fmt.Errorf("failed to fetch history: %w", err)
	}
	if len(msgs) == 0 {
		return "", fmt.Errorf("no messages in %s to summarize", channel)
	}

	// Recent returns newest-first; reverse to chronological for the LLM.
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	var sb strings.Builder
	for _, m := range msgs {
		fmt.Fprintf(&sb, "<%s> %s\n", m.Nick, m.Text)
	}

	prompt := fmt.Sprintf("Summarize these %d messages from %s:\n\n%s", len(msgs), channel, sb.String())

	ctx, cancel := context.WithTimeout(ctx, tbSummarizeTimeout)
	defer cancel()

	summary, _, err := provider.Ask(ctx, prompt, llm.WithSystem(tbSummarizeSystemPrompt), llm.WithMaxTokens(300))
	if err != nil {
		if errors.Is(err, llm.ErrBudgetExhausted) {
			return "", fmt.Errorf("daily AI token budget spent — resets on a rolling 24h window")
		}
		h.log.Warn("tb summarize LLM error", "channel", channel, "err", err)
		return "", fmt.Errorf("LLM request failed: %w", err)
	}
	return strings.TrimSpace(summary), nil
}
