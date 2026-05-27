package irc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

// Service/user NOTICEs (NickServ replies, ChanServ, a user's /notice) and CTCP
// notices (VERSION/PING/TIME replies) must all reach the web gateway so the web
// UI shows them — an attached IRC client (the bouncer path) already does. CTCP
// notices surface decoded (delimiters stripped, "[CTCP]"-tagged).
func TestServiceAndCTCPNoticesReachGateway(t *testing.T) {
	fs, _, a, td := driveConnector(t, &irc.Settings{})
	defer td()

	var mu sync.Mutex
	var notices []string
	a.Events.Subscribe(agent.EventServerNotice, func(_ context.Context, ev *agent.Event) {
		if ev.Fields["kind"] != "notice" {
			return
		}
		text, _ := ev.Fields["text"].(string)
		mu.Lock()
		notices = append(notices, text)
		mu.Unlock()
	})

	// NickServ reply — service hostmask prefix → must surface, with sender.
	require.NoError(t, fs.SendLine(":NickServ!NickServ@services.libera.chat NOTICE turborg :This nickname is registered."))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, n := range notices {
			if n == "NickServ: This nickname is registered." {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "service NOTICE should reach the gateway with sender")

	// CTCP notice (e.g. a VERSION reply) → must surface, decoded + tagged.
	require.NoError(t, fs.SendLine(":bob!u@h NOTICE turborg :\x01VERSION turbo 1.0\x01"))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, n := range notices {
			if n == "bob: [CTCP] VERSION turbo 1.0" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "CTCP notice should reach the gateway, decoded")
}
