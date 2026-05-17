package runtime_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/statepush"
)

// captureServer records every PUT body the test server received.
type captureServer struct {
	mu     sync.Mutex
	bodies [][]byte
	paths  []string
	auths  []string
}

func (c *captureServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.bodies = append(c.bodies, body)
		c.paths = append(c.paths, r.URL.Path)
		c.auths = append(c.auths, r.Header.Get("Authorization"))
		c.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
}

func (c *captureServer) bodyCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bodies)
}

func (c *captureServer) lastSnapshot() (statepush.Snapshot, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.bodies) == 0 {
		return statepush.Snapshot{}, ""
	}
	var snap statepush.Snapshot
	_ = json.Unmarshal(c.bodies[len(c.bodies)-1], &snap)
	return snap, c.paths[len(c.paths)-1]
}

// TestStatePushWiringEndToEnd verifies the full flow: runtime.Build
// constructs an emitter, wires it to the IRC connector's three state
// sources (state machine, wanted-channels, preferred-nick), and each
// mutation produces a PUT against the configured webhook URL with
// the bearer auth applied.
func TestStatePushWiringEndToEnd(t *testing.T) {
	cap := &captureServer{}
	server := httptest.NewServer(cap.handler())
	t.Cleanup(server.Close)

	s := &config.Settings{
		CommandPrefix:          "!",
		StateWebhookURL:        server.URL + "/c/abc/state",
		StateWebhookToken:      "tok",
		StateWebhookDebounceMs: 0, // zero = fire immediately
	}
	ircCfg := &irc.Settings{
		Hostname: "fake",
		Nick:     "turborg",
		Channels: []string{"#seeded"},
	}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)
	require.NotNil(t, b.StatePush)
	t.Cleanup(b.StatePush.Stop)

	// Mutate wanted-channels — fires NotifyChange.
	b.IRC.WantedChannels().Add("#runtime-added", "secret")

	require.Eventually(t, func() bool { return cap.bodyCount() >= 1 },
		2*time.Second, 5*time.Millisecond,
		"expected at least one PUT after wanted-channel mutation")

	snap, path := cap.lastSnapshot()
	assert.Equal(t, "/c/abc/state", path,
		"PUT lands at the configured STATE_WEBHOOK_URL path")
	require.Contains(t, snap.Connectors, "irc")
	conn := snap.Connectors["irc"]
	assert.NotEmpty(t, conn.State,
		"snapshot state must reflect the connector's state machine")

	// Check the bearer token reached the server.
	cap.mu.Lock()
	lastAuth := cap.auths[len(cap.auths)-1]
	cap.mu.Unlock()
	assert.Equal(t, "Bearer tok", lastAuth)

	// Trigger a preferred-nick change → another PUT, this time with
	// the queued nick reflected.
	b.IRC.SetPreferredNick("queued-nick")
	require.Eventually(t, func() bool {
		snap, _ := cap.lastSnapshot()
		c, ok := snap.Connectors["irc"]
		return ok && c.Nick == "queued-nick"
	}, 2*time.Second, 5*time.Millisecond)

	// State-machine transition → another PUT, with the new state.
	b.IRC.UpstreamState().Transition(irc.UpstreamStateConnecting)
	require.Eventually(t, func() bool {
		snap, _ := cap.lastSnapshot()
		c, ok := snap.Connectors["irc"]
		return ok && c.State == string(irc.UpstreamStateConnecting)
	}, 2*time.Second, 5*time.Millisecond)
}

// TestStatePushChannelKeyNullableEncoding verifies the wire-level
// distinction between "no key" (encoded as JSON null) and an
// explicit key — the receiver uses this to format JOIN replays.
func TestStatePushChannelKeyNullableEncoding(t *testing.T) {
	cap := &captureServer{}
	server := httptest.NewServer(cap.handler())
	t.Cleanup(server.Close)

	s := &config.Settings{
		CommandPrefix:          "!",
		StateWebhookURL:        server.URL + "/state",
		StateWebhookDebounceMs: 0,
	}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)
	t.Cleanup(b.StatePush.Stop)

	b.IRC.WantedChannels().Add("#nokey", "")
	b.IRC.WantedChannels().Add("#keyed", "hunter2")

	require.Eventually(t, func() bool { return cap.bodyCount() >= 1 },
		2*time.Second, 5*time.Millisecond)

	// The last PUT body should have both channels with the right
	// key encoding.
	cap.mu.Lock()
	lastBody := cap.bodies[len(cap.bodies)-1]
	cap.mu.Unlock()
	assert.Contains(t, string(lastBody), `"name":"#nokey","key":null`)
	assert.Contains(t, string(lastBody), `"name":"#keyed","key":"hunter2"`)
}

// TestStatePushStopReleasesGoroutine confirms the emitter's
// background goroutine terminates when StatePush.Stop fires. A leak
// here would surface as a hung shutdown in production, since the
// CLI's defer Stop chain would never return.
func TestStatePushStopReleasesGoroutine(t *testing.T) {
	cap := &captureServer{}
	server := httptest.NewServer(cap.handler())
	t.Cleanup(server.Close)

	s := &config.Settings{
		CommandPrefix:          "!",
		StateWebhookURL:        server.URL + "/state",
		StateWebhookDebounceMs: 1,
	}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)

	b.IRC.WantedChannels().Add("#x", "")
	b.StatePush.Stop()
}
