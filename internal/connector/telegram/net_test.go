package telegram

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// stubBotServer answers getMe with a known bot identity and every other Bot API
// method (getUpdates) with an empty OK result, so Start + the Run long-poll loop
// run against a local endpoint instead of api.telegram.org.
func stubBotServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/getMe") {
			_, _ = io.WriteString(w, `{"ok":true,"result":{"id":42,"is_bot":true,"first_name":"bot","username":"turbobot"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true,"result":[]}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestStartRunStopViaStubEndpoint(t *testing.T) {
	srv := stubBotServer(t)
	old := newBotAPI
	newBotAPI = func(token string) (*tgbotapi.BotAPI, error) {
		return tgbotapi.NewBotAPIWithAPIEndpoint(token, srv.URL+"/bot%s/%s")
	}
	t.Cleanup(func() { newBotAPI = old })

	c := New(&Settings{Token: "t"}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	require.NoError(t, c.Start(context.Background()), "Start builds the client via the stub endpoint")
	assert.Equal(t, int64(42), c.selfID)
	assert.Equal(t, "turbobot", c.selfName)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	time.Sleep(75 * time.Millisecond) // let the update loop poll at least once
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "Run returns cleanly on ctx cancel")
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}
}
