package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// White-box tests for unexported helpers. The integration tests in
// gateway_test.go exercise these indirectly; here we cover the branches
// they can't easily reach (RemoteAddr without port in clientIP,
// header-only token in extractToken).

func TestExtractTokenHeaderFallback(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://x/ws", nil)
	r.Header.Set("Authorization", "Bearer hunter2")
	assert.Equal(t, "hunter2", extractToken(r))
}

// TestSessionHeartbeatEngagedVsBare drives sessionHeartbeat directly: an
// engaged session re-asserts "presence" on every tick; a bare session
// (no message sent) never fires; both exit promptly on ctx cancel.
func TestSessionHeartbeatEngagedVsBare(t *testing.T) {
	old := activityHeartbeatInterval
	activityHeartbeatInterval = 5 * time.Millisecond
	t.Cleanup(func() { activityHeartbeatInterval = old })

	var presence atomic.Int32
	g := &Gateway{opts: Options{OnActivity: func(r string) {
		if r == "presence" {
			presence.Add(1)
		}
	}}}

	// Engaged → heartbeats fire.
	engaged := &client{engaged: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); g.sessionHeartbeat(ctx, engaged) }()
	require.Eventually(t, func() bool { return presence.Load() >= 1 },
		time.Second, 5*time.Millisecond, "engaged session must heartbeat")
	cancel()
	<-done

	// Bare (never engaged) → no heartbeat, even across several ticks.
	presence.Store(0)
	bare := &client{engaged: false}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() { defer close(done2); g.sessionHeartbeat(ctx2, bare) }()
	time.Sleep(40 * time.Millisecond)
	cancel2()
	<-done2
	assert.Zero(t, presence.Load(), "bare session must never heartbeat")
}

func TestExtractTokenCaseInsensitiveBearerScheme(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://x/ws", nil)
	r.Header.Set("Authorization", "bearer lowercase")
	assert.Equal(t, "lowercase", extractToken(r))
}

func TestExtractTokenReturnsEmptyWhenAbsent(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://x/ws", nil)
	assert.Equal(t, "", extractToken(r))
}

func TestExtractTokenIgnoresNonBearerAuthScheme(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://x/ws", nil)
	r.Header.Set("Authorization", "Basic ZGV2OmRldg==")
	assert.Equal(t, "", extractToken(r))
}

func TestClientIPSplitsHostPort(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://x/ws", nil)
	r.RemoteAddr = "1.2.3.4:55555"
	assert.Equal(t, "1.2.3.4", clientIP(r))
}

func TestClientIPReturnsRemoteAddrWhenNoPort(t *testing.T) {
	// SplitHostPort fails when the input has no port; fall-through path.
	r, _ := http.NewRequest(http.MethodGet, "http://x/ws", nil)
	r.RemoteAddr = "no-port-here"
	assert.Equal(t, "no-port-here", clientIP(r))
}

func TestMapCopyIndependentOfSource(t *testing.T) {
	src := map[string]any{"a": 1, "b": "two"}
	dst := mapCopy(src)
	dst["a"] = 99
	assert.Equal(t, 1, src["a"], "mutating the copy must not bleed into the source")
	assert.Equal(t, 99, dst["a"])
	assert.Equal(t, "two", dst["b"])
}

// TestSendToRemovesClientOnWriteError drives sendTo's error branch
// deterministically: the client's underlying conn is closed before the
// write, so c.conn.Write returns an error and the client must be dropped
// from the gateway's set. The integration variant races on client-side
// close timing; this white-box test forces the branch every run.
func TestSendToRemovesClientOnWriteError(t *testing.T) {
	// Stand up a trivial WS endpoint so we can obtain a real server-side
	// *websocket.Conn, then close it to guarantee the next write fails.
	connCh := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		connCh <- c
	}))
	defer srv.Close()

	url := "ws" + srv.URL[len("http"):]
	dialConn, _, err := websocket.Dial(context.Background(), url, nil)
	require.NoError(t, err)
	defer dialConn.CloseNow()

	srvConn := <-connCh
	require.NoError(t, srvConn.CloseNow()) // force subsequent Write to fail

	c := &client{conn: srvConn, addr: "test"}
	g := &Gateway{clients: map[*client]struct{}{c: {}}}

	g.sendTo(context.Background(), c, map[string]any{"op": "ping"})

	g.mu.Lock()
	_, present := g.clients[c]
	g.mu.Unlock()
	assert.False(t, present, "client with a broken write must be removed")

	g.metMu.Lock()
	forwarded := g.metrics.messagesForwarded
	g.metMu.Unlock()
	assert.Equal(t, uint64(1), forwarded, "a write attempt still counts as forwarded")
}

func TestStartsWithChannelSigil(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"#channel", true},
		{"&local", true},
		{"+modeless", true},
		{"!safe", true},
		{"alice", false}, // DM target
		{"", false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, startsWithChannelSigil(tc.in), "input=%q", tc.in)
	}
}
