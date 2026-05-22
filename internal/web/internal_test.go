package web

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
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
