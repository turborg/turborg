package web

import (
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// White-box tests for unexported helpers. The integration tests in
// gateway_test.go exercise these indirectly; here we cover the branches
// they can't easily reach (TS-type fallback in sortByTS, RemoteAddr
// without port in clientIP, header-only token in extractToken).

func TestSortByTSStableInt64(t *testing.T) {
	items := []map[string]any{
		{"ts": int64(3)},
		{"ts": int64(1)},
		{"ts": int64(2)},
	}
	sortByTS(items)
	got := []int64{
		items[0]["ts"].(int64),
		items[1]["ts"].(int64),
		items[2]["ts"].(int64),
	}
	assert.Equal(t, []int64{1, 2, 3}, got)
}

func TestSortByTSFallsBackToInt(t *testing.T) {
	// time.Now().Unix() produces int64, but callers populating the
	// buffer with the older int shape hit the fallback branch. Mix the
	// two so both code paths run.
	items := []map[string]any{
		{"ts": int(30)},
		{"ts": int64(10)},
		{"ts": int(20)},
	}
	sortByTS(items)
	toI64 := func(v any) int64 {
		switch x := v.(type) {
		case int64:
			return x
		case int:
			return int64(x)
		}
		return -1
	}
	got := []int64{toI64(items[0]["ts"]), toI64(items[1]["ts"]), toI64(items[2]["ts"])}
	assert.True(t, sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }))
	assert.Equal(t, []int64{10, 20, 30}, got)
}

func TestSortByTSAlreadyOrdered(t *testing.T) {
	items := []map[string]any{{"ts": int64(1)}, {"ts": int64(2)}, {"ts": int64(3)}}
	sortByTS(items)
	assert.Equal(t, int64(1), items[0]["ts"].(int64))
	assert.Equal(t, int64(3), items[2]["ts"].(int64))
}

func TestSortByTSEmpty(t *testing.T) {
	var items []map[string]any
	sortByTS(items) // must not panic
	assert.Empty(t, items)
}

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

func TestRecordChannelTrimsAtCap(t *testing.T) {
	// White-box direct call so the trim branch is exercised
	// deterministically, no goroutines or EventBus draining involved.
	g := &Gateway{channelLog: map[string][]map[string]any{}}
	for i := 0; i < channelLogCap+50; i++ {
		g.recordChannel(map[string]any{"channel": "#x", "n": i})
	}
	got := g.channelLog["#x"]
	assert.Equal(t, channelLogCap, len(got),
		"recordChannel must cap the per-channel ring at channelLogCap")
	// The newest entries should win — the first kept entry should be
	// index 50 (since we trimmed the first 50).
	first := got[0]["n"].(int)
	assert.Equal(t, 50, first, "trim must drop the oldest entries first")
}

func TestRecordChannelEmptyChannelIsNoop(t *testing.T) {
	g := &Gateway{channelLog: map[string][]map[string]any{}}
	g.recordChannel(map[string]any{"text": "no channel field"})
	g.recordChannel(map[string]any{"channel": ""})
	assert.Empty(t, g.channelLog, "empty channel must not record anything")
}
