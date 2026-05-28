package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordedReq struct {
	url  string
	auth string
	body string
}

type recordingDoer struct {
	mu     sync.Mutex
	reqs   []recordedReq
	status int   // 0 → 200
	err    error // when set, Do returns it (records the attempt first)
}

func (d *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	d.mu.Lock()
	d.reqs = append(d.reqs, recordedReq{
		url:  req.URL.String(),
		auth: req.Header.Get("Authorization"),
		body: string(body),
	})
	err := d.err
	status := d.status
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}

func (d *recordingDoer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.reqs)
}

func TestNewActivityAggregatorNilWithoutControlPlane(t *testing.T) {
	require.Nil(t, newActivityAggregator("", "tok", nil))
}

func TestActivityAggregatorBuildsTurborgsActivityURL(t *testing.T) {
	agg := newActivityAggregator("https://cp.example/v1/internal/", "tok", nil)
	require.NotNil(t, agg)
	require.Equal(t, "https://cp.example/v1/internal/turborgs/activity", agg.url)
}

func TestActivityAggregatorFlushPostsDedupedBatch(t *testing.T) {
	agg := newActivityAggregator("https://cp.example/v1/internal", "host-token", nil)
	doer := &recordingDoer{}
	agg.client = doer

	agg.Mark("t1")
	agg.Mark("t2")
	agg.Mark("t1") // duplicate within the window — one entry
	agg.flush(context.Background())

	require.Equal(t, 1, doer.count())
	require.Equal(t, "https://cp.example/v1/internal/turborgs/activity", doer.reqs[0].url)
	require.Equal(t, "Bearer host-token", doer.reqs[0].auth)
	require.Contains(t, doer.reqs[0].body, "t1")
	require.Contains(t, doer.reqs[0].body, "t2")

	// The set is drained on flush — a second flush with no new marks is a no-op.
	agg.flush(context.Background())
	require.Equal(t, 1, doer.count())
}

func TestActivityAggregatorEmptyFlushIsNoop(t *testing.T) {
	agg := newActivityAggregator("https://cp.example", "tok", nil)
	doer := &recordingDoer{}
	agg.client = doer

	agg.flush(context.Background())
	require.Equal(t, 0, doer.count())
}

func TestActivityAggregatorNilReceiverSafe(t *testing.T) {
	var agg *activityAggregator
	require.NotPanics(t, func() {
		agg.Mark("x")
		agg.run(context.Background()) // returns immediately on nil
	})
}

func TestActivityAggregatorRunExitsOnCancel(t *testing.T) {
	agg := newActivityAggregator("https://cp.example", "tok", nil)
	agg.client = &recordingDoer{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { agg.run(ctx); close(done) }()
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not exit on context cancellation")
	}
}

func TestActivityAggregatorRunFlushesOnTick(t *testing.T) {
	agg := newActivityAggregator("https://cp.example", "tok", nil)
	doer := &recordingDoer{}
	agg.client = doer
	agg.interval = 5 * time.Millisecond // tick fast so the ticker branch runs
	agg.Mark("t1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { agg.run(ctx); close(done) }()

	require.Eventually(t, func() bool { return doer.count() >= 1 }, time.Second, 2*time.Millisecond,
		"the ticker should drive at least one flush")
	cancel()
	<-done
}

func TestActivityAggregatorFlushToleratesClientError(t *testing.T) {
	agg := newActivityAggregator("https://cp.example", "tok", nil)
	agg.client = &recordingDoer{err: errors.New("connection refused")}
	agg.Mark("t1")

	require.NotPanics(t, func() { agg.flush(context.Background()) })
}

func TestActivityAggregatorFlushToleratesNon2xx(t *testing.T) {
	agg := newActivityAggregator("https://cp.example", "tok", nil)
	agg.client = &recordingDoer{status: http.StatusInternalServerError}
	agg.Mark("t1")

	require.NotPanics(t, func() { agg.flush(context.Background()) })
}
