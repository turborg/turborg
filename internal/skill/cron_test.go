package skill

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCronErrors(t *testing.T) {
	for _, spec := range []string{
		"* * * *",     // too few fields
		"60 * * * *",  // minute out of range
		"* 24 * * *",  // hour out of range
		"a * * * *",   // non-numeric
		"*/0 * * * *", // bad step
		"5-1 * * * *", // reversed range
		"1- * * * *",  // bad range
	} {
		_, err := parseCron(spec)
		require.Error(t, err, "spec %q must fail", spec)
	}
}

func TestParseCronNextEveryFiveMinutes(t *testing.T) {
	c, err := parseCron("*/5 * * * *")
	require.NoError(t, err)
	from := time.Date(2026, 1, 1, 10, 2, 30, 0, time.UTC)
	got := c.next(from)
	assert.Equal(t, time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC), got)
}

func TestParseCronNextDailyAtHour(t *testing.T) {
	c, err := parseCron("0 9 * * *")
	require.NoError(t, err)
	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	got := c.next(from)
	assert.Equal(t, time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC), got)
}

func TestParseCronListAndRange(t *testing.T) {
	c, err := parseCron("0,30 9-17 * * 1-5")
	require.NoError(t, err)
	// Saturday 2026-01-03 10:05 → next is Monday 2026-01-05 09:00.
	from := time.Date(2026, 1, 3, 10, 5, 0, 0, time.UTC)
	got := c.next(from)
	assert.Equal(t, time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), got)
}

func TestCronMatchesDomOrDow(t *testing.T) {
	// Both dom and dow restricted → match on either (cron convention).
	c, err := parseCron("0 0 1 * 1")
	require.NoError(t, err)
	// 2026-06-01 is a Monday → matches via both. Pick a plain Monday.
	assert.True(t, c.matches(time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)), "a Monday matches via dow")
	assert.True(t, c.matches(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)), "the 1st matches via dom")
	assert.False(t, c.matches(time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)), "a non-1st Tuesday matches neither")
}
