package skill

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseInterval(t *testing.T) {
	d, err := parseInterval("30m")
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, d)

	_, err = parseInterval("30s")
	require.Error(t, err, "sub-minute cadence is rejected")
	_, err = parseInterval("garbage")
	require.Error(t, err)
}

func TestSchedulerReplaceSkillsParses(t *testing.T) {
	s := NewScheduler(NewEngine(Options{}), nil)
	s.ReplaceSkills([]Skill{
		{Name: "interval", Trigger: Trigger{Kind: KindSchedule, Schedule: "1h"}, Action: Action{Type: TypeStatic, Template: "tick"}},
		{Name: "cron", Trigger: Trigger{Kind: KindSchedule, Schedule: "*/5 * * * *"}, Action: Action{Type: TypeStatic, Template: "cron"}},
		{Name: "bad", Trigger: Trigger{Kind: KindSchedule, Schedule: "nonsense"}, Action: Action{Type: TypeStatic}},
		{Name: "notsched", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeStatic}},
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Len(t, s.items, 2, "only valid schedule skills are kept")
}

func TestSchedulerTickFires(t *testing.T) {
	act := &fakeActor{}
	eng := NewEngine(Options{Actor: act})
	s := NewScheduler(eng, nil)
	base := time.Unix(0, 0).UTC()
	s.now = func() time.Time { return base }
	s.ReplaceSkills([]Skill{{
		Name:    "announce",
		Trigger: Trigger{Kind: KindSchedule, Schedule: "30m", Channels: []string{"#room"}},
		Action:  Action{Type: TypeStatic, Template: "scheduled hello"},
	}})

	// Before the first interval — nothing fires.
	s.tick(context.Background(), base.Add(time.Minute))
	assert.Empty(t, act.snapshot())

	// Past the interval — it fires to the scoped channel.
	s.tick(context.Background(), base.Add(31*time.Minute))
	assert.Equal(t, []string{"say #room scheduled hello"}, act.snapshot())

	// Immediately again — not yet re-due.
	s.tick(context.Background(), base.Add(32*time.Minute))
	assert.Len(t, act.snapshot(), 1)
}

func TestSchedulerRunStopsOnCancel(t *testing.T) {
	s := NewScheduler(NewEngine(Options{}), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
