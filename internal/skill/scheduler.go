package skill

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// schedTick is how often the scheduler wakes to evaluate due schedules. A
// 30-second tick bounds cron/interval granularity without busy-spinning.
const schedTick = 30 * time.Second

// Scheduler fires schedule-trigger skills on their cadence. One ticker drives
// every scheduled skill; the set is hot-swappable via ReplaceSkills and the
// loop is goleak-clean — Run returns when its context is cancelled.
type Scheduler struct {
	engine *Engine
	log    *slog.Logger
	now    clock

	mu    sync.Mutex
	items []*scheduled
}

// scheduled is one schedule skill plus its parsed cadence and next-due time.
type scheduled struct {
	skill    Skill
	interval time.Duration // > 0 for interval cadences
	cron     *cronSpec     // non-nil for cron cadences
	nextDue  time.Time
}

// NewScheduler builds a scheduler that fires actions through engine.
func NewScheduler(engine *Engine, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{engine: engine, log: log.With("component", "skill-scheduler"), now: time.Now}
}

// ReplaceSkills atomically swaps the scheduled skill set. Only schedule-kind
// skills are kept; an unparseable schedule is dropped with a log line. Each
// surviving skill's next-due time is computed from now.
func (s *Scheduler) ReplaceSkills(skills []Skill) {
	now := s.now()
	var items []*scheduled
	for _, sk := range skills {
		if sk.Trigger.Kind != KindSchedule {
			continue
		}
		it, err := parseSchedule(sk, now)
		if err != nil {
			s.log.Warn("skipping schedule skill: bad schedule", "skill", sk.Name, "err", err)
			continue
		}
		items = append(items, it)
	}
	s.mu.Lock()
	s.items = items
	s.mu.Unlock()
}

// Run drives the scheduler until ctx is cancelled. Always returns nil
// (cancellation is the normal exit); the error return mirrors the other
// supervised runnables.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(schedTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.tick(ctx, s.now())
		}
	}
}

// tick fires every skill whose next-due time has passed and reschedules it.
func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	s.mu.Lock()
	due := make([]Skill, 0)
	for _, it := range s.items {
		if now.Before(it.nextDue) {
			continue
		}
		due = append(due, it.skill)
		it.nextDue = it.advance(now)
	}
	s.mu.Unlock()
	for _, sk := range due {
		f := renderFields{platform: s.engine.platform, owner: s.engine.owner}
		if len(sk.Trigger.Channels) > 0 {
			f.channel = sk.Trigger.Channels[0]
		}
		s.engine.fire(ctx, sk, f)
	}
	// Auto-lift any timed effects whose deadline has passed (mute/ban/mode
	// with duration_seconds). Runs every tick regardless of due schedules.
	s.engine.SweepLifts(now)
}

// advance computes the next due time after now for this schedule.
func (it *scheduled) advance(now time.Time) time.Time {
	if it.cron != nil {
		return it.cron.next(now)
	}
	return now.Add(it.interval)
}

// parseSchedule parses a skill's schedule into either an interval or a cron
// spec and stamps the first due time.
func parseSchedule(sk Skill, now time.Time) (*scheduled, error) {
	spec := strings.TrimSpace(sk.Trigger.Schedule)
	if spec == "" {
		return nil, fmt.Errorf("empty schedule")
	}
	if d, err := parseInterval(spec); err == nil {
		return &scheduled{skill: sk, interval: d, nextDue: now.Add(d)}, nil
	}
	cron, err := parseCron(spec)
	if err != nil {
		return nil, err
	}
	return &scheduled{skill: sk, cron: cron, nextDue: cron.next(now)}, nil
}

// parseInterval accepts a Go duration ("30m", "1h", "45s"). It rejects
// non-positive and sub-minute cadences so a typo can't busy-fire.
func parseInterval(spec string) (time.Duration, error) {
	d, err := time.ParseDuration(spec)
	if err != nil {
		return 0, err
	}
	if d < time.Minute {
		return 0, fmt.Errorf("interval %s below the one-minute floor", spec)
	}
	return d, nil
}
