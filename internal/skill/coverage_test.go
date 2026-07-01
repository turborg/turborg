package skill

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
)

// erroringActor returns an error from every method so the engine's error
// branches are exercised.
type erroringActor struct{}

func (erroringActor) Say(string, string) error                { return errors.New("boom") }
func (erroringActor) Notice(string, string) error             { return errors.New("boom") }
func (erroringActor) Kick(string, string, string) error       { return errors.New("boom") }
func (erroringActor) Ban(string, string) error                { return errors.New("boom") }
func (erroringActor) Op(string, string) error                 { return errors.New("boom") }
func (erroringActor) Voice(string, string) error              { return errors.New("boom") }
func (erroringActor) Topic(string, string) error              { return errors.New("boom") }
func (erroringActor) Invite(string, string) error             { return errors.New("boom") }
func (erroringActor) SetMode(string, string, ...string) error { return errors.New("boom") }

func TestDefaultPostSuccessAndError(t *testing.T) {
	var gotMethod string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	require.NoError(t, defaultPost(context.Background(), "", srv.URL, []byte(`{}`)))
	assert.Equal(t, http.MethodPost, gotMethod, "empty method defaults to POST")
	// A GET carries no body.
	require.NoError(t, defaultPost(context.Background(), http.MethodGet, srv.URL, []byte(`{"x":1}`)))
	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Empty(t, gotBody, "GET sends no body")

	require.Error(t, defaultPost(context.Background(), http.MethodPost, "http://127.0.0.1:0/nope", []byte(`{}`)))
	require.Error(t, defaultPost(context.Background(), http.MethodPost, "://bad-url", []byte(`{}`)))
}

func TestDoWebhookEmptyURLAndError(t *testing.T) {
	// Empty URL → no-op (no panic).
	e, bus := newEngine(t, Options{})
	e.ReplaceSkills([]Skill{{
		Name:    "noop",
		Trigger: Trigger{Kind: KindMatch, Match: ".*"},
		Action:  Action{Type: TypeWebhook},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x")) // must not panic

	// Post error path is logged, not fatal.
	called := false
	e2, bus2 := newEngine(t, Options{Post: func(context.Context, string, string, []byte) error {
		called = true
		return errors.New("down")
	}})
	e2.ReplaceSkills([]Skill{{
		Name:    "w",
		Trigger: Trigger{Kind: KindMatch, Match: ".*"},
		Action:  Action{Type: TypeWebhook, Webhook: "http://x"},
	}})
	bus2.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.True(t, called)
}

func TestDoReplyEdgeCases(t *testing.T) {
	// Nil actor → no-op.
	e, bus := newEngine(t, Options{})
	e.ReplaceSkills([]Skill{{Name: "r", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeStatic, Template: "hi"}}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x")) // no panic

	// Empty rendered text → no Say.
	act := &fakeActor{}
	e2, bus2 := newEngine(t, Options{Actor: act})
	e2.ReplaceSkills([]Skill{{Name: "blank", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeStatic, Template: "   "}}})
	bus2.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot())

	// Actor error → logged, not fatal.
	e3, bus3 := newEngine(t, Options{Actor: erroringActor{}})
	e3.ReplaceSkills([]Skill{{Name: "e", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeStatic, Template: "hi"}}})
	bus3.Publish(context.Background(), msgEvent("#r", "u", "x"))
}

func TestDoLLMEdgeCases(t *testing.T) {
	// No provider → no-op.
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{Name: "ai", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeLLM, Template: "x"}}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot())

	// Empty template falls back to the message text as the prompt.
	prov := &fakeProvider{resp: "ok"}
	act2 := &fakeActor{}
	e2, bus2 := newEngine(t, Options{Actor: act2, Provider: prov})
	e2.ReplaceSkills([]Skill{{Name: "ai", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeLLM, Template: "  ", Model: "m1"}}})
	bus2.Publish(context.Background(), msgEvent("#r", "u", "the message"))
	assert.Equal(t, "the message", prov.lastPrompt())
	assert.Equal(t, []string{"say #r ok"}, act2.snapshot())

	// Empty response → no Say.
	prov3 := &fakeProvider{resp: "   "}
	act3 := &fakeActor{}
	e3, bus3 := newEngine(t, Options{Actor: act3, Provider: prov3})
	e3.ReplaceSkills([]Skill{{Name: "ai", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeLLM, Template: "x"}}})
	bus3.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act3.snapshot())

	// Provider error → no Say, no crash.
	prov4 := &fakeProvider{err: errors.New("rate limited")}
	act4 := &fakeActor{}
	e4, bus4 := newEngine(t, Options{Actor: act4, Provider: prov4})
	e4.ReplaceSkills([]Skill{{Name: "ai", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeLLM, Template: "x"}}})
	bus4.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act4.snapshot())
}

func TestDoLLMBudgetExhausted(t *testing.T) {
	prov := &fakeProvider{err: llm.ErrBudgetExhausted}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{Name: "ai", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeLLM, Template: "x"}}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot(), "budget exhausted degrades silently")
}

func TestDoClassifyWithModelOverrideAndInstructions(t *testing.T) {
	prov := &fakeProvider{resp: `{"severity":2,"action":"mute","reason":"r"}`}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{
		Name:    "mod",
		Trigger: Trigger{Kind: KindMatch, Match: ".*"},
		Action:  Action{Type: TypeLLMClassify, Instructions: "house rules", Model: "guard-1", Effect: &Effect{Thresholds: &Thresholds{Mute: 2}}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Equal(t, []string{"mode #r +q u"}, act.snapshot())
	assert.Contains(t, prov.system, "house rules")
}

func TestDoClassifyNoThresholds(t *testing.T) {
	prov := &fakeProvider{resp: `{"severity":3}`}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{
		Name:    "mod",
		Trigger: Trigger{Kind: KindMatch, Match: ".*"},
		Action:  Action{Type: TypeLLMClassify, Effect: &Effect{}}, // no thresholds → nothing
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot())
}

func TestFireUnknownActionType(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{Name: "weird", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: "mystery"}}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot())
}

func TestApplyEffectNilActorAndEmptyChannel(t *testing.T) {
	// Nil actor → applyEffect returns early (no panic).
	e, bus := newEngine(t, Options{})
	e.ReplaceSkills([]Skill{{Name: "k", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectKick}}}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))

	// Event with no channel (nick change) → applyEffect no-op on empty channel.
	act := &fakeActor{}
	e2 := NewEngine(Options{Actor: act, MaxSkills: -1})
	bus2 := agent.NewEventBus(nil)
	e2.Subscribe(bus2)
	e2.ReplaceSkills([]Skill{{Name: "k", Trigger: Trigger{Kind: KindEvent, Event: EventUserNickChange}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectKick}}}})
	bus2.Publish(context.Background(), &agent.Event{Type: agent.EventUserNickChange, Fields: map[string]any{"old": "a", "new": "b"}})
	assert.Empty(t, act.snapshot())
}

func TestOnUserEventNoEventSkills(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	// Only a match skill installed; a user event must early-return.
	e.ReplaceSkills([]Skill{{Name: "m", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeStatic, Template: "x"}}})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventTopicChanged, Fields: map[string]any{"channel": "#r", "by": "u"}})
	assert.Empty(t, act.snapshot())
}

func TestSchedulerTickCronAdvance(t *testing.T) {
	act := &fakeActor{}
	s := NewScheduler(NewEngine(Options{Actor: act}), nil)
	base := time.Date(2026, 1, 1, 10, 1, 0, 0, time.UTC)
	s.now = func() time.Time { return base }
	s.ReplaceSkills([]Skill{{
		Name:    "cron",
		Trigger: Trigger{Kind: KindSchedule, Schedule: "*/5 * * * *", Channels: []string{"#r"}},
		Action:  Action{Type: TypeStatic, Template: "tick"},
	}})
	// nextDue is 10:05 → tick at 10:06 fires and advances to 10:10.
	s.tick(context.Background(), time.Date(2026, 1, 1, 10, 6, 0, 0, time.UTC))
	assert.Equal(t, []string{"say #r tick"}, act.snapshot())
	s.mu.Lock()
	next := s.items[0].nextDue
	s.mu.Unlock()
	assert.Equal(t, time.Date(2026, 1, 1, 10, 10, 0, 0, time.UTC), next)
}

func TestPublishModerationNilBus(t *testing.T) {
	// An engine that was never Subscribed has a nil bus; publishModeration must
	// not panic.
	e := NewEngine(Options{Actor: &fakeActor{}, MaxSkills: -1})
	e.ReplaceSkills([]Skill{{Name: "k", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectKick}}}})
	e.onMessage(context.Background(), msgEvent("#r", "u", "x"))
}
