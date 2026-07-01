package skill

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
)

// newEngine builds an engine with an unrestricted skill cap and a captured bus.
func newEngine(t *testing.T, o Options) (*Engine, *agent.EventBus) {
	t.Helper()
	if o.MaxSkills == 0 {
		o.MaxSkills = -1
	}
	e := NewEngine(o)
	bus := agent.NewEventBus(nil)
	e.Subscribe(bus)
	return e, bus
}

func msgEvent(channel, sender, text string) *agent.Event {
	return &agent.Event{
		Type:   agent.EventMessage,
		Fields: map[string]any{"envelope": agent.NewInbound("irc", channel, sender, text)},
	}
}

func TestEngineMatchStaticReply(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{
		Name:    "hi",
		Trigger: Trigger{Kind: KindMatch, Match: `(?i)hello`},
		Action:  Action{Type: TypeStatic, Template: "hey {user}"},
	}})
	bus.Publish(context.Background(), msgEvent("#room", "alice", "well Hello there"))
	assert.Equal(t, []string{"say #room hey alice"}, act.snapshot())

	act2 := act.snapshot()
	bus.Publish(context.Background(), msgEvent("#room", "bob", "nope"))
	assert.Len(t, act2, 1, "a non-matching message fires nothing")
}

func TestEngineChannelScoping(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{
		Name:    "ops-only",
		Trigger: Trigger{Kind: KindMatch, Match: ".*", Channels: []string{"#ops"}},
		Action:  Action{Type: TypeStatic, Template: "seen"},
	}})
	bus.Publish(context.Background(), msgEvent("#general", "a", "x"))
	assert.Empty(t, act.snapshot(), "out-of-scope channel is ignored")
	bus.Publish(context.Background(), msgEvent("#ops", "a", "x"))
	assert.Equal(t, []string{"say #ops seen"}, act.snapshot())
}

func TestEngineEventJoinReply(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{
		Name:    "welcome",
		Trigger: Trigger{Kind: KindEvent, Event: EventUserJoin},
		Action:  Action{Type: TypeStatic, Template: "welcome {nick}"},
	}})
	bus.Publish(context.Background(), &agent.Event{
		Type:   agent.EventUserJoin,
		Fields: map[string]any{"channel": "#room", "nick": "newbie"},
	})
	assert.Equal(t, []string{"say #room welcome newbie"}, act.snapshot())
}

func TestEngineEffectDirect(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{
		Name:    "nolink",
		Trigger: Trigger{Kind: KindMatch, Match: `http://`},
		Action:  Action{Type: TypeEffect, Effect: &Effect{Action: EffectKick, Reason: "no links"}},
	}})
	bus.Publish(context.Background(), msgEvent("#room", "spammer", "see http://x"))
	assert.Equal(t, []string{"kick #room spammer no links"}, act.snapshot())
}

func TestEngineEffectFloodThresholds(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{
		Name:    "flood",
		Trigger: Trigger{Kind: KindMatch, Match: ".*"},
		Action:  Action{Type: TypeEffect, Effect: &Effect{Thresholds: &Thresholds{Warn: 2, Kick: 3}}},
	}})
	// 1st: below warn → nothing. 2nd: warn (notice). 3rd: kick.
	bus.Publish(context.Background(), msgEvent("#r", "alice", "1"))
	bus.Publish(context.Background(), msgEvent("#r", "alice", "2"))
	bus.Publish(context.Background(), msgEvent("#r", "alice", "3"))
	calls := act.snapshot()
	require.Len(t, calls, 2)
	assert.Contains(t, calls[0], "notice #r")
	assert.Equal(t, "kick #r alice ", calls[1])
}

func TestEngineModerationAuditEvent(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	var mu sync.Mutex
	var got *agent.Event
	bus.Subscribe(agent.EventModeration, func(_ context.Context, ev *agent.Event) {
		mu.Lock()
		got = ev
		mu.Unlock()
	})
	e.ReplaceSkills([]Skill{{
		Name:    "ban-it",
		Trigger: Trigger{Kind: KindMatch, Match: `badword`},
		Action:  Action{Type: TypeEffect, Effect: &Effect{Action: EffectBan}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "troll", "badword"))
	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, got)
	assert.Equal(t, "ban", got.Fields["action"])
	assert.Equal(t, "troll", got.Fields["target"])
}

func TestEngineLLMClassifyMapsSeverity(t *testing.T) {
	prov := &fakeProvider{resp: `here is the verdict {"severity":3,"action":"ban","reason":"abuse"} done`}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{
		Name:    "ai-mod",
		Trigger: Trigger{Kind: KindMatch, Match: `.*`},
		Action:  Action{Type: TypeLLMClassify, Effect: &Effect{Thresholds: &Thresholds{Warn: 1, Mute: 2, Ban: 3}}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "troll", "something nasty"))
	require.Equal(t, []string{"ban #r troll"}, act.snapshot())
}

func TestEngineLLMClassifyDegradesWithoutProvider(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act}) // no provider
	e.ReplaceSkills([]Skill{{
		Name:    "ai-mod",
		Trigger: Trigger{Kind: KindMatch, Match: `.*`},
		Action:  Action{Type: TypeLLMClassify, Effect: &Effect{Thresholds: &Thresholds{Kick: 2}}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot(), "no LLM → fail safe, no punitive action")
}

func TestEngineLLMClassifyBudgetExhaustedNoAction(t *testing.T) {
	prov := &fakeProvider{err: llm.ErrBudgetExhausted}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{
		Name:    "ai-mod",
		Trigger: Trigger{Kind: KindMatch, Match: `.*`},
		Action:  Action{Type: TypeLLMClassify, Effect: &Effect{Thresholds: &Thresholds{Kick: 2}}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot())
}

func TestEngineLLMClassifyUnparseable(t *testing.T) {
	prov := &fakeProvider{resp: "I cannot comply"}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{
		Name:    "ai-mod",
		Trigger: Trigger{Kind: KindMatch, Match: `.*`},
		Action:  Action{Type: TypeLLMClassify, Effect: &Effect{Thresholds: &Thresholds{Kick: 2}}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "x"))
	assert.Empty(t, act.snapshot())
}

func TestEngineLLMAction(t *testing.T) {
	prov := &fakeProvider{resp: "a   tidy   reply\n"}
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Provider: prov})
	e.ReplaceSkills([]Skill{{
		Name:    "ai-reply",
		Trigger: Trigger{Kind: KindMatch, Match: `\?$`},
		Action:  Action{Type: TypeLLM, Template: "answer: {text}", Instructions: "be terse"},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "u", "what time is it?"))
	assert.Equal(t, []string{"say #r a tidy reply"}, act.snapshot())
	assert.Equal(t, "answer: what time is it?", prov.lastPrompt())
}

func TestEngineWebhook(t *testing.T) {
	var mu sync.Mutex
	var gotMethod, gotURL string
	var gotBody []byte
	e, bus := newEngine(t, Options{Post: func(_ context.Context, method, url string, body []byte) error {
		mu.Lock()
		gotMethod, gotURL, gotBody = method, url, body
		mu.Unlock()
		return nil
	}})
	e.ReplaceSkills([]Skill{{
		Name:    "to-flow",
		Trigger: Trigger{Kind: KindEvent, Event: EventUserJoin},
		Action:  Action{Type: TypeWebhook, Webhook: "https://flow.example/hook", Template: "joined {nick}"},
	}})
	bus.Publish(context.Background(), &agent.Event{
		Type:   agent.EventUserJoin,
		Fields: map[string]any{"channel": "#room", "nick": "newbie"},
	})
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "https://flow.example/hook", gotURL)
	assert.Equal(t, http.MethodPost, gotMethod, "skill webhook posts via POST")
	assert.Contains(t, string(gotBody), `"user":"newbie"`)
	assert.Contains(t, string(gotBody), `"message":"joined newbie"`)
}

func TestEngineStoreHelpers(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{
		{Name: "set", Trigger: Trigger{Kind: KindMatch, Match: `^!setmotd`}, Action: Action{Type: TypeStatic, Template: "{setvar:motd,hello world}saved"}},
		{Name: "set", Trigger: Trigger{Kind: KindMatch, Match: `^!getmotd`}, Action: Action{Type: TypeStatic, Template: "motd: {getvar:motd}"}},
	})
	bus.Publish(context.Background(), msgEvent("#r", "u", "!setmotd"))
	bus.Publish(context.Background(), msgEvent("#r", "u", "!getmotd"))
	calls := act.snapshot()
	require.Len(t, calls, 2)
	assert.Equal(t, "say #r saved", calls[0])
	assert.Equal(t, "say #r motd: hello world", calls[1])
}

func TestEngineReplaceSkillsCapAndValidation(t *testing.T) {
	e := NewEngine(Options{MaxSkills: 1})
	e.ReplaceSkills([]Skill{
		{Name: "bad", Trigger: Trigger{Kind: KindMatch, Match: `[`}, Action: Action{Type: TypeStatic}}, // bad regex: dropped
		{Name: "m1", Trigger: Trigger{Kind: KindMatch, Match: `a`}, Action: Action{Type: TypeStatic, Template: "x"}},
		{Name: "m2", Trigger: Trigger{Kind: KindMatch, Match: `b`}, Action: Action{Type: TypeStatic, Template: "y"}}, // over cap
		{Name: "ev-bad", Trigger: Trigger{Kind: KindEvent, Event: "NOPE"}, Action: Action{Type: TypeStatic}},         // unknown event
	})
	e.mu.RLock()
	defer e.mu.RUnlock()
	assert.Len(t, e.matches, 1, "bad regex dropped; cap of 1 keeps only the first valid match")
	assert.Equal(t, "m1", e.matches[0].skill.Name)
}

func TestEngineSetMaxSkills(t *testing.T) {
	e := NewEngine(Options{MaxSkills: 0})
	e.SetMaxSkills(-1)
	e.ReplaceSkills([]Skill{{Name: "m", Trigger: Trigger{Kind: KindMatch, Match: `a`}, Action: Action{Type: TypeStatic, Template: "x"}}})
	e.mu.RLock()
	defer e.mu.RUnlock()
	assert.Len(t, e.matches, 1)
}

func TestEngineIgnoresNonEnvelopeMessage(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{Name: "m", Trigger: Trigger{Kind: KindMatch, Match: `.*`}, Action: Action{Type: TypeStatic, Template: "x"}}})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage, Fields: map[string]any{"nope": 1}})
	assert.Empty(t, act.snapshot())
}

func TestParseVerdict(t *testing.T) {
	v, ok := parseVerdict(`prefix {"severity":2,"action":"mute","reason":"x"} suffix`)
	require.True(t, ok)
	assert.Equal(t, 2, v.Severity)
	assert.Equal(t, "mute", v.Action)

	_, ok = parseVerdict("no json here")
	assert.False(t, ok)
	_, ok = parseVerdict(`{not valid}`)
	assert.False(t, ok)

	v, ok = parseVerdict(`{"severity":-5}`)
	require.True(t, ok)
	assert.Equal(t, 0, v.Severity, "negative severity clamps to 0")
}

func TestActionForSeverity(t *testing.T) {
	th := &Thresholds{Warn: 1, Mute: 2, Kick: 3, Ban: 4}
	assert.Equal(t, EffectAction(""), actionForSeverity(0, th))
	assert.Equal(t, EffectNotice, actionForSeverity(1, th))
	assert.Equal(t, EffectMute, actionForSeverity(2, th))
	assert.Equal(t, EffectKick, actionForSeverity(3, th))
	assert.Equal(t, EffectBan, actionForSeverity(5, th))
	assert.Equal(t, EffectAction(""), actionForSeverity(9, nil))
}

func TestApplyEffectModesAndModerationActions(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{
		{Name: "mute", Trigger: Trigger{Kind: KindMatch, Match: `m`}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectMute}}},
		{Name: "op", Trigger: Trigger{Kind: KindMatch, Match: `o`}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectOp}}},
		{Name: "voice", Trigger: Trigger{Kind: KindMatch, Match: `v`}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectVoice}}},
		{Name: "mode", Trigger: Trigger{Kind: KindMatch, Match: `g`}, Action: Action{Type: TypeEffect, Effect: &Effect{Action: EffectMode, Modes: "+m"}}},
		{Name: "notice", Trigger: Trigger{Kind: KindMatch, Match: `n`}, Action: Action{Type: TypeEffect, Template: "rules!", Effect: &Effect{Action: EffectNotice}}},
		{Name: "topic", Trigger: Trigger{Kind: KindMatch, Match: `t`}, Action: Action{Type: TypeEffect, Template: "new topic", Effect: &Effect{Action: EffectTopic}}},
	})
	bus.Publish(context.Background(), msgEvent("#r", "u", "movgnt"))
	calls := act.snapshot()
	assert.Contains(t, calls, "mode #r +q u")
	assert.Contains(t, calls, "op #r u")
	assert.Contains(t, calls, "voice #r u")
	assert.Contains(t, calls, "mode #r +m")
	assert.Contains(t, calls, "notice #r rules!")
	assert.Contains(t, calls, "topic #r new topic")
}

func TestFieldsFromEvent(t *testing.T) {
	kick := fieldsFromEvent(&agent.Event{Type: agent.EventUserKicked, Fields: map[string]any{"channel": "#r", "nick": "victim", "by": "op", "reason": "bye"}})
	assert.Equal(t, "op", kick.user)
	assert.Equal(t, "victim", kick.target)

	nick := fieldsFromEvent(&agent.Event{Type: agent.EventUserNickChange, Fields: map[string]any{"old": "a", "new": "b"}})
	assert.Equal(t, "a", nick.oldNick)
	assert.Equal(t, "b", nick.newNick)

	topic := fieldsFromEvent(&agent.Event{Type: agent.EventTopicChanged, Fields: map[string]any{"channel": "#r", "by": "setter", "topic": "hi"}})
	assert.Equal(t, "setter", topic.user)
	assert.Equal(t, "hi", topic.topic)
}

func TestValidEvent(t *testing.T) {
	assert.True(t, validEvent(EventModeChanged))
	assert.False(t, validEvent("BOGUS"))
}

func TestChannelSetAndAllowed(t *testing.T) {
	assert.Nil(t, channelSet(nil))
	assert.Nil(t, channelSet([]string{"", "  "}))
	set := channelSet([]string{"#Ops"})
	assert.True(t, channelAllowed(set, "#ops"))
	assert.False(t, channelAllowed(set, "#other"))
	assert.True(t, channelAllowed(nil, "#anything"))
}
