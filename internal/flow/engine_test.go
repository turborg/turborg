package flow

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/skill"
)

func newEngine(t *testing.T, o Options) (*Engine, *agent.EventBus) {
	t.Helper()
	if o.MaxFlows == 0 {
		o.MaxFlows = -1
	}
	e := NewEngine(o)
	bus := agent.NewEventBus(nil)
	e.Subscribe(bus)
	return e, bus
}

// branchingFlow: match → if(user ne bot) → true: say; false: stop.
func branchingFlow() Flow {
	return Flow{
		Name:    "greet",
		Trigger: skill.Trigger{Kind: skill.KindMatch, Match: `(?i)hi`},
		Nodes: []Node{
			{ID: "check", Type: "if", Config: map[string]any{"left": "{user}", "op": "ne", "right": "bot"}},
			{ID: "greet", Type: "say", Config: map[string]any{"text": "hello {user}"}},
			{ID: "ignore", Type: "stop"},
		},
		Edges: []Edge{
			{From: "start", To: "check"},
			{From: "check", To: "greet", Port: "true"},
			{From: "check", To: "ignore", Port: "false"},
		},
	}
}

func TestEngineMatchBranchTrue(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{branchingFlow()})
	bus.Publish(context.Background(), msgEvent("#r", "alice", "hi there"))
	assert.Equal(t, []string{"say #r hello alice"}, act.snapshot())
}

func TestEngineMatchBranchFalse(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{branchingFlow()})
	bus.Publish(context.Background(), msgEvent("#r", "bot", "hi"))
	assert.Empty(t, act.snapshot(), "the false branch stops")
}

func TestEngineMatchNoMatch(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{branchingFlow()})
	bus.Publish(context.Background(), msgEvent("#r", "alice", "nope"))
	assert.Empty(t, act.snapshot())
}

func TestEngineChannelScope(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	f := branchingFlow()
	f.Trigger.Channels = []string{"#ops"}
	e.ReplaceFlows([]Flow{f})
	bus.Publish(context.Background(), msgEvent("#general", "alice", "hi"))
	assert.Empty(t, act.snapshot())
	bus.Publish(context.Background(), msgEvent("#ops", "alice", "hi"))
	assert.Equal(t, []string{"say #ops hello alice"}, act.snapshot())
}

func TestEngineEventFlow(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{{
		Name:    "welcome",
		Trigger: skill.Trigger{Kind: skill.KindEvent, Event: "USER_JOIN"},
		Nodes:   []Node{{ID: "s", Type: "say", Config: map[string]any{"text": "welcome {user}"}}},
	}})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventUserJoin, Fields: map[string]any{"channel": "#r", "nick": "newbie"}})
	assert.Equal(t, []string{"say #r welcome newbie"}, act.snapshot())
}

func TestEngineReplaceFlowsValidationAndCap(t *testing.T) {
	e := NewEngine(Options{MaxFlows: 1})
	e.ReplaceFlows([]Flow{
		{Name: "bad-node", Trigger: skill.Trigger{Kind: skill.KindMatch, Match: "a"}, Nodes: []Node{{ID: "x", Type: "no-such-node"}}}, // dropped
		{Name: "bad-regex", Trigger: skill.Trigger{Kind: skill.KindMatch, Match: "["}},                                                // dropped
		{Name: "ok1", Trigger: skill.Trigger{Kind: skill.KindMatch, Match: "a"}},
		{Name: "ok2", Trigger: skill.Trigger{Kind: skill.KindMatch, Match: "b"}}, // over cap
	})
	e.mu.RLock()
	defer e.mu.RUnlock()
	require.Len(t, e.matches, 1)
	assert.Equal(t, "ok1", e.matches[0].flow.Name)
}

func TestEngineSetMaxFlows(t *testing.T) {
	e := NewEngine(Options{MaxFlows: 0})
	e.SetMaxFlows(-1)
	e.ReplaceFlows([]Flow{{Name: "ev", Trigger: skill.Trigger{Kind: skill.KindEvent, Event: "USER_JOIN"}}})
	e.mu.RLock()
	defer e.mu.RUnlock()
	assert.Len(t, e.events, 1)
}

func TestEngineRunOnceSeedsContext(t *testing.T) {
	act := &fakeActor{}
	e := NewEngine(Options{Actor: act, Platform: "irc.x", Owner: "stefan", MaxFlows: -1})
	f := Flow{
		Name:  "ctx",
		Nodes: []Node{{ID: "s", Type: "say", Config: map[string]any{"channel": "#r", "text": "{platform}/{owner}"}}},
	}
	e.RunOnce(context.Background(), f, nil)
	assert.Equal(t, []string{"say #r irc.x/stefan"}, act.snapshot())
}

func TestEngineStepCeiling(t *testing.T) {
	// A 2-node cycle would loop forever without the step ceiling.
	act := &fakeActor{}
	e := NewEngine(Options{Actor: act, MaxFlows: -1})
	f := Flow{
		Name: "loop",
		Nodes: []Node{
			{ID: "a", Type: "set", Config: map[string]any{"fields": map[string]any{"k": "v"}}},
			{ID: "b", Type: "set", Config: map[string]any{"fields": map[string]any{"k": "v"}}},
		},
		Edges: []Edge{{From: "start", To: "a"}, {From: "a", To: "b"}, {From: "b", To: "a"}},
	}
	e.RunOnce(context.Background(), f, Bag{}) // must terminate, not hang
}

func TestEngineNodeErrorContinues(t *testing.T) {
	e := NewEngine(Options{Provider: &fakeProvider{err: errors.New("down")}, Actor: &fakeActor{}, MaxFlows: -1})
	f := Flow{
		Name: "err",
		Nodes: []Node{
			{ID: "x", Type: "llm", Config: map[string]any{"prompt": "q"}},
			{ID: "y", Type: "say", Config: map[string]any{"channel": "#r", "text": "after"}},
		},
		Edges: []Edge{{From: "start", To: "x"}, {From: "x", To: "y"}},
	}
	e.RunOnce(context.Background(), f, Bag{})
}

func TestEngineRunSkipsUnknownAndMissing(t *testing.T) {
	e := NewEngine(Options{Actor: &fakeActor{}, MaxFlows: -1})
	// Edge points to a missing node id; node id present but unknown type is
	// filtered at ReplaceFlows, so exercise the executor's guards via RunOnce.
	f := Flow{
		Name:  "x",
		Nodes: []Node{{ID: "a", Type: "set", Config: map[string]any{"fields": map[string]any{}}}},
		Edges: []Edge{{From: "start", To: "a"}, {From: "a", To: "ghost"}},
	}
	e.RunOnce(context.Background(), f, Bag{})
}

func TestEngineIgnoresNonEnvelope(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{branchingFlow()})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage, Fields: map[string]any{}})
	assert.Empty(t, act.snapshot())
}

func TestEngineOnUserEventNoFlows(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{branchingFlow()}) // only a match flow
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventTopicChanged, Fields: map[string]any{"channel": "#r", "by": "u"}})
	assert.Empty(t, act.snapshot())
}

func TestBagFromEventVariants(t *testing.T) {
	k := bagFromEvent(&agent.Event{Type: agent.EventUserKicked, Fields: map[string]any{"channel": "#r", "nick": "v", "by": "op"}})
	assert.Equal(t, "op", k["user"])
	assert.Equal(t, "v", k["target"])
	n := bagFromEvent(&agent.Event{Type: agent.EventUserNickChange, Fields: map[string]any{"old": "a", "new": "b"}})
	assert.Equal(t, "a", n["old"])
	tp := bagFromEvent(&agent.Event{Type: agent.EventTopicChanged, Fields: map[string]any{"by": "s"}})
	assert.Equal(t, "s", tp["user"])
}

func TestChannelHelpers(t *testing.T) {
	assert.Nil(t, channelSet(nil))
	assert.Nil(t, channelSet([]string{" "}))
	set := channelSet([]string{"#Ops"})
	assert.True(t, channelAllowed(set, "#ops"))
	assert.False(t, channelAllowed(set, "#x"))
	assert.True(t, channelAllowed(nil, "#x"))
}

func TestDefaultPost(t *testing.T) {
	var gotMethod string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	_, err := defaultPost(context.Background(), "", srv.URL, nil, []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod, "empty method defaults to POST")
	assert.Equal(t, `{}`, string(gotBody))
	// A GET carries no request body.
	_, err = defaultPost(context.Background(), http.MethodGet, srv.URL, nil, []byte(`{"ignored":true}`))
	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Empty(t, gotBody, "GET sends no body")
	_, err = defaultPost(context.Background(), http.MethodDelete, srv.URL, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, http.MethodDelete, gotMethod)
	_, err = defaultPost(context.Background(), http.MethodPost, "://bad", nil, []byte(`{}`))
	require.Error(t, err)
	_, err = defaultPost(context.Background(), http.MethodPost, "http://127.0.0.1:0/x", nil, []byte(`{}`))
	require.Error(t, err)
}

func TestValidateUnknownNodeError(t *testing.T) {
	err := validate(Flow{Nodes: []Node{{ID: "a", Type: "ghost"}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
}

func TestEngineWebhookFlowEndToEnd(t *testing.T) {
	var got []byte
	e, bus := newEngine(t, Options{Post: func(_ context.Context, _, _ string, _ map[string]string, b []byte) ([]byte, error) {
		got = b
		return nil, nil
	}})
	e.ReplaceFlows([]Flow{{
		Name:    "to-flow",
		Trigger: skill.Trigger{Kind: skill.KindEvent, Event: "USER_JOIN"},
		Nodes:   []Node{{ID: "w", Type: "webhook", Config: map[string]any{"url": "https://h"}}},
	}})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventUserJoin, Fields: map[string]any{"channel": "#r", "nick": "newbie"}})
	assert.Contains(t, string(got), `"user":"newbie"`)
}

// commandFlow: a !cryptoprice command trigger that says a line.
func commandFlow() Flow {
	return Flow{
		Name:    "Crypto price",
		Trigger: skill.Trigger{Kind: skill.KindCommand, Command: "cryptoprice"},
		Nodes: []Node{
			{ID: "say", Type: "say", Config: map[string]any{"channel": "{channel}", "text": "top prices for {user}"}},
		},
		Edges: []Edge{{From: "start", To: "say"}},
	}
}

func TestEngineCommandDispatch(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Prefix: "!"})
	e.ReplaceFlows([]Flow{commandFlow()})
	bus.Publish(context.Background(), msgEvent("#r", "alice", "!cryptoprice"))
	assert.Equal(t, []string{"say #r top prices for alice"}, act.snapshot())
}

func TestEngineCommandArgsAndCase(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Prefix: "!"})
	e.ReplaceFlows([]Flow{{
		Name:    "echo",
		Trigger: skill.Trigger{Kind: skill.KindCommand, Command: "echo"},
		Nodes:   []Node{{ID: "s", Type: "say", Config: map[string]any{"channel": "{channel}", "text": "got: {args}"}}},
		Edges:   []Edge{{From: "start", To: "s"}},
	}})
	// Prefix + mixed case + trailing args; word match is case-insensitive.
	bus.Publish(context.Background(), msgEvent("#r", "bob", "!ECHO hello world"))
	assert.Equal(t, []string{"say #r got: hello world"}, act.snapshot())
}

func TestEngineCommandNoPrefixIgnored(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Prefix: "!"})
	e.ReplaceFlows([]Flow{commandFlow()})
	// Same word without the prefix must not fire the command flow.
	bus.Publish(context.Background(), msgEvent("#r", "alice", "cryptoprice"))
	assert.Empty(t, act.snapshot())
}

func TestEngineCommandChannelScope(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Prefix: "!"})
	scoped := commandFlow()
	scoped.Trigger.Channels = []string{"#allowed"}
	e.ReplaceFlows([]Flow{scoped})
	bus.Publish(context.Background(), msgEvent("#other", "alice", "!cryptoprice"))
	assert.Empty(t, act.snapshot(), "command flow scoped to another channel does not fire")
	bus.Publish(context.Background(), msgEvent("#allowed", "alice", "!cryptoprice"))
	assert.Equal(t, []string{"say #allowed top prices for alice"}, act.snapshot())
}

func TestEngineCommandEmptyWordSkipped(t *testing.T) {
	act := &fakeActor{}
	e, bus := newEngine(t, Options{Actor: act, Prefix: "!"})
	e.ReplaceFlows([]Flow{{
		Name:    "no-word",
		Trigger: skill.Trigger{Kind: skill.KindCommand, Command: "  "},
		Nodes:   []Node{{ID: "s", Type: "say", Config: map[string]any{"channel": "{channel}", "text": "x"}}},
		Edges:   []Edge{{From: "start", To: "s"}},
	}})
	bus.Publish(context.Background(), msgEvent("#r", "alice", "! "))
	assert.Empty(t, act.snapshot())
}
