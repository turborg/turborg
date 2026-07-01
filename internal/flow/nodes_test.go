package flow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/skill"
)

func TestTypesCatalogSortedAndComplete(t *testing.T) {
	types := Types()
	names := make([]string, len(types))
	for i, nt := range types {
		names[i] = nt.Name
	}
	for _, want := range []string{"set", "if", "switch", "stop", "say", "notice", "effect", "llm", "webhook", "setvar", "getvar", "incr"} {
		assert.Contains(t, names, want)
	}
	// Sorted by name.
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i])
	}
}

func TestWebhookMethod(t *testing.T) {
	for in, want := range map[string]string{
		"get": "GET", "GET": "GET", " put ": "PUT", "patch": "PATCH",
		"delete": "DELETE", "post": "POST", "": "POST", "weird": "POST",
	} {
		assert.Equal(t, want, webhookMethod(in), "webhookMethod(%q)", in)
	}
}

func TestRenderBag(t *testing.T) {
	bag := Bag{"user": "alice", "count": 3}
	assert.Equal(t, "hi alice (3)", renderBag("hi {user} ({count})", bag))
	assert.Equal(t, "plain", renderBag("plain", bag))
	assert.Equal(t, "", renderBag("{missing}", bag), "unknown key renders empty")
}

func TestEvalCond(t *testing.T) {
	assert.True(t, evalCond("eq", "a", "a"))
	assert.True(t, evalCond("ne", "a", "b"))
	assert.True(t, evalCond("contains", "hello world", "wor"))
	assert.True(t, evalCond("matches", "abc123", `\d+`))
	assert.False(t, evalCond("matches", "x", `[`)) // bad regex
	assert.True(t, evalCond("gt", "5", "3"))
	assert.True(t, evalCond("lt", "2", "9"))
	assert.False(t, evalCond("gt", "x", "3")) // non-numeric
	assert.False(t, evalCond("bogus", "a", "a"))
}

func TestNodeSetIfSwitchStop(t *testing.T) {
	nc := &nodeContext{bag: Bag{"user": "alice"}}

	port, err := nodeSet(context.Background(), Node{Config: map[string]any{"fields": map[string]any{"greeting": "hi {user}"}}}, nc)
	require.NoError(t, err)
	assert.Equal(t, "", port)
	assert.Equal(t, "hi alice", nc.bag["greeting"])

	port, _ = nodeIf(context.Background(), Node{Config: map[string]any{"left": "{user}", "op": "eq", "right": "alice"}}, nc)
	assert.Equal(t, "true", port)
	port, _ = nodeIf(context.Background(), Node{Config: map[string]any{"left": "{user}", "op": "eq", "right": "bob"}}, nc)
	assert.Equal(t, "false", port)

	nc.bag["color"] = "red"
	port, _ = nodeSwitch(context.Background(), Node{Config: map[string]any{"value": "{color}", "cases": []any{"red", "green"}}}, nc)
	assert.Equal(t, "red", port)
	port, _ = nodeSwitch(context.Background(), Node{Config: map[string]any{"value": "{color}", "cases": []any{"green"}}}, nc)
	assert.Equal(t, "default", port)

	port, _ = nodeStop(context.Background(), Node{}, nc)
	assert.Equal(t, "\x00stop", port)
}

func TestNodeActorHandlers(t *testing.T) {
	act := &fakeActor{}
	nc := &nodeContext{actor: act, bag: Bag{"channel": "#r", "user": "bob"}}

	_, _ = nodeSay(context.Background(), Node{Config: map[string]any{"text": "hello {user}"}}, nc)
	_, _ = nodeNotice(context.Background(), Node{Config: map[string]any{"text": "psst"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "kick", "reason": "bye"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "ban"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "mute"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "op"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "voice"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "mode", "modes": "+m"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "topic", "reason": "new"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "bogus"}}, nc)

	calls := act.snapshot()
	assert.Equal(t, []string{
		"say #r hello bob",
		"notice #r psst",
		"kick #r bob bye",
		"ban #r bob",
		"mode #r +q bob",
		"op #r bob",
		"voice #r bob",
		"mode #r +m",
		"topic #r new",
	}, calls)
}

func TestNodeActorNilAndEmpty(t *testing.T) {
	// Nil actor → no-ops.
	nc := &nodeContext{bag: Bag{"channel": "#r"}}
	_, _ = nodeSay(context.Background(), Node{Config: map[string]any{"text": "x"}}, nc)
	_, _ = nodeNotice(context.Background(), Node{Config: map[string]any{"text": "x"}}, nc)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "kick"}}, nc)

	// Actor present but empty channel/text → no call.
	act := &fakeActor{}
	nc2 := &nodeContext{actor: act, bag: Bag{}}
	_, _ = nodeSay(context.Background(), Node{Config: map[string]any{"text": ""}}, nc2)
	_, _ = nodeEffect(context.Background(), Node{Config: map[string]any{"action": "kick"}}, nc2)
	assert.Empty(t, act.snapshot())
}

func TestNodeLLM(t *testing.T) {
	prov := &fakeProvider{resp: "tidy   answer\n"}
	nc := &nodeContext{provider: prov, bag: Bag{"text": "q"}}
	_, err := nodeLLM(context.Background(), Node{Config: map[string]any{"prompt": "answer {text}", "system": "be terse", "model": "m", "into": "result"}}, nc)
	require.NoError(t, err)
	assert.Equal(t, "tidy answer", nc.bag["result"])
	assert.Equal(t, "answer q", prov.prompt)

	// Default "into" key + nil provider.
	nc2 := &nodeContext{provider: &fakeProvider{resp: "ok"}, bag: Bag{}}
	_, _ = nodeLLM(context.Background(), Node{Config: map[string]any{"prompt": "x"}}, nc2)
	assert.Equal(t, "ok", nc2.bag["llm"])

	nc3 := &nodeContext{bag: Bag{}}
	_, err = nodeLLM(context.Background(), Node{Config: map[string]any{"prompt": "x"}}, nc3)
	require.NoError(t, err)
}

func TestNodeWebhookAndVars(t *testing.T) {
	var gotMethod, gotURL string
	var gotBody []byte
	nc := &nodeContext{
		bag:      Bag{"user": "alice", "channel": "#r"},
		store:    skill.NewMemoryStore(),
		flowName: "f1",
		post: func(_ context.Context, method, url string, _ map[string]string, body []byte) ([]byte, error) {
			gotMethod, gotURL, gotBody = method, url, body
			return nil, nil
		},
	}
	_, err := nodeWebhook(context.Background(), Node{Config: map[string]any{"url": "https://h/{channel}"}}, nc)
	require.NoError(t, err)
	assert.Equal(t, "https://h/#r", gotURL)
	assert.Equal(t, "POST", gotMethod, "default method is POST")
	assert.Contains(t, string(gotBody), `"user":"alice"`)

	// An explicit method is upper-cased and passed through; unknown verbs fall
	// back to POST.
	_, err = nodeWebhook(context.Background(), Node{Config: map[string]any{"url": "https://h", "method": "delete"}}, nc)
	require.NoError(t, err)
	assert.Equal(t, "DELETE", gotMethod)
	_, err = nodeWebhook(context.Background(), Node{Config: map[string]any{"url": "https://h", "method": "bogus"}}, nc)
	require.NoError(t, err)
	assert.Equal(t, "POST", gotMethod, "unknown method falls back to POST")

	// A custom body template renders against the bag and is posted verbatim
	// (instead of the whole bag dump).
	_, err = nodeWebhook(context.Background(), Node{Config: map[string]any{"url": "https://h", "body": `{"msg":"{user} in {channel}"}`}}, nc)
	require.NoError(t, err)
	assert.Equal(t, `{"msg":"alice in #r"}`, string(gotBody))

	// setvar then getvar round-trips through the store.
	_, _ = nodeSetvar(context.Background(), Node{Config: map[string]any{"key": "motd", "value": "hello {user}"}}, nc)
	_, _ = nodeGetvar(context.Background(), Node{Config: map[string]any{"key": "motd", "into": "m"}}, nc)
	assert.Equal(t, "hello alice", nc.bag["m"])

	// Default into key for getvar + empty url / nil store no-ops.
	_, _ = nodeGetvar(context.Background(), Node{Config: map[string]any{"key": "motd"}}, nc)
	assert.Equal(t, "hello alice", nc.bag["var"])

	// incr persists a monotonic counter (default step 1, or "by") and writes the
	// new value into the bag; the key template renders against the bag.
	_, _ = nodeIncr(context.Background(), Node{Config: map[string]any{"key": "score-{user}", "into": "s"}}, nc)
	assert.Equal(t, "1", nc.bag["s"])
	_, _ = nodeIncr(context.Background(), Node{Config: map[string]any{"key": "score-{user}", "by": "5", "into": "s"}}, nc)
	assert.Equal(t, "6", nc.bag["s"])
	// Default into = the key; empty key and nil store are no-ops.
	_, _ = nodeIncr(context.Background(), Node{Config: map[string]any{"key": "n"}}, nc)
	assert.Equal(t, "1", nc.bag["n"])
	_, _ = nodeIncr(context.Background(), Node{Config: map[string]any{}}, nc)
	_, _ = nodeIncr(context.Background(), Node{Config: map[string]any{"key": "x"}}, &nodeContext{})

	_, err = nodeWebhook(context.Background(), Node{Config: map[string]any{"url": ""}}, nc)
	require.NoError(t, err)
	nc4 := &nodeContext{bag: Bag{}}
	_, _ = nodeSetvar(context.Background(), Node{Config: map[string]any{"key": "k"}}, nc4)
	_, _ = nodeGetvar(context.Background(), Node{Config: map[string]any{"key": "k"}}, nc4)
}

func TestBagStr(t *testing.T) {
	b := Bag{"s": "x", "n": 5}
	assert.Equal(t, "x", b.str("s"))
	assert.Equal(t, "5", b.str("n"))
	assert.Equal(t, "", b.str("missing"))
}

func TestRegisterCustomNode(t *testing.T) {
	Register(NodeType{Name: "test-custom", Ports: []string{""}, Handler: func(context.Context, Node, *nodeContext) (string, error) { return "", nil }})
	_, ok := lookup("test-custom")
	assert.True(t, ok)
	delete(registry, "test-custom")
}
