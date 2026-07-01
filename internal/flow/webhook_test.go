package flow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turborg/turborg/internal/skill"
)

// webhookFlow: a webhook-triggered flow that says the posted text to a channel.
func webhookFlow(name string, channels ...string) Flow {
	return Flow{
		Name:    name,
		Trigger: skill.Trigger{Kind: skill.KindWebhook, Channels: channels},
		Nodes:   []Node{{ID: "s", Type: "say", Config: map[string]any{"channel": "{channel}", "text": "{text} from {user}"}}},
	}
}

func TestFlowFireWebhookDispatch(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{webhookFlow("deploy")})

	ok := e.FireWebhook("deploy", map[string]string{"channel": "#ops", "text": "ship", "user": "ci"})
	assert.True(t, ok)
	assert.Equal(t, []string{"say #ops ship from ci"}, act.snapshot())
}

func TestFlowFireWebhookCaseInsensitive(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{webhookFlow("Deploy")})

	assert.True(t, e.FireWebhook("dEpLoY", map[string]string{"channel": "#c", "text": "x", "user": "u"}))
	assert.Equal(t, []string{"say #c x from u"}, act.snapshot())
}

func TestFlowFireWebhookUnknownName(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceFlows([]Flow{webhookFlow("deploy")})

	assert.False(t, e.FireWebhook("ghost", map[string]string{"channel": "#c", "text": "x"}))
	assert.Empty(t, act.snapshot(), "no match must fire nothing")
}

func TestFlowFireWebhookTriggerChannelWins(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	// Trigger scopes the flow to #locked; a body-supplied channel must not override it.
	e.ReplaceFlows([]Flow{webhookFlow("deploy", "#locked")})

	assert.True(t, e.FireWebhook("deploy", map[string]string{"channel": "#attacker", "text": "x", "user": "u"}))
	assert.Equal(t, []string{"say #locked x from u"}, act.snapshot())
}

func TestFlowFireWebhookNotIndexedForOtherKinds(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	// A match-trigger flow named "deploy" must not be reachable via the webhook index.
	e.ReplaceFlows([]Flow{{
		Name:    "deploy",
		Trigger: skill.Trigger{Kind: skill.KindMatch, Match: "hi"},
		Nodes:   []Node{{ID: "s", Type: "say", Config: map[string]any{"channel": "#c", "text": "x"}}},
	}})
	assert.False(t, e.FireWebhook("deploy", map[string]string{"text": "x"}))
	assert.Empty(t, act.snapshot())
}
