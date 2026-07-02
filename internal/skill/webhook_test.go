package skill

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func webhookSkill(name string, channels ...string) Skill {
	return Skill{
		Name:    name,
		Trigger: Trigger{Kind: KindWebhook, Channels: channels},
		Action:  Action{Type: TypeStatic, Template: "{text} from {user}"},
	}
}

func TestEngineFireWebhookDispatch(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{webhookSkill("deploy")})

	ok := e.FireWebhook("deploy", map[string]string{"channel": "#ops", "text": "ship", "user": "ci"})
	assert.True(t, ok)
	assert.Equal(t, []string{"say #ops ship from ci"}, act.snapshot())
}

func TestEngineFireWebhookCaseInsensitive(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{webhookSkill("Deploy")})

	assert.True(t, e.FireWebhook("DEPLOY", map[string]string{"channel": "#c", "text": "x", "user": "u"}))
	assert.Equal(t, []string{"say #c x from u"}, act.snapshot())
}

func TestEngineFireWebhookUnknownName(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{webhookSkill("deploy")})

	assert.False(t, e.FireWebhook("ghost", map[string]string{"channel": "#c", "text": "x"}))
	assert.Empty(t, act.snapshot())
}

func TestEngineFireWebhookTriggerChannelWins(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{webhookSkill("deploy", "#locked")})

	assert.True(t, e.FireWebhook("deploy", map[string]string{"channel": "#attacker", "text": "x", "user": "u"}))
	assert.Equal(t, []string{"say #locked x from u"}, act.snapshot())
}

func TestEngineFireWebhookEmptyNameDropped(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{webhookSkill("")}) // empty name is dropped at index time

	assert.False(t, e.FireWebhook("", map[string]string{"channel": "#c", "text": "x"}))
	assert.Empty(t, act.snapshot())
}

func TestEngineFireWebhookNotIndexedForOtherKinds(t *testing.T) {
	act := &fakeActor{}
	e, _ := newEngine(t, Options{Actor: act})
	e.ReplaceSkills([]Skill{{
		Name:    "deploy",
		Trigger: Trigger{Kind: KindMatch, Match: "hi"},
		Action:  Action{Type: TypeStatic, Template: "x"},
	}})
	assert.False(t, e.FireWebhook("deploy", map[string]string{"text": "x"}))
	assert.Empty(t, act.snapshot())
}
