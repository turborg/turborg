// Package skill generalizes data-driven command definitions into a
// trigger+action engine. A skill pairs a trigger (what makes it fire) with
// an action (what it does):
//
//   - command  trigger: a prefixed word a user types (the historical "command").
//   - event    trigger: a connector lifecycle event (join/leave/kick/…).
//   - match    trigger: a regular expression tested against every message.
//   - schedule trigger: a fixed interval or cron cadence.
//
// Actions render a template as a literal reply, run it as an LLM prompt,
// classify a message with an LLM and map the verdict onto a moderation
// effect, or run a moderation effect directly (kick/ban/mute/…).
//
// The wire shape is FLAT and backward-compatible: a legacy command
// definition ({name,type,template,instructions,access,allowlist}) decodes to
// a command/reply skill identical to the historical one, and any skill whose
// new fields are all default marshals byte-identically to the legacy shape.
// All new fields are optional. Internally a Skill is a nested
// Skill{Trigger, Action} value; (Un)MarshalJSON map between the flat wire and
// the nested form.
//
// The package is transport-agnostic and declarative only: there is no
// sandboxed or arbitrary code execution. Definitions arrive as JSON (from an
// env var or a polled feed), are decoded into Skill values, and split into
// command handlers (for the agent's CommandRegistry) and engine skills (for
// the event/match/schedule Engine).
package skill

import (
	"encoding/json"

	"github.com/turborg/turborg/internal/commands"
)

// Type is the action kind. Static renders the template as a literal reply;
// LLM runs it as a prompt; LLMClassify asks an LLM for a structured verdict
// and maps it onto a moderation effect; Effect runs a moderation effect.
type Type string

const (
	// TypeStatic renders the template (with placeholders substituted) as the
	// literal reply — a deterministic, zero-cost canned response.
	TypeStatic Type = "static"
	// TypeLLM runs the rendered template as a prompt against the configured
	// LLM provider and replies with the model's answer.
	TypeLLM Type = "llm"
	// TypeLLMClassify asks the LLM for a structured {severity,action,reason}
	// verdict on the triggering message and maps the severity onto a
	// moderation effect via the action's effect thresholds.
	TypeLLMClassify Type = "llm_classify"
	// TypeEffect runs a moderation effect (kick/ban/mute/…) directly when the
	// trigger fires. With effect thresholds set it becomes a flood gate keyed
	// off a per-sender sliding-window message count.
	TypeEffect Type = "effect"
	// TypeWebhook POSTs the trigger context as JSON to an external URL when the
	// skill fires. It is the integration seam to outbound flow engines: turborg
	// is the trigger source + action sink, the external graph does the rest.
	TypeWebhook Type = "webhook"
)

// Access is the per-skill trust policy deciding who may trigger a command
// skill. It is meaningful for command triggers; event/match/schedule skills
// are operator-installed and not user-invoked, so access does not gate them.
type Access = commands.Access

const (
	// AccessEveryone lets any sender trigger the skill.
	AccessEveryone = commands.AccessEveryone
	// AccessOwner restricts the skill to the verified owner.
	AccessOwner = commands.AccessOwner
	// AccessAllowlist allows the owner plus an explicit nick/account list.
	AccessAllowlist = commands.AccessAllowlist
)

// TriggerKind selects what makes a skill fire.
type TriggerKind string

const (
	// KindCommand fires on a prefixed command word (the historical default).
	KindCommand TriggerKind = "command"
	// KindEvent fires on a connector lifecycle event named by Trigger.Event.
	KindEvent TriggerKind = "event"
	// KindMatch fires when Trigger.Match (a regex) matches a message.
	KindMatch TriggerKind = "match"
	// KindSchedule fires on the cadence named by Trigger.Schedule.
	KindSchedule TriggerKind = "schedule"
	// KindWebhook fires when an external system POSTs to the skill's inbound
	// webhook URL. The decoded request body seeds the render/data bag. It is the
	// inbound counterpart to the outbound TypeWebhook action: here turborg is the
	// action sink an external event drives, rather than the trigger source.
	KindWebhook TriggerKind = "webhook"
)

// Event names valid for an event-trigger skill. They mirror the agent's
// EventBus user-event types but are kept as a closed set here so a malformed
// definition fails validation rather than silently never firing.
const (
	EventUserJoin       = "USER_JOIN"
	EventUserLeave      = "USER_LEAVE"
	EventUserKicked     = "USER_KICKED"
	EventUserNickChange = "USER_NICK_CHANGE"
	EventTopicChanged   = "TOPIC_CHANGED"
	EventModeChanged    = "MODE_CHANGED"
)

// EffectAction is a moderation action an Effect can perform.
type EffectAction string

const (
	EffectKick   EffectAction = "kick"
	EffectBan    EffectAction = "ban"
	EffectMute   EffectAction = "mute"
	EffectOp     EffectAction = "op"
	EffectVoice  EffectAction = "voice"
	EffectMode   EffectAction = "mode"
	EffectNotice EffectAction = "notice"
	EffectTopic  EffectAction = "topic"
)

// Thresholds maps a 0..3 severity onto an escalating action. Each field is
// the lowest severity (or, for a flood gate, the lowest count) at which that
// action triggers; the highest matched tier wins. A zero field disables that
// tier. warn maps to a notice.
type Thresholds struct {
	Warn int `json:"warn,omitempty"`
	Mute int `json:"mute,omitempty"`
	Kick int `json:"kick,omitempty"`
	Ban  int `json:"ban,omitempty"`
}

// Effect parameterizes a moderation action.
type Effect struct {
	// Action is the moderation action to perform. For llm_classify (and a
	// flood gate) the action is derived from Thresholds instead and this is
	// the optional default fallback.
	Action EffectAction `json:"action,omitempty"`
	// Modes is the raw mode string for Action=="mode" (e.g. "+m").
	Modes string `json:"modes,omitempty"`
	// Reason annotates a kick/ban.
	Reason string `json:"reason,omitempty"`
	// Thresholds maps a verdict severity / flood count onto an action.
	Thresholds *Thresholds `json:"thresholds,omitempty"`
	// DurationSeconds, when > 0 and Action is a reversible mode
	// (mute/ban/mode/op/voice), auto-lifts the applied mode after that many
	// seconds. The pending lift is persisted via the Store so it survives a
	// restart. Non-reversible actions (kick/notice/topic) ignore it.
	DurationSeconds int `json:"duration_seconds,omitempty"`
}

// Trigger is what makes a skill fire. The JSON tags are used when a Trigger is
// nested directly in another document (e.g. a flow); the flat skill wire maps
// these fields by hand in Skill's (Un)MarshalJSON and does not marshal a
// Trigger directly.
type Trigger struct {
	// Kind selects the trigger family. Empty decodes as KindCommand.
	Kind TriggerKind `json:"kind,omitempty"`
	// Event is the lifecycle event for KindEvent.
	Event string `json:"event,omitempty"`
	// Match is the regex for KindMatch (and the cheap prefilter for an
	// llm_classify action).
	Match string `json:"match,omitempty"`
	// Schedule is the interval ("30m"/"1h"/"45s") or 5-field cron for
	// KindSchedule.
	Schedule string `json:"schedule,omitempty"`
	// Channels scopes the trigger to specific channels. Empty = all.
	Channels []string `json:"channels,omitempty"`
}

// Action is what a skill does when it fires.
type Action struct {
	// Type selects the action kind.
	Type Type
	// Template is the reply/prompt template, with placeholders.
	Template string
	// Instructions is the LLM system prompt for llm / llm_classify actions.
	Instructions string
	// Model optionally overrides the LLM model for this skill.
	Model string
	// Effect parameterizes a moderation action (effect / llm_classify).
	Effect *Effect
	// Webhook is the outbound URL a webhook action POSTs the trigger context
	// to. Empty for non-webhook actions.
	Webhook string
}

// Skill is one declarative trigger+action unit. It is the nested in-memory
// form; the wire form is flat (see MarshalJSON / UnmarshalJSON).
type Skill struct {
	Name      string
	Access    Access
	Allowlist []string
	Trigger   Trigger
	Action    Action
}

// wire is the flat JSON shape shared by the legacy command definition and the
// generalized skill. Field order matches the legacy commands.Definition so a
// command/reply skill marshals byte-identically to the historical wire; the
// generalized fields follow and are all omitempty.
type wire struct {
	Name         string   `json:"name"`
	Type         Type     `json:"type"`
	Template     string   `json:"template"`
	Instructions string   `json:"instructions,omitempty"`
	Model        string   `json:"model,omitempty"`
	Access       Access   `json:"access"`
	Allowlist    []string `json:"allowlist,omitempty"`

	TriggerKind TriggerKind `json:"trigger_kind,omitempty"`
	Event       string      `json:"event,omitempty"`
	Match       string      `json:"match,omitempty"`
	Schedule    string      `json:"schedule,omitempty"`
	Channels    []string    `json:"channels,omitempty"`
	Effect      *Effect     `json:"effect,omitempty"`
	Webhook     string      `json:"webhook,omitempty"`
}

// UnmarshalJSON decodes the flat wire (including the legacy command shape)
// into the nested Skill. An absent trigger_kind defaults to KindCommand, so
// historical command definitions decode to command/reply skills unchanged.
func (s *Skill) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	kind := w.TriggerKind
	if kind == "" {
		kind = KindCommand
	}
	*s = Skill{
		Name:      w.Name,
		Access:    w.Access,
		Allowlist: w.Allowlist,
		Trigger: Trigger{
			Kind:     kind,
			Event:    w.Event,
			Match:    w.Match,
			Schedule: w.Schedule,
			Channels: w.Channels,
		},
		Action: Action{
			Type:         w.Type,
			Template:     w.Template,
			Instructions: w.Instructions,
			Model:        w.Model,
			Effect:       w.Effect,
			Webhook:      w.Webhook,
		},
	}
	return nil
}

// MarshalJSON emits the flat wire and omits defaults: a command-kind skill
// leaves trigger_kind unset so the output matches the legacy command shape
// byte-for-byte, and all generalized fields are omitempty.
func (s Skill) MarshalJSON() ([]byte, error) {
	w := wire{
		Name:         s.Name,
		Type:         s.Action.Type,
		Template:     s.Action.Template,
		Instructions: s.Action.Instructions,
		Model:        s.Action.Model,
		Access:       s.Access,
		Allowlist:    s.Allowlist,
		Event:        s.Trigger.Event,
		Match:        s.Trigger.Match,
		Schedule:     s.Trigger.Schedule,
		Channels:     s.Trigger.Channels,
		Effect:       s.Action.Effect,
		Webhook:      s.Action.Webhook,
	}
	// Omit the default command kind so legacy payloads round-trip identically.
	if s.Trigger.Kind != "" && s.Trigger.Kind != KindCommand {
		w.TriggerKind = s.Trigger.Kind
	}
	return json.Marshal(w)
}

// IsCommand reports whether the skill fires on a command word (the default).
func (s Skill) IsCommand() bool {
	return s.Trigger.Kind == "" || s.Trigger.Kind == KindCommand
}

// ToDefinition projects a command-kind skill onto the legacy command
// definition consumed by the command builder. It is only meaningful for
// command-kind skills (IsCommand); the generalized trigger/effect fields are
// not part of a command definition and are dropped.
func (s Skill) ToDefinition() commands.Definition {
	return commands.Definition{
		Name:         s.Name,
		Type:         commands.Type(s.Action.Type),
		Template:     s.Action.Template,
		Instructions: s.Action.Instructions,
		Model:        s.Action.Model,
		Access:       s.Access,
		Allowlist:    s.Allowlist,
	}
}

// Command builds a command-kind skill from flat fields — an ergonomic
// constructor for callers (and tests) that would otherwise nest by hand.
func Command(name string, typ Type, template string, access Access, allowlist ...string) Skill {
	return Skill{
		Name:      name,
		Access:    access,
		Allowlist: allowlist,
		Trigger:   Trigger{Kind: KindCommand},
		Action:    Action{Type: typ, Template: template},
	}
}

// Split partitions skills into command-kind definitions (for the command
// registry) and the remaining event/match/schedule skills (for the Engine).
// Order is preserved within each partition.
func Split(skills []Skill) (cmds []commands.Definition, engine []Skill) {
	for _, s := range skills {
		if s.IsCommand() {
			cmds = append(cmds, s.ToDefinition())
			continue
		}
		engine = append(engine, s)
	}
	return cmds, engine
}
