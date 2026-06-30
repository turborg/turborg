// Package server hosts the pooled (multi-instance) turborg runtime: one
// long-lived process that holds N tenants, each an isolated agent. It runs
// alongside the single-instance binary (cmd/turborg), which stays the default
// for env-driven single-bot deployments.
//
// The Server attaches and detaches tenants from a TenantSource and supervises
// their goroutines, with crash isolation, per-tenant limits, and SNI-based
// ingress routing.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/turborg/turborg/internal/flow"
	"github.com/turborg/turborg/internal/safe"
	"github.com/turborg/turborg/internal/skill"
)

// ConnectorSpec describes one connector instance a tenant runs. The Type field
// matches turborg's connector names.
type ConnectorSpec struct {
	Type    string         `json:"type"`
	Config  map[string]any `json:"config"`
	Secrets map[string]any `json:"secrets"`
}

// TenantSpec is the desired state of a single tenant. Keyed by the logical
// TurborgID — never a runtime identifier. Both the FileSource and the
// HTTPSource decode the same JSON shape.
type TenantSpec struct {
	TurborgID        string            `json:"turborg_id"`
	ShardID          int               `json:"shard_id,omitempty"`
	RuntimeMode      string            `json:"runtime_mode"`
	UserUUID         string            `json:"user_uuid"`
	PlanCode         string            `json:"plan_code"`
	CommandPrefix    string            `json:"command_prefix,omitempty"`
	Connectors       []ConnectorSpec   `json:"connectors"`
	PlanCapabilities *PlanCapabilities `json:"plan_capabilities,omitempty"`

	// IgnoredNicks is the owner's ignore list: the command guard drops
	// !commands from these nicks. Mirrors the single-instance TURBORG_IGNORED_NICKS.
	IgnoredNicks []string `json:"ignored_nicks,omitempty"`

	// Commands is the tenant's data-driven command set (the same wire shape
	// the single-instance runtime carries as TURBORG_COMMANDS). The pooled runtime
	// swaps it into the agent's registry, and — uniquely — a change to ONLY
	// this field is applied in place without dropping the IRC connection
	// (see Tenant.update). Ordered; an empty slice means no commands. The flat
	// wire is backward-compatible: a legacy command array decodes to
	// command-kind skills unchanged.
	Commands []skill.Skill `json:"commands,omitempty"`

	// Flows is the tenant's declarative node-graph flow set, run by the flow
	// engine on event/match triggers. Ordered; an empty slice means no flows.
	Flows []flow.Flow `json:"flows,omitempty"`

	// GatewayToken authorizes the per-tenant web shell (the `/ws` surface).
	// An empty token means "no web shell for this tenant" and the pooled
	// gateway is not built. The web router's StaticPasswordVerifier checks the
	// `?token=` query against it.
	GatewayToken string `json:"gateway_token,omitempty"`

	// EgressIP is the tenant's assigned public egress IP (accounts-api's
	// host_ip_id). The pool resolves it to its own local source IP on the
	// matching egress network (via TURBORG_EGRESS_MAP) and binds the IRC dial to
	// it, so the host SNAT egresses on this IP. Empty → default route (single-IP
	// hosts / unconfigured).
	EgressIP string `json:"egress_ip,omitempty"`
}

// PlanCapabilities is the per-tenant capability limits the pooled runtime
// enforces in-process. The single-instance binary expresses the same limits via
// env vars; here they arrive in the tenant feed. 0 = unrestricted by convention.
type PlanCapabilities struct {
	NickLocked            bool `json:"nick_locked"`
	RealnameLocked        bool `json:"realname_locked"`
	MaxChannels           int  `json:"max_channels"`
	OutboundMsgsPerWindow int  `json:"outbound_msgs_per_window"`
	OutboundWindowSeconds int  `json:"outbound_window_seconds"`
	// OwnerDMNudgeEvery: DM the owner every N outbound PRIVMSGs with a usage
	// summary. Fires only when an owner nick is configured.
	OwnerDMNudgeEvery int `json:"owner_dm_nudge_every"`
	// CommandMaxPerWindow/WindowSeconds: per-sender !command throttle.
	// 0 = unthrottled by convention.
	CommandMaxPerWindow  int `json:"command_max_per_window"`
	CommandWindowSeconds int `json:"command_window_seconds"`
	// CTCPMaxPerWindow/WindowSeconds: per-sender CTCP throttle.
	// BouncerMaxFailedAttempts: bouncer auth-failure ceiling. All
	// 0 = fall back to the connector's ApplyDefaults values.
	CTCPMaxPerWindow         int `json:"ctcp_max_per_window"`
	CTCPWindowSeconds        int `json:"ctcp_window_seconds"`
	BouncerMaxFailedAttempts int `json:"bouncer_max_failed_attempts"`
	// QuitMessage is the IRC QUIT brand applied to the tenant's connector.
	// Empty → the connector default.
	QuitMessage string `json:"quit_message"`

	// CustomCommandsMax caps how many data-driven commands the tenant's
	// registry accepts (a safety net; the control plane enforces the same
	// cap on attach). 0 = no commands, -1 = unrestricted.
	CustomCommandsMax int `json:"custom_commands_max"`

	// TBSummarizeMaxMessages caps how many channel messages the /tb summarize
	// subcommand can consume in a single invocation. 0 = feature disabled.
	TBSummarizeMaxMessages int `json:"tb_summarize_max_messages"`

	// LLMInputTokensPerDay / LLMOutputTokensPerDay are the rolling-24h
	// token budget caps. 0 = unrestricted.
	LLMInputTokensPerDay  int `json:"llm_input_tokens_per_day"`
	LLMOutputTokensPerDay int `json:"llm_output_tokens_per_day"`

	// LLMInputTokensUsed / LLMOutputTokensUsed seed the budget with consumption
	// the control plane has already recorded across the account for the rolling
	// window (this tenant's prior incarnations + sibling tenants). They make the
	// cap enforce per account/window rather than per tenant-instance, so a
	// delete+recreate can't reset the window. 0 = fresh window.
	LLMInputTokensUsed  int `json:"llm_input_tokens_used"`
	LLMOutputTokensUsed int `json:"llm_output_tokens_used"`

	// LLM carries the OpenAI-compatible router config for LLM-type commands.
	// Documentation-only on the turborg side: the pooled process builds one
	// shared provider from its own env (the API key is a server-side secret,
	// never in the feed) and the model catalog is enforced upstream at attach
	// time. Mirrors the single-instance TURBORG_LLM_* env.
	LLM *LLMRouterConfig `json:"llm,omitempty"`
}

// LLMRouterConfig is the OpenAI-compatible LLM router block. It travels in
// the capability config for documentation + parity with the single-instance
// TURBORG_LLM_* env; the secret API key is never carried here.
type LLMRouterConfig struct {
	Provider      string   `json:"provider"`
	BaseURL       string   `json:"base_url"`
	DefaultModel  string   `json:"default_model"`
	AllowedModels []string `json:"allowed_models,omitempty"`
}

// TenantEventKind distinguishes an attach/update from a detach.
type TenantEventKind int

const (
	// TenantUpserted creates the tenant if new, or updates it in place.
	TenantUpserted TenantEventKind = iota
	// TenantRemoved detaches the tenant identified by TurborgID.
	TenantRemoved
)

// TenantEvent is a single change emitted by a TenantSource after the initial
// snapshot. For TenantUpserted, Spec is populated. For TenantRemoved, only
// TurborgID is meaningful.
type TenantEvent struct {
	Kind      TenantEventKind
	Spec      TenantSpec
	TurborgID string
}

// TenantSource feeds the Server its desired tenant set. Initial returns the
// full snapshot at boot; Watch streams subsequent changes. Implementations
// include a JSON file (FileSource) and an HTTP poll against a control plane
// (HTTPSource).
type TenantSource interface {
	Initial(ctx context.Context) ([]TenantSpec, error)
	Watch(ctx context.Context) (<-chan TenantEvent, error)
}

// fileDoc is the on-disk shape read by FileSource.
type fileDoc struct {
	Tenants []TenantSpec `json:"tenants"`
}

// FileSource loads tenants from a JSON file. It reads the snapshot once at
// boot; the Watch channel stays open until ctx is cancelled (no live reload on
// file change yet).
type FileSource struct {
	Path string
}

// Initial reads and decodes the tenants file.
func (f *FileSource) Initial(_ context.Context) ([]TenantSpec, error) {
	raw, err := os.ReadFile(f.Path)
	if err != nil {
		return nil, fmt.Errorf("read tenants file %q: %w", f.Path, err)
	}
	var doc fileDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse tenants file %q: %w", f.Path, err)
	}
	return doc.Tenants, nil
}

// Watch returns a channel that yields no events and closes when ctx is
// cancelled. Filesystem-watch-driven hot reload is a later milestone.
func (f *FileSource) Watch(ctx context.Context) (<-chan TenantEvent, error) {
	ch := make(chan TenantEvent)
	safe.Go("filesource-watch", func() {
		<-ctx.Done()
		close(ch)
	})
	return ch, nil
}

// StaticSource serves a fixed snapshot plus an optional caller-controlled
// event channel. Primarily a test and embedding seam.
type StaticSource struct {
	Tenants []TenantSpec
	// Events, when non-nil, is returned by Watch so callers can drive
	// upsert/remove events. When nil, Watch yields nothing until ctx ends.
	Events chan TenantEvent
}

// Initial returns the fixed snapshot.
func (s *StaticSource) Initial(_ context.Context) ([]TenantSpec, error) {
	return s.Tenants, nil
}

// Watch returns the caller-controlled channel, or a ctx-closed empty one.
func (s *StaticSource) Watch(ctx context.Context) (<-chan TenantEvent, error) {
	if s.Events != nil {
		return s.Events, nil
	}
	ch := make(chan TenantEvent)
	safe.Go("staticsource-watch", func() {
		<-ctx.Done()
		close(ch)
	})
	return ch, nil
}
