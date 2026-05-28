// Package server hosts the multi-tenant ("pooled") turborg runtime: one
// long-lived process that holds N tenants, each an isolated agent. It runs
// alongside the existing single-tenant binary (cmd/turborg), which stays the
// default for hobbyist/OSS env-driven deployments.
//
// M1 (this milestone) is lifecycle-only: the Server attaches and detaches
// tenants from a TenantSource and supervises their goroutines. Real connector
// behaviour, crash isolation, per-tenant limits, and the SNI bouncer router
// land in later milestones (see accounts-api/dev/PLAN-multi-tenancy.md WS2).
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/turborg/turborg/internal/safe"
)

// ConnectorSpec describes one connector instance a tenant runs. Mirrors the
// sidecar/accounts-api wire shape; Type matches turborg's connector names.
type ConnectorSpec struct {
	Type    string         `json:"type"`
	Config  map[string]any `json:"config"`
	Secrets map[string]any `json:"secrets"`
}

// TenantSpec is the desired state of a single tenant. Keyed by the logical
// TurborgID — never a runtime identifier. Wire-compatible with the sidecar
// TenantSpec so an HTTPSource (M5) can decode the same JSON.
type TenantSpec struct {
	TurborgID        string            `json:"turborg_id"`
	ShardID          int               `json:"shard_id,omitempty"`
	RuntimeMode      string            `json:"runtime_mode"`
	UserUUID         string            `json:"user_uuid"`
	PlanCode         string            `json:"plan_code"`
	CommandPrefix    string            `json:"command_prefix,omitempty"`
	Connectors       []ConnectorSpec   `json:"connectors"`
	PlanCapabilities *PlanCapabilities `json:"plan_capabilities,omitempty"`

	// GatewayToken authorizes the per-tenant web shell (the appui `/ws`
	// surface). It's the turborg's existing container_token, threaded through
	// by accounts-api; an empty token means "no web shell for this tenant" and
	// the pooled gateway is not built. The web router's StaticPasswordVerifier
	// checks the `?token=` query against it.
	GatewayToken string `json:"gateway_token,omitempty"`
}

// PlanCapabilities is the subset of accounts-api's plan-tier caps the pooled
// runtime enforces per tenant (M4). Mirrors the sidecar PlanCapabilities wire
// shape; container mode delegates these to cgroups + the connector's env,
// pooled mode enforces them in-process. 0 = unrestricted by convention.
type PlanCapabilities struct {
	NickLocked            bool `json:"nick_locked"`
	RealnameLocked        bool `json:"realname_locked"`
	MaxChannels           int  `json:"max_channels"`
	OutboundMsgsPerWindow int  `json:"outbound_msgs_per_window"`
	OutboundWindowSeconds int  `json:"outbound_window_seconds"`
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
// full snapshot at boot; Watch streams subsequent changes. Three
// implementations are planned (plan WS1 F1): env (single tenant), file
// (this package), and HTTP long-poll against accounts-api (M5).
type TenantSource interface {
	Initial(ctx context.Context) ([]TenantSpec, error)
	Watch(ctx context.Context) (<-chan TenantEvent, error)
}

// fileDoc is the on-disk shape read by FileSource.
type fileDoc struct {
	Tenants []TenantSpec `json:"tenants"`
}

// FileSource loads tenants from a JSON file. M1 reads the snapshot once at
// boot; live reload on file change lands in a later milestone (the Watch
// channel simply stays open until ctx is cancelled for now).
//
// JSON rather than the plan's YAML to avoid promoting the indirect yaml.v3
// dependency to a direct one without sign-off; the wire shape is identical.
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
