// Package hive defines the future shared-intelligence layer turborg
// plugs into hive.xshellz.com. v0.1 ships only the interface and a
// no-op default; the real client lands in a later release.
//
// Why an interface today? Because the place a hive client plugs into
// the Agent is small and stable. Freezing it now lets v0.1 users wire
// their bots to "the hive slot" without code changes when the real
// client ships — they flip a setting, swap the noop for the real
// implementation, done.
package hive

import "context"

// Client is the surface every hive implementation must expose.
//
// Lifecycle is Connect → use → Disconnect. Heartbeat is called
// periodically by the agent to keep registration alive; implementations
// should be cheap and idempotent (failing one heartbeat is normal,
// failing several in a row should be loud but recoverable).
type Client interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	Heartbeat(ctx context.Context) error
	Connected() bool
}
