package hive

import "context"

// NoopClient is the v0.1 default: every method is a no-op and
// Connected reports true. Lets the agent wire against Client today
// without depending on a hive endpoint that doesn't exist yet.
type NoopClient struct{}

func (NoopClient) Connect(context.Context) error    { return nil }
func (NoopClient) Disconnect(context.Context) error { return nil }
func (NoopClient) Heartbeat(context.Context) error  { return nil }
func (NoopClient) Connected() bool                  { return true }
