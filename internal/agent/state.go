package agent

import "time"

// ConnectorState is a connector-agnostic snapshot of a connector's live
// connection state, surfaced to the state-mirror emitter. State is one of:
// "connecting", "connected", "suspended", "error", "disconnected".
type ConnectorState struct {
	State  string
	Since  time.Time
	Reason string
}

// StateReporter is the optional capability a connector implements to expose
// its live connection state.
type StateReporter interface{ ConnectorState() ConnectorState }

// StateSubscriber optionally lets a connector push a notify() callback on
// every state change so the emitter is event-driven rather than polled.
type StateSubscriber interface{ OnStateChange(fn func()) }
