package agent

import "context"

// Connector is the lifecycle contract every protocol adapter must implement.
//
// Start runs the synchronous handshake. Run is the long-lived loop the agent
// supervises inside its errgroup; it should return only on ctx cancellation
// or fatal error. Stop sends a clean shutdown signal and closes resources;
// it must be safe to call after Run has returned and idempotent.
//
// Inbound delivers messages observed from the protocol. Send queues an
// outbound message; implementations may serialize writes via a mutex.
//
// ClaimSupervision must return true for connectors that want their Run
// method owned by the agent's errgroup. PoC: always true.
type Connector interface {
	Name() string
	Start(ctx context.Context) error
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
	Inbound() <-chan *InboundEnvelope
	Send(env *OutboundEnvelope) error
	ClaimSupervision() bool
}
