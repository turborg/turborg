package irc

// IdentReporter receives the live mapping between an upstream connection's
// local TCP source port and the tenant's IRC ident (the USER value). The pooled
// runtime implements it so an external RFC-1413 responder can answer an IRC
// server's ident query with a real, verified username instead of the
// HIDDEN-USER stub (which leaves the user with a ~-prefixed ident and trips the
// "identd required" gate on networks like IRCnet/Undernet).
//
// Implementations must be safe for concurrent use; the connector calls Set/Clear
// from its reconnect supervisor goroutine. A nil reporter (single-instance
// runtime, tests) disables reporting entirely.
type IdentReporter interface {
	// Set records that the connection on localPort belongs to ident.
	Set(localPort int, ident string)
	// Clear drops the mapping for localPort (connection closed/replaced).
	Clear(localPort int)
}
