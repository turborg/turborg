// Package ident maintains the live mapping from an upstream IRC connection's
// local TCP source port to the owning tenant's IRC ident (the USER value), and
// serves it over HTTP for the sidecar's RFC-1413 (ident) responder.
//
// One Registry per turborg process: the pooled server shares a single Registry
// across every tenant; a dedicated container holds a single entry. The sidecar
// resolves an incoming ident query to (containerIP, sourcePort) via conntrack,
// then asks that container's router for the ident — so this same code backs
// both runtimes.
package ident

import "sync"

// Registry is a concurrency-safe source-port → ident map. It satisfies
// irc.IdentReporter (Set/Clear) structurally, so the connector can report into
// it without this package importing irc.
type Registry struct {
	mu sync.RWMutex
	m  map[int]string
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{m: make(map[int]string)}
}

// Set records that the upstream connection on localPort belongs to ident.
// No-ops on a zero port or empty ident so a half-open dial can't poison the map.
func (r *Registry) Set(localPort int, ident string) {
	if localPort == 0 || ident == "" {
		return
	}
	r.mu.Lock()
	r.m[localPort] = ident
	r.mu.Unlock()
}

// Clear drops the mapping for localPort (connection closed or replaced).
func (r *Registry) Clear(localPort int) {
	if localPort == 0 {
		return
	}
	r.mu.Lock()
	delete(r.m, localPort)
	r.mu.Unlock()
}

// Lookup returns the ident registered for localPort, and whether it was present.
func (r *Registry) Lookup(localPort int) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.m[localPort]
	return id, ok
}
