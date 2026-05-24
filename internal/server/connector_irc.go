package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/turborg/turborg/internal/connector/irc"
)

// settingsFromConnectorSpec maps a tenant's IRC ConnectorSpec (the
// accounts-api/sidecar wire shape: config map + secrets map) onto irc.Settings.
// Pure — no IO. The single-tenant binary derives the same struct from env via
// irc.LoadSettings; this is the pooled-mode equivalent sourced from a spec.
//
// The per-tenant bouncer is intentionally NOT wired here: N tenants in one
// process can't each bind a host bouncer port. Pooled bouncer attach is the
// SNI-router milestone (M6); until then pooled tenants run upstream-only.
func settingsFromConnectorSpec(cs ConnectorSpec) (*irc.Settings, error) {
	host, port, err := splitNetwork(stringField(cs.Config, "network"))
	if err != nil {
		return nil, err
	}
	nick := stringField(cs.Config, "nick")
	if nick == "" {
		return nil, fmt.Errorf("irc connector: empty nick")
	}

	s := &irc.Settings{
		Hostname: host,
		Port:     port,
		UseTLS:   boolField(cs.Config, "use_tls", true),
		Nick:     nick,
		Username: stringField(cs.Config, "username"),
		RealName: stringFieldOr(cs.Config, "real_name", "turborg agent"),
		AuthMode: stringField(cs.Config, "auth_mode"),
		Channels: stringSlice(cs.Config, "channels"),

		SASLUser:         stringField(cs.Secrets, "sasl_user"),
		SASLPassword:     stringField(cs.Secrets, "sasl_password"),
		NickServPassword: stringField(cs.Secrets, "nickserv_password"),
		ServerPassword:   stringField(cs.Secrets, "server_password"),
	}

	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("irc settings: %w", err)
	}
	return s, nil
}

// splitNetwork parses "host:port" (the wire shape accounts-api emits). A bare
// host defaults to 6697 (TLS IRC). Returns an error for an empty host.
func splitNetwork(network string) (string, int, error) {
	network = strings.TrimSpace(network)
	if network == "" {
		return "", 0, fmt.Errorf("irc connector: empty network")
	}
	host, portStr, found := strings.Cut(network, ":")
	if !found {
		return network, 6697, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("irc connector: invalid port in %q: %w", network, err)
	}
	return host, port, nil
}

// stringField reads a string value from a JSON-decoded config map, returning
// "" when absent or not a string.
func stringField(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func stringFieldOr(m map[string]any, key, fallback string) string {
	if v := stringField(m, key); v != "" {
		return v
	}
	return fallback
}

// boolField reads a bool, tolerating absent keys (returns fallback).
func boolField(m map[string]any, key string, fallback bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return fallback
}

// stringSlice reads a []string from a JSON-decoded array ([]any of strings).
func stringSlice(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
