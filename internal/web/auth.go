// Package web is turborg's control-plane gateway: a JSON-over-WS protocol
// that streams the agent's events to browser clients and accepts command
// ops, plus the bundled vanilla-JS reference UI served from the same HTTP
// server. SaaS deployments replace the TokenVerifier with their own auth
// backend; self-host uses a single shared password from
// TURBORG_GATEWAY_PASSWORD.
package web

import (
	"crypto/subtle"
	"errors"
)

// TokenVerifier is the plug point for Gateway auth. Implementations
// return true only if the token authenticates the caller; constant-time
// comparison is the implementation's job.
type TokenVerifier interface {
	Verify(token string) bool
}

// StaticPasswordVerifier compares the WS-supplied token against a single
// shared secret using crypto/subtle.ConstantTimeCompare so a timing
// side-channel can't be used to learn the password byte-by-byte.
type StaticPasswordVerifier struct {
	password []byte
}

func NewStaticPasswordVerifier(password string) (*StaticPasswordVerifier, error) {
	if password == "" {
		return nil, errors.New("web: static password cannot be empty")
	}
	return &StaticPasswordVerifier{password: []byte(password)}, nil
}

func (s *StaticPasswordVerifier) Verify(token string) bool {
	if token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), s.password) == 1
}
