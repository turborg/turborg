// Package web is turborg's hosted web-chat connector: a JSON-over-WebSocket
// chat surface turborg serves itself (no external network to dial). It
// implements the connector-agnostic agent.Connector + agent.Actor contracts,
// so the same commands, skills, and flows that run on IRC run here unchanged.
//
// Distinct from internal/web, which is the IRC-specific management gateway —
// that streams an IRC session to a dashboard and is not a chat transport.
package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Claims is the decoded web-chat token payload. It binds a token to one
// tenant, one room, and one role, with an expiry. The JSON field names are a
// wire contract shared with the token signer that mints these — renaming one
// breaks every already-issued token, so treat them as frozen.
type Claims struct {
	// TenantID is the turborg the token authorizes. VerifyClaims rejects a
	// token whose TenantID doesn't match the tenant resolved from the request
	// path, so a token minted for one tenant can't be replayed against another.
	TenantID string `json:"tid"`
	// Room is the chat room the token authorizes (e.g. "console").
	Room string `json:"room"`
	// Role is the caller's role in the room ("owner", …), surfaced to handlers
	// via the inbound envelope metadata.
	Role string `json:"role"`
	// VisitorID identifies the human the token was minted for (a user id).
	VisitorID string `json:"vid"`
	// IssuedAt / ExpiresAt are unix seconds. VerifyClaims rejects an expired
	// token; IssuedAt is informational.
	IssuedAt  int64 `json:"iat"`
	ExpiresAt int64 `json:"exp"`
}

// SignedTokenVerifier authenticates web-chat tokens against one tenant's
// signing key using HMAC-SHA256. The token format is JWT-style:
//
//	token   = base64url(payloadJSON) "." base64url(HMAC_SHA256(base64url(payloadJSON), key))
//
// base64url is RFC 4648 §5 with no padding, and the HMAC is computed over the
// base64url payload STRING (not the raw JSON). The key is the tenant's own
// per-tenant secret — there is no fleet-wide key — so a token signed for the
// wrong tenant already fails the HMAC; VerifyClaims additionally checks the
// tenant id in the payload as defence-in-depth.
type SignedTokenVerifier struct {
	key []byte
	// now is injected so tests can pin the clock for expiry checks. Nil uses
	// time.Now.
	now func() time.Time
}

// NewSignedTokenVerifier builds a verifier over a tenant's signing key. An
// empty key is rejected — a zero-length HMAC key would authenticate a
// trivially-forgeable token.
func NewSignedTokenVerifier(key string) (*SignedTokenVerifier, error) {
	if key == "" {
		return nil, errors.New("web: signing key cannot be empty")
	}
	return &SignedTokenVerifier{key: []byte(key)}, nil
}

// Errors returned by VerifyClaims. Callers should treat every one as a plain
// 401 with no detail — distinguishing them to the client would let an attacker
// probe which check failed.
var (
	ErrMalformedToken = errors.New("web: malformed token")
	ErrBadSignature   = errors.New("web: bad token signature")
	ErrExpiredToken   = errors.New("web: token expired")
	ErrWrongTenant    = errors.New("web: token tenant mismatch")
	ErrWrongRoom      = errors.New("web: token room mismatch")
)

// VerifyClaims authenticates token and returns its claims, or an error if any
// check fails. The checks, in order:
//
//  1. Recompute the HMAC over the payload segment and constant-time compare it
//     to the presented signature.
//  2. Reject if the token has expired.
//  3. Reject if the payload's tenant id != the tenant resolved from the request
//     path (pathTenantID). MANDATORY: the pooled router resolves the tenant
//     from the URL, so without this a token minted for tenant A could be
//     replayed against /chat/B.
//  4. Reject if the payload's room != the room this connector serves.
func (v *SignedTokenVerifier) VerifyClaims(token, pathTenantID, room string) (*Claims, error) {
	// Split on the LAST '.' so a '.' inside the base64url payload (there won't
	// be one, but be defensive) doesn't misalign the segments.
	dot := strings.LastIndexByte(token, '.')
	if dot <= 0 || dot == len(token)-1 {
		return nil, ErrMalformedToken
	}
	payloadSeg, sigSeg := token[:dot], token[dot+1:]

	mac := hmac.New(sha256.New, v.key)
	mac.Write([]byte(payloadSeg))
	expected := mac.Sum(nil)

	presented, err := base64.RawURLEncoding.DecodeString(sigSeg)
	if err != nil {
		return nil, ErrMalformedToken
	}
	if !hmac.Equal(presented, expected) {
		return nil, ErrBadSignature
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(payloadSeg)
	if err != nil {
		return nil, ErrMalformedToken
	}
	var claims Claims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, ErrMalformedToken
	}

	now := time.Now
	if v.now != nil {
		now = v.now
	}
	if claims.ExpiresAt > 0 && now().Unix() >= claims.ExpiresAt {
		return nil, ErrExpiredToken
	}
	if claims.TenantID != pathTenantID {
		return nil, ErrWrongTenant
	}
	if claims.Room != room {
		return nil, ErrWrongRoom
	}
	return &claims, nil
}
