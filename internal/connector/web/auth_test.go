package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// farFuture is an exp well past any test run, so a valid token never expires
// mid-test. Year ~2100.
const farFuture = 4102444800

// mintToken signs a web-chat token exactly as the canonical signer does:
// base64url(payloadJSON) "." base64url(HMAC_SHA256(base64url(payloadJSON), key)),
// RFC 4648 §5 with no padding, HMAC over the base64url payload STRING. This is
// the reference the PHP WebChatTokenService must match byte-for-byte, so a Go
// round-trip against it is the format-compatibility test.
func mintToken(t *testing.T, key string, c Claims) string {
	t.Helper()
	payload, err := json.Marshal(c)
	require.NoError(t, err)
	seg := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(seg))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return seg + "." + sig
}

func newVerifier(t *testing.T, key string) *SignedTokenVerifier {
	t.Helper()
	v, err := NewSignedTokenVerifier(key)
	require.NoError(t, err)
	return v
}

func TestNewSignedTokenVerifierRejectsEmptyKey(t *testing.T) {
	_, err := NewSignedTokenVerifier("")
	assert.Error(t, err)
}

func TestVerifyClaimsRoundTrip(t *testing.T) {
	key := "tenant-a-container-token"
	claims := Claims{
		TenantID: "tenant-a", Room: "console", Role: "owner",
		VisitorID: "user-1", IssuedAt: 1000, ExpiresAt: farFuture,
	}
	tok := mintToken(t, key, claims)

	got, err := newVerifier(t, key).VerifyClaims(tok, "tenant-a", "console")
	require.NoError(t, err)
	assert.Equal(t, "tenant-a", got.TenantID)
	assert.Equal(t, "owner", got.Role)
	assert.Equal(t, "user-1", got.VisitorID)
}

func TestVerifyClaimsRejectsTamperedPayload(t *testing.T) {
	key := "k"
	tok := mintToken(t, key, Claims{TenantID: "tenant-a", Room: "console", ExpiresAt: farFuture})

	// Re-encode a different payload but keep the original signature → the
	// recomputed HMAC no longer matches.
	forged, _ := json.Marshal(Claims{TenantID: "tenant-a", Room: "console", Role: "owner", ExpiresAt: farFuture})
	tampered := base64.RawURLEncoding.EncodeToString(forged) + "." + tok[len(tok)-43:]

	_, err := newVerifier(t, key).VerifyClaims(tampered, "tenant-a", "console")
	assert.ErrorIs(t, err, ErrBadSignature)
}

func TestVerifyClaimsRejectsWrongKey(t *testing.T) {
	tok := mintToken(t, "real-key", Claims{TenantID: "tenant-a", Room: "console", ExpiresAt: farFuture})
	_, err := newVerifier(t, "attacker-key").VerifyClaims(tok, "tenant-a", "console")
	assert.ErrorIs(t, err, ErrBadSignature)
}

// TestVerifyClaimsRejectsCrossTenant is the core defence: a token minted for
// tenant A, presented against tenant B's path, must fail closed even when B's
// signing key is used (it isn't here — keys are per-tenant — but the explicit
// tid check is the belt-and-braces the plan mandates).
func TestVerifyClaimsRejectsCrossTenant(t *testing.T) {
	key := "shared-somehow"
	tok := mintToken(t, key, Claims{TenantID: "tenant-a", Room: "console", ExpiresAt: farFuture})
	_, err := newVerifier(t, key).VerifyClaims(tok, "tenant-b", "console")
	assert.ErrorIs(t, err, ErrWrongTenant)
}

func TestVerifyClaimsRejectsWrongRoom(t *testing.T) {
	key := "k"
	tok := mintToken(t, key, Claims{TenantID: "tenant-a", Room: "console", ExpiresAt: farFuture})
	_, err := newVerifier(t, key).VerifyClaims(tok, "tenant-a", "lobby")
	assert.ErrorIs(t, err, ErrWrongRoom)
}

func TestVerifyClaimsRejectsExpired(t *testing.T) {
	key := "k"
	tok := mintToken(t, key, Claims{TenantID: "tenant-a", Room: "console", ExpiresAt: 1000})
	_, err := newVerifier(t, key).VerifyClaims(tok, "tenant-a", "console")
	assert.ErrorIs(t, err, ErrExpiredToken)
}

func TestVerifyClaimsRejectsMalformed(t *testing.T) {
	v := newVerifier(t, "k")
	for _, tok := range []string{"", "nodot", ".", "abc.", ".abc"} {
		_, err := v.VerifyClaims(tok, "tenant-a", "console")
		assert.ErrorIs(t, err, ErrMalformedToken, "token %q", tok)
	}
}
