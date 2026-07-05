package server

import (
	"fmt"

	webconn "github.com/turborg/turborg/internal/connector/web"
)

// webSettingsFromConnectorSpec maps a tenant's web ConnectorSpec (the feed wire
// shape: config map) onto web.Settings plus the token verifier the connector
// authenticates every attaching browser against. Pure — no IO.
//
// signingKey is the tenant's gateway_token (= its container_token): the HMAC key
// web-chat tokens are signed with. There is no fleet-wide key by design, so the
// per-tenant token doubles as the signing secret; an empty key disables the web
// connector (there is nothing to verify against).
func webSettingsFromConnectorSpec(cs ConnectorSpec, signingKey string) (webconn.Settings, *webconn.SignedTokenVerifier, error) {
	if signingKey == "" {
		return webconn.Settings{}, nil, fmt.Errorf("web connector: empty signing key")
	}
	verifier, err := webconn.NewSignedTokenVerifier(signingKey)
	if err != nil {
		return webconn.Settings{}, nil, fmt.Errorf("web connector: %w", err)
	}
	s := webconn.Settings{
		BotNick: stringFieldOr(cs.Config, "bot_nick", "turborg"),
		Room:    stringFieldOr(cs.Config, "room", "console"),
		Public:  boolField(cs.Config, "public", false),
	}
	return s, verifier, nil
}
