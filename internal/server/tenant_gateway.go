package server

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/web"
)

// Web-shell brute-force defaults. The pooled gateway has no env layer of its
// own (the single-instance path sources these from config.Settings in
// runtime.buildGateway); a tenant's token is a strong per-tenant secret, but
// the router faces a public proxy, so a lockout still gates guessing.
// Mirrors the GATEWAY_* config defaults so both modes behave the same.
const (
	gatewayMaxFailedAttempts = 5
	gatewayFailureWindow     = 60 * time.Second
	gatewayLockoutWindow     = 5 * time.Minute
)

// buildTenantGateway constructs a pooled tenant's web shell around its live IRC
// connector. The connector is both the gateway's IRC bridge and its outbound
// Sender (same wiring as runtime.buildGateway's web.New(ircConn, ircConn, …) —
// kept in lockstep so the single-instance and pooled web shells stay one behaviour).
//
// No Host/Port: the pooled gateway never binds its own listener — the web
// router serves the shared Handler() per request. No MessageStore: durable
// pooled scrollback is a follow-up; live streaming works without it.
func buildTenantGateway(bridge *irc.Connector, token string, log *slog.Logger, store messages.Store, llmProvider llm.Provider, tbSummarizeCap int, onActivity func(reason string), webhookFire func(name string, bag map[string]string) bool) (*web.Gateway, error) {
	verifier, err := web.NewStaticPasswordVerifier(token)
	if err != nil {
		return nil, fmt.Errorf("gateway verifier: %w", err)
	}
	rl, err := irc.NewRateLimiter(gatewayMaxFailedAttempts, gatewayFailureWindow, gatewayLockoutWindow, nil)
	if err != nil {
		return nil, fmt.Errorf("gateway ratelimit: %w", err)
	}
	gw, err := web.New(bridge, bridge, web.Options{
		Verifier:               verifier,
		RateLimiter:            rl,
		Log:                    log,
		MessageStore:           store,
		LLMProvider:            llmProvider,
		TBSummarizeMaxMessages: tbSummarizeCap,
		// Owner-presence from the pooled web shell marks the tenant active
		// in the pool's coalescing aggregator (same sink the bouncer attach
		// hook uses) — engaged web session / owner /tb keep it alive.
		OnActivity: onActivity,
		// Inbound-webhook ingress: POST /c/<id>/hook/<name> dispatches to this
		// tenant's flow/skill engines through the live wiring.
		WebhookFire: webhookFire,
	})
	if err != nil {
		return nil, err
	}
	return gw, nil
}
