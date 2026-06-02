# Gateway

turborg's **web gateway** is a single HTTP server that
exposes:

- `/ws` — a JSON-over-WebSocket protocol that streams agent events to
  connected clients and accepts command ops (`say`, `join`, `part`,
  `kick`, `topic`, `whois`, `list`, `who`, `raw`).
- `/` — a bundled vanilla-JS single-page reference UI that speaks the
  WS protocol.
- `/health`, `/metrics` — operational endpoints.

It is **not a connector**. A connector originates and consumes
Envelopes from an external chat network; the gateway is a *client of
the agent* that lets web clients observe and control it. Today the
events it streams are IRC events (the only connector that ships in
v0.1), but the env vars don't name a transport or a connector — the
same names survive any future protocol/connector additions.

All gateway settings live under `TURBORG_GATEWAY_*`.

---

## Quickstart

```bash
export TURBORG_GATEWAY_PASSWORD=hunter2
turborg run
```

Then open <http://127.0.0.1:8765/> and log in with the password. The
reference UI shows joined channels, member lists, scrollback, and a
message composer.

---

## Configuration

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_GATEWAY_PASSWORD` | string | `""` | **Set this to enable the gateway.** Unset = gateway disabled, no listener. Embedders can swap `StaticPasswordVerifier` for a JWT- or session-aware `TokenVerifier`; the wire protocol is unchanged. |
| `TURBORG_GATEWAY_HOST` | string | `127.0.0.1` | Listen interface. **Keep on loopback** unless you put a TLS-terminating reverse proxy in front — WebSocket auth tokens travel in cleartext over plain HTTP. |
| `TURBORG_GATEWAY_PORT` | int | `8765` | HTTP listen port. |
| `TURBORG_GATEWAY_MAX_FAILED_ATTEMPTS` | int | `5` | Per-IP brute-force cap on `/ws` auth. |
| `TURBORG_GATEWAY_FAILURE_WINDOW_SECONDS` | int | `60` | Failure-counter window. |
| `TURBORG_GATEWAY_LOCKOUT_SECONDS` | int | `300` | Lockout duration after the threshold trips. |
| `TURBORG_GATEWAY_IDLE_SHUTDOWN_SECONDS` | int | `0` (disabled) | When > 0, the gateway calls its `OnIdleShutdown` callback N seconds after the **last WebSocket client** disconnects. An embedder can wire this to a process/container stop for idle auto-pause; the bundled binary just logs it. Bouncer connections do **not** extend the timer — only WS clients do. |

---

## Operational notes

- The reference UI at `/` is intentionally **vanilla JS / single file /
  no build step**. Polished, production-grade frontend work belongs in
  a separate client built against the same WS protocol.
- The wire protocol is byte-stable across patch releases — embedders
  with their own UI can rely on this.
- **Port 0 is legal** in code (it tells `net.Listen` to pick an
  OS-assigned port). The env default is `8765`, so production
  deployments always get a stable port; port-0 is reserved for tests.
- The gateway's `IRCBridge` interface is the v0.1 wiring. When future
  connectors land, this is expected to generalize to a
  `ConnectorBridge` so a single gateway can surface events from
  multiple connectors at once — the `TURBORG_GATEWAY_*` env names are
  chosen to survive that refactor without another rename.

---

## Cross-references

- **Sample env file:** [`/.env.example`](../.env.example)
- **Source of truth:** `internal/web/gateway.go`, `internal/config/settings.go`
  (the `Gateway*` fields), and the bundled reference UI in
  `internal/web/static/index.html`.
