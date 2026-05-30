# IRC connector

The reference connector that ships in v0.1. Speaks RFC 1459 + IRCv3
extensions, maintains per-channel state from the wire, and offers two
optional fan-out surfaces:

- A built-in **bouncer** that local IRC clients (HexChat / irssi / mIRC)
  can attach to and tunnel through the bot's upstream session.
- A built-in **WebSocket gateway + reference UI** for browser clients
  (or for any custom frontend that wants real-time JSON events).

All settings live under the `TURBORG_IRC_*` prefix and are read by
`internal/connector/irc/settings.go`. Connection + nick are required;
everything else has a sensible default.

---

## Quickstart — the minimum that works

```bash
export TURBORG_IRC_HOSTNAME=irc.libera.chat
export TURBORG_IRC_NICK=myturborg
export TURBORG_IRC_CHANNELS=#turborg-test
turborg run
```

That's it. The bot connects with TLS on port 6697 and joins
`#turborg-test`. Define what it responds to in `TURBORG_COMMANDS` (see the
README). Everything below this line is optional.

---

## Connection

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_HOSTNAME` | string | **required** | Upstream IRC server (hostname or IP). |
| `TURBORG_IRC_PORT` | int | `6697` | TLS port by default. Use `6667` if you set `USE_TLS=false`. |
| `TURBORG_IRC_USE_TLS` | bool | `true` | Connect with TLS. **Don't disable this on the public internet** — IRC traffic includes your NickServ password in plaintext otherwise. |
| `TURBORG_IRC_NICK` | string | **required** | Bot nick. Must be unique on the network — if your nick is in use, registration fails and the bot exits. |
| `TURBORG_IRC_USERNAME` | string | = `NICK` | The IRC ident (the `USER` command's first arg). Most networks display this in WHOIS. Defaults to the nick when unset. |
| `TURBORG_IRC_REAL_NAME` | string | `"turborg agent"` | The USER command's trailing arg — shown on WHOIS as the "real name". Operators sometimes use this to flag the bot as a bot. |
| `TURBORG_IRC_CHANNELS` | CSV | `""` | Comma-separated list of channels to join after registration. Bare names without a `#` are auto-prefixed (`mychannel` → `#mychannel`); `&`, `+`, `!` prefixes are preserved. Also accepts a JSON-array shape (`["#a","#b"]`) for compatibility with config systems that emit lists. |

### Operational notes

- **Channel keys (passwords) aren't a separate var.** If you need to
  join a `+k` channel, send the JOIN manually from a built-in command
  or the bouncer. The connector intentionally doesn't model per-channel
  keys in the spawn config — keep keys out of env that may end up in
  process listings or container introspection.
- **NICK collisions are fatal.** The connector does not append `_` or
  cycle alt-nicks; if your registered nick is in use, registration
  fails with `433 Nickname is already in use` and the agent exits.
  Operators monitor the bot and restart it once the collision clears.

---

## Timing

These three knobs control how aggressively the connector detects a
silently broken upstream connection and gives up on a stalled
handshake.

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_HANDSHAKE_TIMEOUT` | duration | `30s` | If the server hasn't sent `001 RPL_WELCOME` within this window, the connector aborts the connection. Bump to `60s` on slow services-heavy networks where SASL adds round-trips. |
| `TURBORG_IRC_READ_IDLE_TIMEOUT` | duration | `300s` | If no bytes arrive from upstream for this long, the connector treats the link as dead and reconnects. Includes PING responses, so a healthy link is well under the limit. |
| `TURBORG_IRC_CLIENT_PING_INTERVAL` | duration | `120s` | How often the connector sends `PING` upstream. **Must be strictly less than `READ_IDLE_TIMEOUT`** — `Settings.Validate()` rejects configs where ping ≥ idle, because the silent-death timer would fire while a PING is in flight and trigger a false-positive disconnect. |

Durations use Go syntax: `30s`, `5m`, `1h30m`.

### Operational notes

- The healthy ratio is ping interval ≤ ⅓ of read idle. Default 120s /
  300s gives 2× headroom; the connector sends two PINGs before the
  idle timer fires, so a single dropped PONG won't reset the link.
- If you see frequent reconnect loops on a slow link, **raise
  `READ_IDLE_TIMEOUT` first**, not the ping interval. The validator
  enforces `ping < idle` so lowering the ping won't help; raising idle
  buys headroom on both sides.

---

## Authentication

Three independent paths. Use whichever your network supports. Set
multiple together for defense-in-depth — SASL during the CAP handshake
covers identity early, NickServ identify after the handshake covers
networks that NAK SASL.

### Server PASS (rare)

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_SERVER_PASSWORD` | string | `""` | Sent as `PASS <password>` during registration. Only needed on networks that gate connections behind a server-wide password. |

### NickServ identify

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_NICKSERV_PASSWORD` | string | `""` | After the handshake completes (but before joining channels), the connector sends `PRIVMSG NickServ :IDENTIFY <password>`. Use on networks where the nick is registered with NickServ. Sent **after** the link is up, so the password is protected by TLS if `USE_TLS=true`. |

### SASL PLAIN (preferred)

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_SASL_USER` | string | `""` | SASL PLAIN account name. Both this and `SASL_PASSWORD` must be set to enable. |
| `TURBORG_IRC_SASL_PASSWORD` | string | `""` | SASL PLAIN password. |

When both are set, the connector negotiates `CAP REQ :sasl` and runs
`AUTHENTICATE PLAIN` during the handshake — identity is verified
**before** the bot is allowed to join channels. Bad credentials surface
as an error and abort startup. Networks that don't support SASL `NAK`
the capability and the connector falls back to unauthenticated.

### Which to use?

- **Libera.Chat / OFTC / modern IRCv3 networks** → SASL. It happens
  before the nick is locked, before channels are joined, before any
  trust assumption is baked in.
- **Older networks (no SASL CAP)** → NickServ identify. Works on
  anything from the last 25 years.
- **Defense-in-depth** → set both. SASL succeeds on networks that
  support it; NickServ runs after the handshake either way (harmless if
  you're already authenticated).
- **Don't ship the literal placeholder strings** from `.env.example`
  (`__your_nickserv_account__` etc.). They'll fail upstream auth and
  the bot will exit.

---

## Channel events (CTCP)

CTCP is the in-band protocol IRC clients use for `/me` actions,
`VERSION` queries, file transfers, etc. The connector auto-replies to a
small set (`VERSION`, `PING`, `TIME`) and silently drops the rest.

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_CTCP_AUTO_REPLY` | bool | `true` | Set `false` to silently ignore every inbound CTCP. The bot still parses them — this just suppresses the outbound reply. |
| `TURBORG_IRC_CTCP_MAX_PER_WINDOW` | int | `3` | Per-sender sliding-window cap. Excess CTCPs are dropped silently (no reply) to avoid being used as a reflection-amplification vector. |
| `TURBORG_IRC_CTCP_WINDOW_SECONDS` | int | `30` | Length of the sliding window. |

### Operational notes

- The throttle scope is **per sender** — a single user spamming
  `VERSION` can't lock other users out of their own CTCP replies.
- The drop is **silent**. A throttled CTCP returns no reply at all,
  matching how a real IRCd would NOP the client out of the conversation.

---

## Bouncer

A TCP server on a loopback port that local IRC clients (HexChat,
irssi, mIRC) attach to. Each attached client authenticates with a
password, then tunnels through the bot's upstream session — sending
PRIVMSGs as the bot, seeing the bot's joined channels, the bot's
PMs, etc.

The bouncer **does not** ship channel scrollback by default; clients
see traffic from the moment they attach. (The WS gateway does cache
scrollback; see below.)

| Variable | Type | Default | Notes |
|---|---|---|---|
| `TURBORG_IRC_BOUNCER_PASSWORD` | string | `""` | **Set this to enable the bouncer.** Unset = bouncer disabled, no listener. |
| `TURBORG_IRC_BOUNCER_HOST` | string | `127.0.0.1` | Listen interface. **Keep on loopback** unless you put a TLS-terminating proxy in front. Plain-text IRC over the public internet is unsafe. Use `0.0.0.0` to expose on a LAN you control. |
| `TURBORG_IRC_BOUNCER_PORT` | int | `31337` | Bouncer TCP port. Pick something out of well-known ranges. |
| `TURBORG_IRC_BOUNCER_RATELIMIT_ENABLED` | bool | `true` | Per-IP brute-force protection on the auth handshake. Disable only if you have an upstream rate-limiter (e.g. behind a reverse proxy that already gates the port). |
| `TURBORG_IRC_BOUNCER_MAX_FAILED_ATTEMPTS` | int | `5` | Failed auths per IP before lockout. |
| `TURBORG_IRC_BOUNCER_FAILURE_WINDOW_SECONDS` | int | `60` | Sliding-window length for counting failures. |
| `TURBORG_IRC_BOUNCER_LOCKOUT_SECONDS` | int | `300` | How long an IP is locked out once it trips the threshold. |

### Operational notes

- The listener **pre-binds before upstream Dial**. Attached clients can
  connect on bot startup without seeing a connection-refused window
  during the upstream handshake.
- **IRCv3 CAP negotiation suspends registration.** Per spec the
  bouncer must not send `001 Welcome` to an attached client until that
  client sends `CAP END`. The connector handles this correctly; if you
  see clients hanging at registration, check that their CAP version is
  compatible.
- **HexChat 2.16 has a known self-message-cap quirk.** It silently
  skips advertising the `echo-message` cap, so the bouncer fans
  self-DMs to every attached client regardless of whether the client
  advertised the cap. The wrong-tab artifact is a HexChat bug; using a
  newer client (or HexChat ≥ 2.17 once released) resolves it.

---

## Gateway (cross-reference)

The bot also exposes a control-plane gateway (WS protocol + reference
UI) that today streams IRC events and accepts `say`/`join`/`kick` ops
flowing back through the agent. The gateway is **not** an IRC
sub-feature — it's a top-level surface that just happens to surface
IRC events while IRC is the only connector. See
[`docs/gateway.md`](../gateway.md) for the full reference.

---

## Owner / authorization

Owner-gating isn't IRC-specific — it lives on top-level `TURBORG_*` —
but it directly affects which IRC users can run `!commands`:

- `TURBORG_OWNER_NICK` — case-insensitive nick match.
- `TURBORG_OWNER_ACCOUNT` — IRCv3 `account-tag` match. **Fails closed**
  if the network has no services / the client hasn't been
  authenticated, so this is the safer of the two.

By default (both unset) **anyone** in a channel can run `!commands`.
Set both for defense-in-depth on services-enabled networks.

See `.env.example` and the top-level docs for the full list of
cross-cutting `TURBORG_*` settings (logging, command throttle, LLM,
hive).

---

## Cross-references

- **Sample env file:** [`/.env.example`](../../.env.example)
- **Source of truth:** `internal/connector/irc/settings.go` — every
  field here has an `env:"…"` tag matching the variable name above.
- **Gateway internals (SaaS-only):** `docs/backend/PLAN.md` (gitignored;
  shared with the xshellz orchestrator team out-of-band).
- **WS protocol + reference UI gap list (SaaS-only):** `docs/ui/PLAN.md`
  (gitignored; shared with the xshellz Angular team out-of-band).
