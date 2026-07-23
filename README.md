# turborg

**A Go framework for chat-network agents — an IRC bouncer + browser UI + bot orchestrator in one static binary. Optional LLM hookup for AI features.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go ≥1.26](https://img.shields.io/badge/go-%E2%89%A51.26-00ADD8.svg)](https://go.dev/dl/)
[![CI](https://github.com/turborg/turborg/actions/workflows/ci.yml/badge.svg)](https://github.com/turborg/turborg/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-94%25-brightgreen.svg)](.testcoverage.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/turborg/turborg)](https://goreportcard.com/report/github.com/turborg/turborg)

## What it does

turborg connects an IRC nick to a network, joins channels, runs commands you register, and (optionally) lets a real human attach a HexChat / irssi / mIRC client through a built-in bouncer or use a browser-based reference UI through a built-in WebSocket gateway. All in one static binary, no runtime dependencies.

- **A real IRC connector** — TLS, SASL PLAIN, NickServ identify, IRCv3 `server-time` + `account-tag`, full PRIVMSG / JOIN / PART / KICK / NICK / TOPIC tracking, per-channel state cached from the wire.
- **A built-in IRC bouncer** — local clients connect to the bot's loopback port, authenticate with a password, and tunnel through the bot's upstream session. The bot stays in control while a human can lurk and chat from their own client.
- **A WebSocket gateway + reference web UI** — vanilla-JS single-page client at `/` with channel sidebar, member list, slash commands, IndexedDB-backed scrollback, browser notifications. The protocol is stable and documented so embedders can swap in their own UI.
- **A normalized `Envelope`** — the same handler that runs on IRC will also run on Discord / Telegram / Web when those connectors land.
- **Data-driven commands** — the bot ships with none. Define commands as data (a trigger name, a `static` or `llm` type, a template, and an access policy); they're loaded from config and dispatched at runtime. No recompile to add or change one.
- **Optional LLM, not the centerpiece** — point a command at an LLM (Anthropic, or any OpenAI-compatible endpoint via the router) and it answers prompts. Leave the LLM unset and the rest of the bot doesn't care.

The design separates **how the bot talks to a network** (the connector) from **how it thinks** (any LLM, or none) from **what it does** (your handlers).

## Quickstart: run the binary

The bot ships as a single binary. Install it, set three env vars, run.

```bash
go install github.com/turborg/turborg/cmd/turborg@latest
```

Or grab a release from the [releases page](https://github.com/turborg/turborg/releases), or pull the container:

```bash
docker pull ghcr.io/turborg/turborg:latest   # multi-arch (amd64+arm64), distroless
```

Minimum config — three env vars and you're online:

```bash
export TURBORG_IRC_HOSTNAME=irc.libera.chat
export TURBORG_IRC_NICK=myturborg
export TURBORG_IRC_CHANNELS=#turborg-test
export TURBORG_CUSTOM_COMMANDS_MAX=-1
export TURBORG_COMMANDS='[{"name":"ping","type":"static","template":"pong","access":"everyone"}]'
turborg run
```

In `#turborg-test`, type `!ping` — the bot replies `pong`, because that's the command you just defined. Swap the template, add more, or point a `"type":"llm"` command at a model — all without recompiling.

### Optional extras (each independent)

| Want to…                                | Set this                                          |
|------------------------------------------|---------------------------------------------------|
| Back `llm`-type commands with an LLM     | `TURBORG_LLM_API_KEY=sk-...` (`TURBORG_LLM_PROVIDER=anthropic` by default) |
| Attach HexChat through the bouncer       | `TURBORG_IRC_BOUNCER_PASSWORD=…`                  |
| Open the web UI at `http://127.0.0.1:8765/` | `TURBORG_GATEWAY_PASSWORD=…`                   |

See [`docs/connectors/irc.md`](docs/connectors/irc.md) for the IRC connector reference (SASL, NickServ identify, bouncer, rate limits, timing) and [`docs/gateway.md`](docs/gateway.md) for the gateway (WS protocol, auth, idle-shutdown).

### Observability

When the gateway is enabled, it exposes two unauthenticated, non-cacheable endpoints on the gateway address (by default `http://127.0.0.1:8765`):

- `GET /health` returns JSON with `status` (`"ok"`), `uptime_seconds`, and `ws_clients`. Use it for a liveness check.
- `GET /metrics` returns Prometheus text exposition with counters for successful WebSocket authentications (`turborg_ws_connections_total`), failed authentication attempts (`turborg_ws_auth_failures_total`), and IRC messages forwarded to WebSocket clients (`turborg_messages_forwarded_total`), plus gauges for connected clients (`turborg_ws_clients_current`) and uptime (`turborg_uptime_seconds`).

For example, a Prometheus server that can reach the gateway can scrape it with:

```yaml
scrape_configs:
  - job_name: turborg
    static_configs:
      - targets: ["turborg:8765"]
    metrics_path: /metrics
```

Keep the gateway on a trusted network or publish it behind appropriate network controls: these endpoints do not require the gateway password.

## Quickstart: a minimal bot in Go

If you want to read the code end-to-end or hack on it, a runnable ~40-line example lives in [`examples/turborg_irc/main.go`](examples/turborg_irc/main.go):

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "strings"
    "syscall"

    "github.com/turborg/turborg/internal/agent"
    "github.com/turborg/turborg/internal/connector/irc"
)

func main() {
    ircCfg, err := irc.LoadSettings()
    if err != nil {
        log.Fatal(err)
    }
    _ = ircCfg.Validate()

    a := agent.New(nil)
    a.AddConnector(irc.New(ircCfg, nil, a.Events))

    a.Commands.Register("echo", func(_ context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
        return agent.ReplyTo(env, strings.Join(args, " ")), nil
    }, nil)

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    _ = a.Run(ctx)
}
```

Same env vars as the CLI quickstart:

```bash
export TURBORG_IRC_HOSTNAME=irc.libera.chat
export TURBORG_IRC_NICK=myturborg
export TURBORG_IRC_CHANNELS=#turborg-test
go run ./examples/turborg_irc
```

The agent ships with no commands; `!echo hello world` is the one this example registers in code. (The binary instead loads commands from `TURBORG_COMMANDS` — see the CLI quickstart above.) Ctrl-C unwinds cleanly within ~500ms.

## Run with Docker

The published image is `ghcr.io/turborg/turborg:latest` (multi-arch `linux/amd64` + `linux/arm64`, distroless/static).

**Inline env vars — fastest path:**

```bash
docker run --rm \
  -e TURBORG_IRC_HOSTNAME=irc.libera.chat \
  -e TURBORG_IRC_NICK=myturborg \
  -e TURBORG_IRC_CHANNELS=#turborg-test \
  ghcr.io/turborg/turborg:latest
```

**With an .env file:**

```bash
cp .env.example .env       # fill in TURBORG_IRC_* (+ optional LLM / web / bouncer)
docker run --rm --env-file .env ghcr.io/turborg/turborg:latest
```

**docker compose:**

```bash
docker compose up          # same .env file, plus port mappings if you enabled the web UI
```

Pin a specific version in production — `ghcr.io/turborg/turborg:0.1.1` rather than `:latest`. Image tags drop the `v` prefix from the git tag (`v0.1.1` → `:0.1.1`); see the [releases page](https://github.com/turborg/turborg/releases) for available versions.

To expose the web UI to the host, also publish the port:

```bash
docker run --rm \
  -e TURBORG_GATEWAY_PASSWORD=changeme \
  -e TURBORG_GATEWAY_HOST=0.0.0.0 \
  -p 8765:8765 \
  --env-file .env \
  ghcr.io/turborg/turborg:latest
```

## Connectors

| Connector  | Status     | Notes                                                                  |
|------------|------------|------------------------------------------------------------------------|
| [IRC](docs/connectors/irc.md) | Stable | with bouncer + WS gateway — see [the IRC connector docs](docs/connectors/irc.md) for every env var |
| Discord    | Roadmap    | hopper                                                                 |
| Telegram   | Roadmap    | hopper                                                                 |
| WhatsApp   | Roadmap    | hopper                                                                 |

LLM providers are **optional**. turborg runs perfectly fine as a pure command-driven bot without any LLM. When you want one, Anthropic (default, with prompt caching) ships in-tree.

## Project layout

```
turborg/
├── cmd/
│   ├── turborg/                 single-bot entry point — cobra + config + runtime
│   ├── turborg-server/          pooled runtime — many bots in one process
│   ├── devirc/                  tiny IRC stub for local dev
│   └── loadgen/                 footprint/load harness for the pooled runtime
├── examples/turborg_irc/        minimal embeddable example (runnable)
├── internal/
│   ├── agent/                   Agent, EventBus, CommandRegistry, Envelope
│   ├── connector/irc/           IRC connector + bouncer + protocol + state
│   ├── llm/                     Provider interface + budget; anthropic/ + openaicompat/
│   ├── web/                     WebSocket gateway + embedded reference UI
│   ├── commands/                data-driven command definitions → handlers/guards
│   ├── messages/ messagesink/   channel-history store + durable forwarding
│   ├── statepush/ activity/     optional state + activity webhooks
│   ├── budgetrefresh/ commandrefresh/  optional live budget + command hot-reload
│   ├── ident/                   source-port → ident registry (RFC-1413 backing)
│   ├── server/                  pooled multi-instance runtime internals
│   ├── config/                  TURBORG_* settings (env-driven)
│   ├── runtime/                 build the wired agent from settings
│   ├── hive/                    Hive client (noop stub today)
│   ├── logging/ safe/           slog handler factory + goroutine helpers
│   └── version/                 single Version constant
├── tests/fixtures/              FakeIRCServer + FakeConnector for tests
└── .github/workflows/           ci + release + cla
```

## Testing

```bash
make test                  # go test -race -count=1 -timeout 120s ./...
make cover                 # tests + coverage profile + total summary
make cover-gate            # enforce per-package + total thresholds (.testcoverage.yml)
make lint                  # golangci-lint run ./...
```

CI runs the same gates plus a Go 1.26 test job and a build smoke test. **Total coverage gate: ≥ 94%.**

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the dev setup, branching strategy, and PR rules. By submitting a contribution you agree to the [Contributor License Agreement](CLA.md) — the cla-assistant bot will guide you on first PR.

- Conventional Commits for PR titles (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`)
- Squash-merge to `main`; linear history
- gofmt-formatted, golangci-lint clean
- `go test -race` green, coverage gate green

## License

[Apache License 2.0](LICENSE) — see [TRADEMARKS.md](TRADEMARKS.md) for the trademark policy on the names "turborg" and "xshellz".

## Security

Found a vulnerability? Please **do not** open a public issue. See [SECURITY.md](SECURITY.md) for the responsible-disclosure process.

---

*Part of the [**xshellz**](https://www.xshellz.com) ecosystem. The future hosted hive lives at [hive.xshellz.com](https://hive.xshellz.com).*
