# turborg

**A Go framework for chat-network agents — an IRC bouncer + browser UI + bot orchestrator in one static binary. Optional LLM hookup for AI features.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go ≥1.25](https://img.shields.io/badge/go-%E2%89%A51.25-00ADD8.svg)](https://go.dev/dl/)
[![CI](https://github.com/turborg/turborg/actions/workflows/ci.yml/badge.svg)](https://github.com/turborg/turborg/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/turborg/turborg)](https://goreportcard.com/report/github.com/turborg/turborg)

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "github.com/turborg/turborg/internal/agent"
    "github.com/turborg/turborg/internal/connector/irc"
)

func main() {
    a := agent.New(nil)
    a.AddConnector(irc.New(&irc.Settings{
        Hostname: "irc.libera.chat",
        Nick:     "myturborg",
        Channels: []string{"#turborg-test"},
    }, nil, a.Events))

    a.Commands.Register("ping", func(_ context.Context, env *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
        return agent.ReplyTo(env, "pong"), nil
    }, nil)

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    _ = a.Run(ctx)
}
```

That's a working IRC bot — no LLM required. Add the WebGateway and the Bouncer and you've also got a browser-side IRC client and a HexChat/irssi tunnel through the same process — all in a single ~5 MB static binary.

---

## What turborg gives you

- **A real IRC connector**, not a toy — TLS, SASL PLAIN, NickServ identify, IRCv3 `server-time` + `account-tag`, full PRIVMSG / JOIN / PART / KICK / NICK / TOPIC tracking, and per-channel state cached from the wire. See [docs/connectors/irc.md](docs/connectors/irc.md).
- **A built-in IRC bouncer.** Local clients (HexChat, irssi, mIRC) connect to the bot's loopback port, authenticate with a password, and tunnel through the bot's upstream session. The bot stays in control while a human can lurk and chat from their own client.
- **A WebSocket gateway + reference web UI.** A vanilla-JS single-page client at `/` with channel sidebar, member list, slash commands, autocomplete, IndexedDB-backed per-channel scrollback that survives reloads, browser notifications, and clear UX for kicks/bans. SaaS deployments can swap in their own polished UI; the protocol is stable and documented.
- **A normalized `Envelope`** so the same handler that runs on IRC will also run on Discord / Telegram / Web when those connectors land.
- **Optional LLM**, not the centerpiece. If you set an API key, `!ask` is wired in (Anthropic Claude with prompt caching by default). If you don't, the rest of the bot doesn't care.

The design separates **how the bot talks to a network** (the connector) from **how it thinks** (any LLM, or none) from **what it does** (your handlers).

For the long-term vision — **hive.xshellz.com**, a shared-intelligence cloud any turborg instance can attach to — see [docs/hive.md](docs/hive.md).

## Status

**v0.1.0** — first Go release. Ported byte-for-byte from the Python `turborg` (now archived at [`turborg/turborg-python`](https://github.com/turborg/turborg-python)) with full feature parity. Same TURBORG_* env-var contract, same WS protocol JSON, same reference UI. The Python implementation is frozen at v0.8.0; new work happens here.

| Connector  | Status     | Notes                                  |
|------------|------------|----------------------------------------|
| IRC        | Stable     | with bouncer + WS gateway              |
| Discord    | Roadmap    | hopper                                 |
| Telegram   | Roadmap    | hopper                                 |
| WhatsApp   | Roadmap    | hopper                                 |

LLM providers are **optional**. turborg runs perfectly fine as a pure command-driven bot without any LLM. When you do want one, Anthropic (default, with prompt caching) ships in-tree.

## Install

```bash
go install github.com/turborg/turborg/cmd/turborg@latest
```

Or grab a pre-built binary from the [releases page](https://github.com/turborg/turborg/releases), or pull the container image:

```bash
docker pull ghcr.io/turborg/turborg:latest
```

The container is ~10 MB (distroless/static, multi-arch linux/amd64 + linux/arm64).

Configure via environment variables (everything is `TURBORG_*`-prefixed) or a `.env` file. The most common minimum:

```bash
TURBORG_IRC_HOSTNAME=irc.libera.chat
TURBORG_IRC_NICK=myturborg
TURBORG_IRC_CHANNELS=#turborg-test
```

See [docs/configuration.md](docs/configuration.md) for the full list — SASL, NickServ identify, the bouncer, the web gateway, idle-shutdown, rate limits, logging.

## Quickstart

```bash
export TURBORG_IRC_HOSTNAME=irc.libera.chat
export TURBORG_IRC_NICK=myturborg
export TURBORG_IRC_CHANNELS=#turborg-test
turborg run
```

You're online. To enable the AI command, also set `TURBORG_ANTHROPIC_API_KEY`. To attach HexChat through the built-in bouncer, set `TURBORG_IRC_BOUNCER_PASSWORD`. To open the reference web UI at `http://127.0.0.1:8765/`, set `TURBORG_WEB_PASSWORD`. Each is independent — pick what you need.

## Run with Docker

```bash
cp .env.example .env       # fill in TURBORG_IRC_* (and optionally LLM / web / bouncer)
docker compose up
```

Or without compose:

```bash
docker run --env-file .env ghcr.io/turborg/turborg:latest
```

## Documentation

- [Quickstart](docs/quickstart.md) — get a bot online in 5 minutes
- [Architecture](docs/architecture.md) — the agent / connector / LLM / Envelope model
- [IRC connector](docs/connectors/irc.md) — bouncer, SASL, web gateway, channel state
- [Configuration](docs/configuration.md) — every environment variable, every default
- [LLM providers (optional)](docs/llm-providers.md) — Anthropic (default) with streaming + prompt caching
- [Writing a connector](docs/writing-a-connector.md) — add Discord, Telegram, your own
- [Hive](docs/hive.md) — the future shared-intelligence cloud

## Project layout

```
turborg/
├── cmd/turborg/                 entry point — wires cobra + config + runtime
├── internal/
│   ├── agent/                   Agent, EventBus, CommandRegistry, Envelope
│   ├── connector/
│   │   └── irc/                 IRC connector + bouncer + protocol + state
│   ├── llm/
│   │   └── anthropic/           Anthropic Claude with streaming + prompt caching
│   ├── web/                     WebSocket gateway + embedded reference UI
│   ├── config/                  TURBORG_* settings (env-driven)
│   ├── runtime/                 build the wired agent from settings
│   ├── hive/                    Hive client (noop stub today)
│   ├── logging/                 slog handler factory
│   └── version/                 single Version constant (release-please bumps)
├── tests/fixtures/              FakeIRCServer + FakeConnector for tests
├── .github/workflows/           ci + release + cla
├── Dockerfile                   multi-stage → distroless/static-debian12
└── docs/                        full documentation
```

## Testing

```bash
make test                  # go test -race -count=1 -timeout 120s ./...
make cover                 # tests + coverage profile + total summary
make cover-gate            # enforce per-package + total thresholds (.testcoverage.yml)
make lint                  # golangci-lint run ./...
```

CI runs the same gates plus a Go 1.25/1.26 test matrix and a build smoke test (`turborg --version`).

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the dev setup, branching strategy, and PR rules. By submitting a contribution you agree to the [Contributor License Agreement](CLA.md) — the cla-assistant bot will guide you on first PR.

## License

[Apache License 2.0](LICENSE) — see [TRADEMARKS.md](TRADEMARKS.md) for the trademark policy on the names "turborg" and "xshellz".

## Security

Found a vulnerability? Please **do not** open a public issue. See [SECURITY.md](SECURITY.md) for the responsible-disclosure process.

## Style

- Conventional Commits for PR titles (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`)
- Squash-merge to `main`; linear history
- gofmt-formatted, golangci-lint clean
- `go test -race` with coverage; **fail under 90% total**

---

*Part of the [**xshellz**](https://www.xshellz.com) ecosystem. The future hosted hive lives at [hive.xshellz.com](https://hive.xshellz.com).*
