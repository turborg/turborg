# turborg

**A Go framework for chat-network agents — an IRC bouncer + browser UI + bot orchestrator in one static binary. Optional LLM hookup for AI features.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go ≥1.25](https://img.shields.io/badge/go-%E2%89%A51.25-00ADD8.svg)](https://go.dev/dl/)
[![CI](https://github.com/turborg/turborg/actions/workflows/ci.yml/badge.svg)](https://github.com/turborg/turborg/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-95%25-brightgreen.svg)](.testcoverage.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/turborg/turborg)](https://goreportcard.com/report/github.com/turborg/turborg)

## What it does

turborg connects an IRC nick to a network, joins channels, runs commands you register, and (optionally) lets a real human attach a HexChat / irssi / mIRC client through a built-in bouncer or use a browser-based reference UI through a built-in WebSocket gateway. All in one ~5 MB static binary.

- **A real IRC connector** — TLS, SASL PLAIN, NickServ identify, IRCv3 `server-time` + `account-tag`, full PRIVMSG / JOIN / PART / KICK / NICK / TOPIC tracking, per-channel state cached from the wire.
- **A built-in IRC bouncer** — local clients connect to the bot's loopback port, authenticate with a password, and tunnel through the bot's upstream session. The bot stays in control while a human can lurk and chat from their own client.
- **A WebSocket gateway + reference web UI** — vanilla-JS single-page client at `/` with channel sidebar, member list, slash commands, IndexedDB-backed scrollback, browser notifications. The protocol is stable and documented so SaaS deployments can swap in their own UI.
- **A normalized `Envelope`** — the same handler that runs on IRC will also run on Discord / Telegram / Web when those connectors land.
- **Optional LLM, not the centerpiece** — set an API key and `!ask` is wired in (Anthropic Claude with prompt caching). Leave it unset and the rest of the bot doesn't care.

The design separates **how the bot talks to a network** (the connector) from **how it thinks** (any LLM, or none) from **what it does** (your handlers).

## Quickstart: run the binary

The bot ships as a single binary. Install it, set three env vars, run.

```bash
go install github.com/turborg/turborg/cmd/turborg@latest
```

Or grab a release from the [releases page](https://github.com/turborg/turborg/releases), or pull the container:

```bash
docker pull ghcr.io/turborg/turborg:latest   # ~10 MB, multi-arch
```

Minimum config — three env vars and you're online:

```bash
export TURBORG_IRC_HOSTNAME=irc.libera.chat
export TURBORG_IRC_NICK=myturborg
export TURBORG_IRC_CHANNELS=#turborg-test
turborg run
```

In `#turborg-test`, type `!ping` — the bot replies `pong`. That's it.

### Optional extras (each independent)

| Want to…                                | Set this                                          |
|------------------------------------------|---------------------------------------------------|
| Enable the `!ask` AI command             | `TURBORG_ANTHROPIC_API_KEY=sk-...`                |
| Attach HexChat through the bouncer       | `TURBORG_IRC_BOUNCER_PASSWORD=…`                  |
| Open the web UI at `http://127.0.0.1:8765/` | `TURBORG_WEB_PASSWORD=…`                       |

See `docs/configuration.md` for the full env-var reference — SASL, NickServ identify, rate limits, idle-shutdown, logging.

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

`!ping`, `!help`, `!version` are pre-registered builtins. `!echo hello world` is the custom command this example adds. Ctrl-C unwinds cleanly within ~500ms.

## Run with Docker

The published image is `ghcr.io/turborg/turborg:latest` (multi-arch `linux/amd64` + `linux/arm64`, ~10 MB, distroless/static).

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

Pin a specific version in production — `ghcr.io/turborg/turborg:v0.1.0` rather than `:latest`. Tags follow the GitHub releases.

To expose the web UI to the host, also publish the port:

```bash
docker run --rm \
  -e TURBORG_WEB_PASSWORD=changeme \
  -e TURBORG_WEB_HOST=0.0.0.0 \
  -p 8765:8765 \
  --env-file .env \
  ghcr.io/turborg/turborg:latest
```

## Connectors

| Connector  | Status     | Notes                                  |
|------------|------------|----------------------------------------|
| IRC        | Stable     | with bouncer + WS gateway              |
| Discord    | Roadmap    | hopper                                 |
| Telegram   | Roadmap    | hopper                                 |
| WhatsApp   | Roadmap    | hopper                                 |

LLM providers are **optional**. turborg runs perfectly fine as a pure command-driven bot without any LLM. When you want one, Anthropic (default, with prompt caching) ships in-tree.

## Project layout

```
turborg/
├── cmd/turborg/                 entry point — wires cobra + config + runtime
├── examples/turborg_irc/        minimal embeddable example (runnable)
├── internal/
│   ├── agent/                   Agent, EventBus, CommandRegistry, Envelope
│   ├── connector/irc/           IRC connector + bouncer + protocol + state
│   ├── llm/anthropic/           Anthropic Claude with streaming + prompt caching
│   ├── web/                     WebSocket gateway + embedded reference UI
│   ├── config/                  TURBORG_* settings (env-driven)
│   ├── runtime/                 build the wired agent from settings
│   ├── hive/                    Hive client (noop stub today)
│   ├── logging/                 slog handler factory
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

CI runs the same gates plus a Go 1.25/1.26 test matrix and a build smoke test. **Total coverage gate: ≥ 95%.**

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
