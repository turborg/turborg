# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
release-please maintains the entries below from Conventional Commits — manual
edits will survive but the bot will keep adding new entries above.

## [Unreleased]

## [0.1.0] — first Go release

Feature parity with the final Python release ([turborg/turborg-python](https://github.com/turborg/turborg-python) v0.8.0).
Every `TURBORG_*` env var, every WebSocket protocol op, and the reference
UI's HTML/CSS/JS are preserved byte-for-byte so the xshellz sidecar +
accounts-api + appui stack needs no changes.

### Features

- **Core**: `Agent` with `errgroup`-based supervision, `EventBus` pub/sub
  with panic-isolated handlers, `CommandRegistry` with prefix parsing +
  global / per-command guard composition, `InboundEnvelope` /
  `OutboundEnvelope` with `ReplyTo` factory that routes DMs back to the
  sender.
- **IRC**: TLS dial, IRCv3 CAP REQ (`server-time`, `account-tag`, `sasl`),
  SASL PLAIN with mid-flight PING/PONG, NickServ IDENTIFY,
  `RPL_ENDOFMOTD`/`ERR_NOMOTD` handshake-complete signaling, full
  `ChannelState` cache (members + mode prefixes, topic + setter, NAMES
  completion), CTCP auto-reply with per-sender Throttle, server-PING
  keep-alive + idle-read timeout (the silent-death fix), bouncer with
  PASS auth + per-IP RateLimiter + state replay + per-channel ring
  buffer + bidirectional message relay.
- **LLM**: `Provider` interface with `Ask` + `Stream` (Go 1.23 `iter.Seq2`).
  Anthropic backend uses the official `anthropic-sdk-go` with streaming
  + prompt caching (`CacheControlEphemeralParam` on system blocks).
- **Web**: `coder/websocket` gateway with the full JSON-over-WS protocol
  (state / message / join / part / kick / nick / topic / names / server /
  mode_changed / whois_result / list_result / who_result / join_failed
  server→client, say / join / part / nick / mode / kick / whois / topic /
  list / who / raw client→server). Per-channel + server-log replay rings.
  Token via `?token=` or `Authorization: Bearer`. Idle-shutdown timer
  for free-tier SaaS deployments. `/health` + `/metrics` endpoints.
  Reference UI served at `/` via `go:embed`.
- **Runtime**: `BuildAgent` composes everything from settings, mutually-
  stopping `agent.Run` + `gateway.Serve` pair so SIGTERM unwinds in
  milliseconds. Cobra CLI with `turborg run` + `--version`.
- **Hive**: stub interface + `NoopClient` for future `hive.xshellz.com`.

### Infrastructure

- multi-stage Dockerfile → `gcr.io/distroless/static-debian12:nonroot`
  (~10 MB image, single static binary).
- GoReleaser builds linux/amd64 + linux/arm64 archives and a multi-arch
  container image at `ghcr.io/turborg/turborg`.
- CI on Go 1.25 + 1.26 with race detector + coverage gate
  (`go-test-coverage`).
- release-please drives version bumps + CHANGELOG.
- dependabot covers gomod, github-actions, and docker.

[Unreleased]: https://github.com/turborg/turborg/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/turborg/turborg/releases/tag/v0.1.0
