# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
release-please maintains the entries below from Conventional Commits — manual
edits will survive but the bot will keep adding new entries above.

## [0.1.1](https://github.com/turborg/turborg/compare/v0.1.0...v0.1.1) (2026-05-14)


### Bug Fixes

* **irc:** match Python's CTCP VERSION reply, drop turborg-go leftovers ([#6](https://github.com/turborg/turborg/issues/6)) ([5fb480c](https://github.com/turborg/turborg/commit/5fb480c033eb4171477665bbef6fedf219ae4259))
* **test:** determinize agent drain test, restore reliable 98% coverage ([#11](https://github.com/turborg/turborg/issues/11)) ([4d0002b](https://github.com/turborg/turborg/commit/4d0002b6a8df6ef2f80641cb2354765e18b7f8f4))
* **test:** stabilize two flakes exposed by PR [#8](https://github.com/turborg/turborg/issues/8) ([#10](https://github.com/turborg/turborg/issues/10)) ([e429ac2](https://github.com/turborg/turborg/commit/e429ac2bcf5a8708e6aaddfd7abbda1bb15f538f))


### Documentation

* simplify README, add runnable example + coverage badge + docker quickstart ([#9](https://github.com/turborg/turborg/issues/9)) ([24a1476](https://github.com/turborg/turborg/commit/24a147656870f627d20d6a4a629d26f9f8ae8163))

## 0.1.0 (2026-05-14)


### Features

* **agent:** phase 1 — Envelope + EventBus + CommandRegistry ([1deeb02](https://github.com/turborg/turborg/commit/1deeb0262ccd9f9f2c192063ed2a775c5a95620f))
* **hive:** phase 6 — hive.Client interface + NoopClient stub ([2377fc1](https://github.com/turborg/turborg/commit/2377fc11f998e9c170a047cee08f09633595148b))
* initial PoC — agent + IRC echo connector ([4c8d844](https://github.com/turborg/turborg/commit/4c8d8444206d1f1e296a5d4a432d398ef8487fed))
* **irc/bouncer:** advertise the standard IRCv3 cap set ([fa739f7](https://github.com/turborg/turborg/commit/fa739f7f228057a8a415e306397b5fea5ef6bbad))
* **irc/bouncer:** advertise znc.in/self-message for older HexChat / weechat ([22bd402](https://github.com/turborg/turborg/commit/22bd402cf0d04e78501ae005905690d91ad46bfc))
* **irc:** phase 2a/2b — IRCv3 parser, codes, ChannelState ([d78f4e2](https://github.com/turborg/turborg/commit/d78f4e2daecfd2247cb295faca75cd9a4f94a004))
* **irc:** phase 2c/2e — Settings (env-var contract) + RateLimiter + Throttle ([a511ed2](https://github.com/turborg/turborg/commit/a511ed2bc577df71d03c605ccf07cca5ad7f2cf2))
* **irc:** phase 2f — bouncer (TCP server with state replay + relay) ([1722ef9](https://github.com/turborg/turborg/commit/1722ef9d6f051596404b5f461c4e43aaa9328e3c))
* **irc:** phase 2g — wire state, events, CTCP, and bouncer into the connector ([995e62f](https://github.com/turborg/turborg/commit/995e62f2878cbccf64ee2c8c47956282e7c2b975))
* **llm:** phase 3 — LLM provider interface + Anthropic backend ([e31a326](https://github.com/turborg/turborg/commit/e31a326c0987645ccbabe7b792e9b733cb774c62))
* **runtime:** phase 5 — config + runtime composition + cobra CLI ([071d71e](https://github.com/turborg/turborg/commit/071d71e4bc7e081889ba2e0b5022e449b0448cb5))
* **web:** phase 4 — WebSocket gateway + bundled reference UI ([133ec60](https://github.com/turborg/turborg/commit/133ec60e0554661acec734ed2889a1d3cea5e121))


### Bug Fixes

* **irc,web:** outbound DEBUG, idle timeout + client PING, MESSAGE_SENT envelope ([5e084f7](https://github.com/turborg/turborg/commit/5e084f793d398651caf713ec5ae77cf88c3925a8))
* **irc/bouncer:** defer welcome until CAP END (IRCv3 registration suspension) ([6a4c625](https://github.com/turborg/turborg/commit/6a4c6257da377524dc7ab41c27655cd39d62b960))
* **irc/bouncer:** suppress self-DM fan for clients without echo-message ([e17d43c](https://github.com/turborg/turborg/commit/e17d43c0140e7518895c9d56649dc329e58dc55d))
* **irc/bouncer:** use real upstream ident/host in fan-out prefix ([8d3f840](https://github.com/turborg/turborg/commit/8d3f840b2c8644cec061bd25fedc500835c10191))
* **irc:** echo-message cap + EventChannelNames broadcast ([e070d9f](https://github.com/turborg/turborg/commit/e070d9f85ea0d6a16dea9f8e35538f82af9c51e7))
* **irc:** revert DM gate + pre-bind bouncer listener at startup ([d6528b2](https://github.com/turborg/turborg/commit/d6528b2f04e682263caad53102aee150f74fff20))
* **irc:** track live nick + log every outbound write ([2c75805](https://github.com/turborg/turborg/commit/2c75805bf3a1300114d1b21966bf385fd7884765))
* **web:** own send visible to sender; remove duplicate MESSAGE event ([29c1aae](https://github.com/turborg/turborg/commit/29c1aaee5d5d1ca35e2b9aded3be361ef05ce68c))


### Documentation

* phase 7c — top-level repo meta files ([3b55cb4](https://github.com/turborg/turborg/commit/3b55cb48ea2ed2fd301388ee4482b5554c1843a1))


### Chores

* pin first release to v0.1.0 ([1c2c164](https://github.com/turborg/turborg/commit/1c2c164118832e76b8bc735d1fffbae284855ef1))

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
