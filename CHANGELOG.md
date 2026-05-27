# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
release-please maintains the entries below from Conventional Commits — manual
edits will survive but the bot will keep adding new entries above.

## [0.2.1](https://github.com/turborg/turborg/compare/v0.2.0...v0.2.1) (2026-05-27)


### Bug Fixes

* **irc:** forward WHOIS/WHO over the bouncer; surface CTCP notices on web ([f74baba](https://github.com/turborg/turborg/commit/f74baba3a39694468a994bd25ce3bd7d39666d7f))

## [0.2.0](https://github.com/turborg/turborg/compare/v0.1.2...v0.2.0) (2026-05-25)


### ⚠ BREAKING CHANGES

* **gateway:** Anything setting TURBORG_WEB_PASSWORD, TURBORG_WEB_HOST, TURBORG_WEB_PORT, TURBORG_WEB_MAX_FAILED_ATTEMPTS, TURBORG_WEB_FAILURE_WINDOW_SECONDS, TURBORG_WEB_LOCKOUT_SECONDS, or TURBORG_WEB_IDLE_SHUTDOWN_SECONDS must rename to TURBORG_IRC_WEB_*. Without TURBORG_IRC_WEB_PASSWORD, irc.Settings.WebEnabled() returns false and the WS gateway silently does not start.

### Features

* bouncer rejection feedback — state machine, channel-targeted NOTICEs, WS state events ([#17](https://github.com/turborg/turborg/issues/17)) ([ee669e6](https://github.com/turborg/turborg/commit/ee669e69e82ac7e7c3dcff2ec6292830a8fa29b1))
* **config:** operator policy envs for network / identity / throttle limits ([#15](https://github.com/turborg/turborg/issues/15)) ([675ad77](https://github.com/turborg/turborg/commit/675ad77126f9d3c5913c87ec4204648cd6c44d17))
* **connector/irc:** active PING/PONG roundtrip watchdog ([#19](https://github.com/turborg/turborg/issues/19)) ([55c9a9c](https://github.com/turborg/turborg/commit/55c9a9cfd9a822e5ec208bf6889f8b4e59c2bce0))
* **irc:** configurable QUIT body via TURBORG_IRC_QUIT_MESSAGE ([#20](https://github.com/turborg/turborg/issues/20)) ([6a15462](https://github.com/turborg/turborg/commit/6a154629fecb8d3480c72d5892261fbb01b850d3))
* **messages:** durable message mirror via MessageRecorder ([#24](https://github.com/turborg/turborg/issues/24)) ([a8b5420](https://github.com/turborg/turborg/commit/a8b54203308622eb5ab88e5025a66a91ba15c356))
* **messages:** shared Store + WS history op + IRCv3 CHATHISTORY + tagged replay ([#27](https://github.com/turborg/turborg/issues/27)) ([8c8278b](https://github.com/turborg/turborg/commit/8c8278bdaa3ff8a9fe26247184f5c40518bf1951))
* **runtime:** owner-mode + auth-mode for !commands authorization ([#28](https://github.com/turborg/turborg/issues/28)) ([1ed046f](https://github.com/turborg/turborg/commit/1ed046fcfaabb8a37dde777918d8fd4f6369fc7b))
* **runtime:** TURBORG_IGNORED_NICKS gates !commands at the guard ([863c806](https://github.com/turborg/turborg/commit/863c8069269b6fec93a5059d7b0714fe573c8a55))
* **server:** pooled multi-tenant runtime (M1–M5, M7, M8) ([#30](https://github.com/turborg/turborg/issues/30)) ([d3152e2](https://github.com/turborg/turborg/commit/d3152e23d180708b7313baac338c7762f9b0ac78))
* **statepush:** state webhook emitter — debounced per-connector snapshots ([#18](https://github.com/turborg/turborg/issues/18)) ([6aa2fc4](https://github.com/turborg/turborg/commit/6aa2fc4155b75649804dc766c2b4c38708ad3bee))


### Bug Fixes

* **irc:** bound the upstream dial with DialTimeout ([#31](https://github.com/turborg/turborg/issues/31)) ([8a271ff](https://github.com/turborg/turborg/commit/8a271ff2534518cf03f883006f556346710d8bd0))
* **irc:** remove unsynchronized slice read from NOTICE test assertion ([#34](https://github.com/turborg/turborg/issues/34)) ([aec3321](https://github.com/turborg/turborg/commit/aec33213b061e9866f4b5f84dcdbc0e4983d4307))
* **irc:** surface service/user NOTICEs in the web UI ([#32](https://github.com/turborg/turborg/issues/32)) ([b5cbab0](https://github.com/turborg/turborg/commit/b5cbab0d57ef95471f6cd15f0ad977fcb1364828))
* upstream reconnect sync (backoff, members, gateway initial state) + devirc dev tool ([#25](https://github.com/turborg/turborg/issues/25)) ([1c88ee5](https://github.com/turborg/turborg/commit/1c88ee5748888f83bce5a9ca782f1eda49e88c31))


### Refactoring

* **gateway:** move WS gateway env to TURBORG_GATEWAY_* ([#14](https://github.com/turborg/turborg/issues/14)) ([5c989b8](https://github.com/turborg/turborg/commit/5c989b8642c29e3c1f01243dd405a92dc547b106))

## [0.1.2](https://github.com/turborg/turborg/compare/v0.1.1...v0.1.2) (2026-05-14)


### Bug Fixes

* **version:** make Version a var so -X injection actually works ([#12](https://github.com/turborg/turborg/issues/12)) ([dd0b055](https://github.com/turborg/turborg/commit/dd0b055285d32e7db4b91850114c11aa859a473c))

## [0.1.1](https://github.com/turborg/turborg/compare/v0.1.0...v0.1.1) (2026-05-14)


### Bug Fixes

* **irc:** standardize the CTCP VERSION reply ([#6](https://github.com/turborg/turborg/issues/6)) ([5fb480c](https://github.com/turborg/turborg/commit/5fb480c033eb4171477665bbef6fedf219ae4259))
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

## [0.1.0] — initial release

First public release of turborg. Ships the full framework: agent core,
IRC connector with bouncer + WebSocket gateway, Anthropic LLM backend,
and a bundled reference UI.

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
