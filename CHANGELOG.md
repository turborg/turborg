# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
release-please maintains the entries below from Conventional Commits — manual
edits will survive but the bot will keep adding new entries above.

## [0.16.0](https://github.com/turborg/turborg/compare/v0.15.2...v0.16.0) (2026-06-08)


### Features

* **connector/irc:** apply nick + channel changes live, without a reconnect ([#113](https://github.com/turborg/turborg/issues/113)) ([1153c38](https://github.com/turborg/turborg/commit/1153c3828acfad9b97532d7b0cdb27ed6e076955))
* **connector/irc:** park & resume the upstream link (Suspend/Resume + nick recovery) ([#118](https://github.com/turborg/turborg/issues/118)) ([47750c5](https://github.com/turborg/turborg/commit/47750c535a6d394a81de5af0cb08a85d3221998f))
* **connector/irc:** reconnect hardening + nick fallback/reclaim (anti-flood) ([#112](https://github.com/turborg/turborg/issues/112)) ([7e30f62](https://github.com/turborg/turborg/commit/7e30f622f702b14362593671e83fe598c080435d))
* **connector/irc:** report desired nick distinctly; adopt a self-rename as intent ([#114](https://github.com/turborg/turborg/issues/114)) ([a8983af](https://github.com/turborg/turborg/commit/a8983afdf9a87e384dc3bf58c9d7101e0772ed41))
* **statepush:** include server-supplied reason in connector snapshot ([#111](https://github.com/turborg/turborg/issues/111)) ([137e497](https://github.com/turborg/turborg/commit/137e4975de517da6c47a1745ad864cc33c379bfe))


### Bug Fixes

* **connector/irc:** live nick correctness — no false disconnect, self-detect by live nick, push on change ([#115](https://github.com/turborg/turborg/issues/115)) ([2fd9650](https://github.com/turborg/turborg/commit/2fd965016798972b3cf6917f8f1c7b08d27c4226))
* **connector/irc:** register ident before the TLS handshake ([#110](https://github.com/turborg/turborg/issues/110)) ([9bbae6a](https://github.com/turborg/turborg/commit/9bbae6a69cc296887045c3662178eff1a8272fd1))
* **web:** set Prometheus exposition Content-Type on /metrics ([#108](https://github.com/turborg/turborg/issues/108)) ([abb5db8](https://github.com/turborg/turborg/commit/abb5db8fa74f1b4235b6db87032443e036baae8c))

## [0.15.2](https://github.com/turborg/turborg/compare/v0.15.1...v0.15.2) (2026-06-06)


### Bug Fixes

* **server:** apply ignore-list edits in place without a reconnect ([#86](https://github.com/turborg/turborg/issues/86)) ([35f2c86](https://github.com/turborg/turborg/commit/35f2c86ef782753c1b56a560244179d0abffe500))

## [0.15.1](https://github.com/turborg/turborg/compare/v0.15.0...v0.15.1) (2026-06-02)


### Bug Fixes

* **web:** scope nick-change notice to channels the user shares ([645bcc6](https://github.com/turborg/turborg/commit/645bcc667e6ce062e7a2a598b1c12cb38401d67c))

## [0.15.0](https://github.com/turborg/turborg/compare/v0.14.0...v0.15.0) (2026-06-02)


### Features

* **egress:** bind per-tenant source IP for pooled egress round-robin ([#84](https://github.com/turborg/turborg/issues/84)) ([3c93c7a](https://github.com/turborg/turborg/commit/3c93c7ae103dd057d508800ead127c4271c9dc6f))


### Documentation

* align comments and docs with framework-neutral framing ([#82](https://github.com/turborg/turborg/issues/82)) ([18104f3](https://github.com/turborg/turborg/commit/18104f3ee18032a4f5d690a9bff17679d74afc74))

## [0.14.0](https://github.com/turborg/turborg/compare/v0.13.0...v0.14.0) (2026-06-02)


### Features

* **ident:** serve per-tenant ident over an HTTP router for the sidecar responder ([#80](https://github.com/turborg/turborg/issues/80)) ([50c198c](https://github.com/turborg/turborg/commit/50c198c9c8b2138db03020a1a008523c3e7e879d))

## [0.13.0](https://github.com/turborg/turborg/compare/v0.12.0...v0.13.0) (2026-06-01)


### Features

* **commands:** connector-agnostic static-skill template placeholders ([#74](https://github.com/turborg/turborg/issues/74)) ([027ec1d](https://github.com/turborg/turborg/commit/027ec1dbc4a86efe36e1c85e7a9c5fa2a49a2478))

## [0.12.0](https://github.com/turborg/turborg/compare/v0.11.0...v0.12.0) (2026-06-01)


### Features

* **connector/irc:** add /tb tldr &lt;url&gt; command ([#72](https://github.com/turborg/turborg/issues/72)) ([94415bf](https://github.com/turborg/turborg/commit/94415bf1f7054022dae92bf5af6f7613e65eaf02))
* **connector/irc:** extract title/description metadata for /tb tldr ([b52db4d](https://github.com/turborg/turborg/commit/b52db4d8fe885e26b3122ba79137c7f40317772e))


### Bug Fixes

* **connector/irc:** harden /tb tldr fetch against SSRF edges and prompt-injection breakout ([5b6e4df](https://github.com/turborg/turborg/commit/5b6e4df3924aecb26cdfe66776874a3a68c81958))
* **connector/irc:** raise /tb tldr body cap so large pages' metadata is reached ([f794f39](https://github.com/turborg/turborg/commit/f794f39cffc671a91a2903dd58f4be4dac37bd19))

## [0.11.0](https://github.com/turborg/turborg/compare/v0.10.0...v0.11.0) (2026-06-01)


### Features

* **activity:** count only genuine owner presence for idle auto-pause ([#70](https://github.com/turborg/turborg/issues/70)) ([e63ab59](https://github.com/turborg/turborg/commit/e63ab5954666463cd8c4e10fd5ebaeae0b60965e))

## [0.10.0](https://github.com/turborg/turborg/compare/v0.9.1...v0.10.0) (2026-06-01)


### Features

* **connector/irc:** gate AI history commands behind channel-op consent on strict networks ([#68](https://github.com/turborg/turborg/issues/68)) ([b0924f9](https://github.com/turborg/turborg/commit/b0924f97e977b0a1daf7638f8d89d70f5252e5d2))
* **runtime:** live-reload data-driven commands on a single agent ([#66](https://github.com/turborg/turborg/issues/66)) ([ab25af6](https://github.com/turborg/turborg/commit/ab25af6fc4be9a7ace7f9372534f5f5af66db4aa))

## [0.9.1](https://github.com/turborg/turborg/compare/v0.9.0...v0.9.1) (2026-05-30)


### Bug Fixes

* **llm:** seed rolling-window budget with prior usage so the cap survives restarts ([#64](https://github.com/turborg/turborg/issues/64)) ([c313a5e](https://github.com/turborg/turborg/commit/c313a5e5d0a0770cdae1f7b95c5d8c4f73cbcc86))

## [0.9.0](https://github.com/turborg/turborg/compare/v0.8.1...v0.9.0) (2026-05-30)


### Features

* **commands:** data-driven user commands + LLM router ([#61](https://github.com/turborg/turborg/issues/61)) ([58b1518](https://github.com/turborg/turborg/commit/58b1518b0b7f35328008bb593422445e5d394c96))
* **llm:** token tracking + rolling-24h budget enforcement ([#63](https://github.com/turborg/turborg/issues/63)) ([911159b](https://github.com/turborg/turborg/commit/911159bb757cefe51b752edf69aa2c30b53964d1))

## [0.8.1](https://github.com/turborg/turborg/compare/v0.8.0...v0.8.1) (2026-05-28)


### Bug Fixes

* **irc:** degrade gracefully on a failed initial connect ([#59](https://github.com/turborg/turborg/issues/59)) ([0fdbe7c](https://github.com/turborg/turborg/commit/0fdbe7cb2ec806a757b5483099e1a5a5693fd6f3))

## [0.8.0](https://github.com/turborg/turborg/compare/v0.7.0...v0.8.0) (2026-05-28)


### Features

* **server:** apply host QUIT brand + per-tier CTCP/bouncer caps to pooled ([#57](https://github.com/turborg/turborg/issues/57)) ([3f47844](https://github.com/turborg/turborg/commit/3f47844fbfa5edb54ac4ee29cebba7ed798d3daa))

## [0.7.0](https://github.com/turborg/turborg/compare/v0.6.0...v0.7.0) (2026-05-28)


### Features

* **runtime:** unify dedicated + pooled agent wiring via WireCommon ([#54](https://github.com/turborg/turborg/issues/54)) ([2e8a541](https://github.com/turborg/turborg/commit/2e8a541d27ca440b83d226260e2c35f882588682))
* **server:** full pooled parity — activity, durable history, nudge, throttle ([#55](https://github.com/turborg/turborg/issues/55)) ([559e1b4](https://github.com/turborg/turborg/commit/559e1b4d49ccfb934203009a912d704bc37585a3))


### Bug Fixes

* **server:** wire bouncer password for pooled tenants ([#52](https://github.com/turborg/turborg/issues/52)) ([2978ea7](https://github.com/turborg/turborg/commit/2978ea764fecada7a3f40bc8623efdb2904c0eab))

## [0.6.0](https://github.com/turborg/turborg/compare/v0.5.0...v0.6.0) (2026-05-28)


### Features

* **irc:** Settings.ApplyDefaults — shared defaults for hand-built settings ([#50](https://github.com/turborg/turborg/issues/50)) ([88f4443](https://github.com/turborg/turborg/commit/88f4443662c43ec4c28fe066400bf77c56150455))

## [0.5.0](https://github.com/turborg/turborg/compare/v0.4.0...v0.5.0) (2026-05-28)


### Features

* **server:** pooled connector state-sync ([#48](https://github.com/turborg/turborg/issues/48)) ([d4744ee](https://github.com/turborg/turborg/commit/d4744eecccc256b226633cb1fd699b5abaf0d66c))

## [0.4.0](https://github.com/turborg/turborg/compare/v0.3.1...v0.4.0) (2026-05-28)


### Features

* **server:** pooled web shell gateway + router ([#46](https://github.com/turborg/turborg/issues/46)) ([12f07f3](https://github.com/turborg/turborg/commit/12f07f3ea7862a1107946b170d011f64fd46471c))

## [0.3.1](https://github.com/turborg/turborg/compare/v0.3.0...v0.3.1) (2026-05-27)


### Bug Fixes

* **release:** ship /turborg-server in the published image ([#44](https://github.com/turborg/turborg/issues/44)) ([81e7660](https://github.com/turborg/turborg/commit/81e76606bd44aff8a6494eb8ecc4f077d6424298))

## [0.3.0](https://github.com/turborg/turborg/compare/v0.2.3...v0.3.0) (2026-05-27)


### Features

* **irc:** bouncer pings attached clients to keep idle attachments alive ([#39](https://github.com/turborg/turborg/issues/39)) ([e36d97f](https://github.com/turborg/turborg/commit/e36d97f0d08293c9cddbebb5e0238f33e80ac205))
* **irc:** reconnect-storm circuit breaker ([#43](https://github.com/turborg/turborg/issues/43)) ([2d4f386](https://github.com/turborg/turborg/commit/2d4f38620241c29a8608eb465302e43ade7830ea))
* pooled bouncer ingress (M6 — SNI/PROXY-v2 routing) ([#41](https://github.com/turborg/turborg/issues/41)) ([d5b9edc](https://github.com/turborg/turborg/commit/d5b9edcd54d4c2881a49dab01a2b0b7df4b65238))
* **server,irc:** pooled resource governance — GOMEMLIMIT, watchdog, slow-consumer ([#42](https://github.com/turborg/turborg/issues/42)) ([fa3b52b](https://github.com/turborg/turborg/commit/fa3b52b6ed22905a2ea0a3a0fd9f80f07b44c8bc))

## [0.2.3](https://github.com/turborg/turborg/compare/v0.2.2...v0.2.3) (2026-05-27)


### Documentation

* **changelog:** point 0.2.1 whois/CTCP entry at the current commit SHA ([6887a7a](https://github.com/turborg/turborg/commit/6887a7a0c35c0329043ba9ffe1360cbf58de80db))

## [0.2.2](https://github.com/turborg/turborg/compare/v0.2.1...v0.2.2) (2026-05-27)


### Bug Fixes

* **irc:** per-client WHOIS/WHO; surface the bot's own CTCP replies ([#36](https://github.com/turborg/turborg/issues/36)) ([5a39245](https://github.com/turborg/turborg/commit/5a39245fc1f296e3b440e64812373e33f39251ee))

## [0.2.1](https://github.com/turborg/turborg/compare/v0.2.0...v0.2.1) (2026-05-27)


### Bug Fixes

* **irc:** forward WHOIS/WHO over the bouncer; surface CTCP notices on web ([6f8c850](https://github.com/turborg/turborg/commit/6f8c85012301217be0530d20e726af4e443c0ed8))

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
