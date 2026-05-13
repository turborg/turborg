# Contributing to turborg

Thanks for your interest in turborg. The maintainers are happy to review high-quality patches, especially new connectors, bug fixes with tests, and documentation improvements.

## Code of Conduct

This project follows the [Contributor Covenant 2.1](CODE_OF_CONDUCT.md). By participating, you agree to abide by it.

## Contributor License Agreement (CLA)

turborg uses a CLA so the maintainers can keep the option of relicensing future versions if a strategic need arises (see [LICENSE](LICENSE) for current terms — Apache 2.0). Already-released versions stay under their published license forever; the CLA only affects what license future versions can ship under.

The first time you open a PR, the [cla-assistant](https://github.com/contributor-assistant/github-action) bot will ask you to comment

> I have read the CLA Document and I hereby sign the CLA

The full text is at [CLA.md](CLA.md). One signature per contributor — subsequent PRs don't re-prompt.

## Development setup

You need Go 1.25 or later (`go version`).

```bash
git clone https://github.com/turborg/turborg.git
cd turborg
go mod download
```

Optional but recommended: install [`golangci-lint`](https://golangci-lint.run/welcome/install/) and [`go-test-coverage`](https://github.com/vladopajic/go-test-coverage) for the same gates CI enforces.

Verify your setup:

```bash
make test         # go test -race -count=1 -timeout 120s ./...
make lint         # golangci-lint run
make cover-gate   # enforce per-package + total coverage thresholds
```

All three must pass before a PR will be merged. CI runs the same gates on Go 1.25 and 1.26.

## Branching strategy

We use **trunk-based development**:

- One long-lived branch: `main`
- Short-lived feature branches: `feat/<topic>`, `fix/<topic>`, `docs/<topic>`, `refactor/<topic>`, `chore/<topic>`
- All changes go through PRs; **squash-merge only** so `main` stays linear and readable
- Branch protection on `main` blocks force-push and direct push; CI + CLA must pass

No `develop`, no GitFlow.

## Commit messages

PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(connector/irc): add SASL authentication
fix(agent): handle reconnect race on bouncer disconnect
docs(quickstart): clarify env var precedence
refactor(llm): extract retry logic into shared helper
chore(deps): bump anthropic-sdk-go to v1.44
```

The squash commit on `main` inherits the PR title — it becomes the project's history. Make it count.

**Do not include `Co-Authored-By: Claude` (or any AI co-author) in commit messages.** Every commit is attributed to a human contributor.

## Releases

We use [release-please](https://github.com/googleapis/release-please) — Conventional Commits drive automated CHANGELOG updates, version bumps in `internal/version/version.go`, and GitHub Release creation. [GoReleaser](https://goreleaser.com/) builds linux/amd64 + linux/arm64 binaries and the multi-arch container image on every tagged release.

You don't need to update `CHANGELOG.md` or the `Version` constant manually — release-please does it.

## Pull request checklist

Before opening a PR:

- [ ] PR title follows Conventional Commits
- [ ] Tests added or updated to cover the change
- [ ] Coverage stays at or above 90% total (`make cover-gate`)
- [ ] `make lint test` is green locally
- [ ] Documentation updated if user-facing behavior changed
- [ ] CLA signed (cla-assistant bot will prompt you)

## Testing

turborg uses Go's stdlib `testing` package + [testify](https://github.com/stretchr/testify) for assertions and [goleak](https://github.com/uber-go/goleak) for goroutine-leak detection. Coverage is enforced per-package + total via [`go-test-coverage`](https://github.com/vladopajic/go-test-coverage) against `.testcoverage.yml`.

Tests live alongside their packages plus a `tests/fixtures/` tree for shared infrastructure:

- `internal/<pkg>/*_test.go` — unit + small integration tests for that package
- `tests/fixtures/fakeirc/` — in-process IRC server stub
- `tests/fixtures/fakeconn/` — in-memory `Connector` stub

Run a subset:

```bash
go test -race ./internal/connector/irc/...   # just the IRC tests
go test -race -run TestEchoPing ./...        # one test by name
go test -race -count=1 ./...                 # everything, no cache
```

Integration tests against the IRC connector use the in-memory `FakeIRCServer` — no external dependencies.

## Writing a new connector

See [docs/writing-a-connector.md](docs/writing-a-connector.md) for the worked example. The short version:

1. Implement the `agent.Connector` interface (`Name`, `Start`, `Run`, `Stop`, `Inbound`, `Send`, `ClaimSupervision`).
2. Translate inbound transport messages into `*agent.InboundEnvelope`; translate `*agent.OutboundEnvelope` to your transport's send shape.
3. Add a `Settings` struct loaded via `caarlos0/env` with the `TURBORG_<CONNECTOR>_*` prefix.
4. Register the connector name in `internal/config.ValidConnectors` and the `runtime.Build` switch.
5. Add tests using a fake server (model on `tests/fixtures/fakeirc/`).
6. Add an entry to the connector matrix in `README.md`.

## Reporting issues

- **Bug**: open a [bug report](https://github.com/turborg/turborg/issues/new?template=bug_report.yml). Include `turborg --version`, `go version`, OS, minimal repro.
- **Feature**: open a [feature request](https://github.com/turborg/turborg/issues/new?template=feature_request.yml).
- **New connector idea**: use the [connector request](https://github.com/turborg/turborg/issues/new?template=connector_request.yml) template.
- **Security vulnerability**: see [SECURITY.md](SECURITY.md) — do **not** open a public issue.

## Questions

For usage questions or design discussions, please use [GitHub Discussions](https://github.com/turborg/turborg/discussions). The issue tracker is for bugs and concrete proposals only.

---

Thank you for contributing.
