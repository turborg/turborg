# Security Policy

## Reporting a vulnerability

If you've found a security issue in turborg, please **do not** open a public GitHub issue.

The fastest path is GitHub's private security advisory:

→ **[Report a vulnerability privately](https://github.com/turborg/turborg/security/advisories/new)**

If you cannot use GitHub for any reason, email **security@xshellz.com** with the same information.

### What to include

- A clear description of the vulnerability and what an attacker could do with it
- Steps to reproduce — proof-of-concept code is highly appreciated
- Affected versions of turborg
- Any suggestions for a fix (optional)

### What happens next

| Step             | Timeline                       |
|------------------|--------------------------------|
| Acknowledgment   | Within 48 hours                |
| Initial triage   | Within 7 days                  |
| Fix released     | Within 30 days for high-severity issues; longer for low-severity |
| Public disclosure| Coordinated with the reporter once a fix is released and users have had time to upgrade |

We will credit you in the published advisory unless you prefer to remain anonymous.

## Supported versions

We provide security updates for the latest minor release.

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

Once 0.2 ships, 0.1.x will receive critical security fixes for 90 days, then move to unsupported.

## Scope

In scope:

- The published `turborg` Python package on PyPI
- The official source repository at `github.com/turborg/turborg`
- The example bots in `examples/`

Out of scope:

- Third-party connectors not maintained by the turborg core team
- The future `hive.xshellz.com` service (separate disclosure process; will be documented when the service launches)
- Issues in upstream dependencies — please report those to the upstream project, but feel free to CC us if turborg's usage exposes the issue

## Security model and assumptions

turborg is a framework for chat agents. It assumes:

- The operator runs the bot and trusts their own LLM credentials
- IRC and other transports are untrusted — the connector parsers are designed to be robust against malformed input, but operators should not run turborg with elevated privileges
- The optional bouncer accepts authenticated connections only — operators are responsible for choosing a strong password and binding to a non-public interface in untrusted environments
- LLM responses are user-influenced text — handlers that pass LLM output back into IRC channels should be aware of injection risks (e.g., `\r\n` smuggling, oversized lines)

If you've found an issue that the model above does not cover, we want to hear about it.

---

*Part of the [**xshellz**](https://www.xshellz.com) ecosystem.*
