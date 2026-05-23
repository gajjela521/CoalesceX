# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.2.x   | Yes — current stable release |
| 1.1.x   | Security fixes only |
| < 1.1   | No |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security bugs by emailing:

**gajjelsa@protonmail.com**

Include in your report:
- A description of the vulnerability
- Steps to reproduce or a proof-of-concept
- The affected version(s)
- Potential impact assessment

### Response SLA

| Action | Target |
|--------|--------|
| Acknowledgement | Within 48 hours |
| Initial assessment | Within 5 business days |
| Fix or mitigation | Within 30 days for critical issues |
| Public disclosure | Coordinated with reporter after fix is available |

We follow responsible disclosure. If you discover a vulnerability and report it privately, we will credit you in the release notes (unless you prefer anonymity).

## Scope

Security bugs in this project include:

- Deadlock or starvation in the coalescing barrier under concurrent Virtual Thread load
- Race conditions in `AsyncRequestCoalescer` that could cause duplicate execution of a coalesced request
- Memory leaks in the pending-request map under high-throughput conditions
- Incorrect timeout propagation that silently drops a caller without notification
- Thread-safety violations in `CoalesceXMetrics` that produce corrupt metric state

Out of scope:
- Vulnerabilities in third-party dependencies (report those upstream; we will upgrade promptly)
- Issues that require physical access to the deployment host
- Performance degradation that does not constitute a correctness or safety failure

## Security Design Notes

CoalesceX is a concurrency primitive, not a security boundary. Relevant design decisions for operators:

- `RequestCoalescer` and `AsyncRequestCoalescer` share a single `CompletableFuture` across all concurrent callers for the same key — ensure the computation itself does not leak caller-specific sensitive data across requests
- Timeouts are enforced per coalesced group, not per individual caller; callers that arrive late may inherit a shorter effective timeout
- `CoalesceXMetrics` records key names in metric labels — avoid using secrets or PII as coalescer keys
