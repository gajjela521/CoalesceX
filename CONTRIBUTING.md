# Contributing to CoalesceX

Thank you for your interest in contributing. This document covers how to build, test, and submit changes.

## Building Locally

Requirements: **Java 21+**, **Gradle 9+**

```bash
git clone https://github.com/coalescex/CoalesceX.git
cd CoalesceX

# Full build with all tests
./gradlew build

# Run tests only
./gradlew test

# Run the simulation harness
./gradlew run

# Run with Virtual Thread pinning diagnostics
./gradlew run -Pjvmargs="-Djdk.tracePinnedThreads=full"
```

## Branch Naming

| Prefix | Use for |
|--------|---------|
| `feat/` | New feature |
| `fix/` | Bug fix |
| `docs/` | Documentation only |
| `refactor/` | Refactoring (no behavior change) |
| `test/` | Adding or fixing tests |
| `perf/` | Performance improvement |

Example: `feat/async-batch-coalescer`, `fix/timeout-propagation-edge-case`

## Pull Request Process

1. Fork the repo and create your branch from `main`
2. Make your changes
3. Add or update tests — every new public API must have test coverage
4. Ensure `./gradlew build` passes locally
5. Fill out the PR description completely
6. Link any related issues

## Where to Contribute

### Adding a New Coalescer Variant

CoalesceX ships two coalescer flavors: `RequestCoalescer` (blocking) and `AsyncRequestCoalescer` (non-blocking). New variants belong in `src/main/java/io/coalescex/` and must implement the `Coalescer<K, V>` interface.

Steps:
1. Implement `Coalescer<K, V>` in `src/main/java/io/coalescex/`
2. Wire metric hooks through the existing `CoalescerMetrics` interface
3. Add a corresponding test class in `src/test/java/io/coalescex/`

### Improving Metrics

`CoalescerMetrics` is an interface — new metric dimensions (e.g., per-key latency histograms) can be added there without breaking existing implementations. Keep the `LoggingCoalesceXMetrics` reference implementation in sync.

### Fixing a Bug

1. Write a failing test that reproduces the bug first
2. Fix the bug
3. Confirm the test now passes

## Code Style

- No comments unless the **why** is non-obvious (a hidden constraint, a workaround, a subtle invariant)
- No multi-line docstrings — one short line max
- No unused code, backwards-compat shims, or `_unused` variables
- Records and sealed interfaces preferred for immutable data
- All mutable shared state must be thread-safe — CoalesceX is designed for Virtual Thread concurrency and must never pin carrier threads

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(async): add AsyncRequestCoalescer with CompletableFuture chaining
fix(timeout): correctly propagate timeout to late-arriving callers
perf(coalescer): reduce contention on pending-request ConcurrentHashMap
docs: add Virtual Thread pinning guidance to README
```

## Reporting Issues

- **Security vulnerabilities** → see [SECURITY.md](SECURITY.md), do not open a public issue
- **Bugs** → open a GitHub issue with a minimal reproducer
- **Feature requests** → open a GitHub issue describing the use case
