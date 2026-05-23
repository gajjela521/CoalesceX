# Changelog

All notable changes to CoalesceX are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [1.2.0] — 2025-05-23

### Changed
- Renamed Java package from `com.github.gajjela521.coalescex` to `io.coalescex` for a clean, product-centric namespace
- Source tree relocated to `src/main/java/io/coalescex/` and `src/test/java/io/coalescex/`
- License changed from MIT to Apache 2.0

## [1.1.0] — 2025-04-18

### Added
- `AsyncRequestCoalescer` — non-blocking coalescer returning `CompletableFuture<V>`; callers share one in-flight future per key without blocking a carrier thread
- `CoalesceXMetrics` interface — pluggable metric hooks for hit rate, miss rate, and timeout counters
- `LoggingCoalesceXMetrics` — reference implementation that emits SLF4J log lines at DEBUG level

### Changed
- `RequestCoalescer` promoted to stable API; constructor now accepts an optional `CoalescerMetrics` instance
- `CoalescerTimeoutException` now carries the coalescer key and the configured timeout duration for richer diagnostics

## [1.0.0] — 2024-12-01

### Added
- `Coalescer<K, V>` interface — core contract for request-collapsing implementations
- `RequestCoalescer<K, V>` — Virtual Thread–safe synchronous coalescer; concurrent callers for the same key block on a single `CompletableFuture`, eliminating duplicate downstream I/O
- `CoalescerException` — base unchecked exception for coalescer failures
- `CoalescerTimeoutException` — thrown when the coalesced computation exceeds the configured deadline
- `SimulationHarness` — runnable load simulation demonstrating thundering-herd collapse under sustained concurrency
- SLF4J 2.x logging integration
- Full JUnit 5 test suite

[Unreleased]: https://github.com/gajjela521/CoalesceX/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/gajjela521/CoalesceX/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/gajjela521/CoalesceX/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/gajjela521/CoalesceX/releases/tag/v1.0.0
