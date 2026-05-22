# CoalesceX

**Enterprise-grade request-collapsing gateway for Java 21+ Virtual Thread architectures.**

Prevents **thundering-herd distributed I/O storms** by coalescing concurrent requests for the
same resource into a single upstream execution. Every caller transparently receives the same
result once it resolves — eliminating redundant database queries, HTTP fetches, and cache-miss
stampedes without ever touching a `synchronized` block or a `ReentrantLock`.

[![CI](https://github.com/gajjela521/CoalesceX/actions/workflows/ci.yml/badge.svg)](https://github.com/gajjela521/CoalesceX/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-21%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## The Problem

In high-concurrency systems, a cache miss or a traffic spike on a hot key causes hundreds or
thousands of threads to race toward the same downstream resource simultaneously:

```
Thread 1 ──► DB query ──► result
Thread 2 ──► DB query ──► result   (identical query, wasted)
Thread 3 ──► DB query ──► result   (identical query, wasted)
  ⋮
Thread N ──► DB query ──► result   (identical query, wasted)
```

With Java Virtual Threads the concurrency ceiling is extremely high, making the storm worse.
The instinct to gate queries behind `synchronized` or `ReentrantLock` causes a different
failure mode: **carrier-thread pinning**, which freezes OS threads and collapses throughput.

---

## The Solution

CoalesceX registers a single in-flight `CompletableFuture` per key. Every concurrent caller for
the same key joins the existing future rather than issuing a new upstream request:

```
Thread 1 ──► upstream fetch ──┐
Thread 2 ──► join future ──────┤──► shared result
Thread 3 ──► join future ──────┤
  ⋮                            │
Thread N ──► join future ──────┘
```

All coordination happens through lock-free `ConcurrentHashMap` atomics — no monitors, no pinning.

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **`compute()` not `computeIfAbsent()`** | `computeIfAbsent()` holds a `synchronized` bin monitor while the mapping function executes. On Java 21–23 this pins the Virtual Thread's OS carrier if the lambda could yield. `compute()` is used with a lean, non-blocking lambda (a conditional pointer swap) so the critical section is held for nanoseconds. On Java 24+ (JEP 491) `synchronized` no longer pins, but the leaner pattern is correct regardless. |
| **Promise pattern** | `compute()` stores a bare `CompletableFuture` (a "promise") that is completed asynchronously. The actual loader runs via `supplyAsync()` entirely outside the map's critical section — no work, no I/O, no blocking inside the map operation. |
| **Remove-before-complete** | The map entry is removed *before* `promise.complete()` is called. Callers arriving after the removal start a fresh coalescing window rather than attaching to a just-finished future and receiving a potentially stale result. Callers already blocked on `promise.get()` hold a direct reference and receive the result normally. |
| **Failure eviction** | Failed futures are evicted (remove-before-completeExceptionally) so the next wave of callers dispatches a fresh retry rather than replaying a stale error. |
| **`LongAdder` counters** | `LongAdder` maintains per-CPU cells and sums lazily on read, reducing CAS contention under the high write parallelism typical of coalesced workloads — lower overhead than `AtomicLong` when writes vastly outnumber reads. |
| **`future.get(timeout, unit)` not `future.join()`** | Enables per-call timeouts. Virtual Threads unmount from their OS carrier during `get()` — the carrier is freed to run other virtual threads while this one waits. |
| **`CoalesceXMetrics` SPI** | Zero-cost interface (NOOP default = empty methods, JIT-eliminated). Plug in Micrometer, OpenTelemetry, or the built-in `LoggingCoalesceXMetrics` without changing application code. |

---

## Requirements

- Java 21 or later (Virtual Thread support)
- Gradle 8+ or Maven 3.8+ (to build from source)

---

## API Overview

```
Coalescer<K,V>               ← interface (inject this in your services)
  └── RequestCoalescer        ← blocking implementation (Virtual Thread friendly)

AsyncRequestCoalescer         ← non-blocking implementation (returns CompletableFuture)

CoalesceXMetrics              ← metrics SPI (plug in Micrometer / OTEL / logging)
  └── LoggingCoalesceXMetrics ← built-in LongAdder-based implementation

CoalescerMetrics              ← immutable stats snapshot
CoalescerException            ← unchecked wrapper for checked loader failures
CoalescerTimeoutException     ← thrown when a timeout is exceeded
```

---

## Usage

### Zero-config

```java
RequestCoalescer<String, CustomerData> coalescer = RequestCoalescer.create();

// 10,000 concurrent callers for the same key → exactly 1 upstream DB call
CustomerData data = coalescer.compute("customer:42", () -> db.loadCustomer(42));
```

### Builder — timeout, executor, metrics

```java
RequestCoalescer<String, ProductDto> coalescer =
    RequestCoalescer.<String, ProductDto>builder()
        .defaultTimeout(Duration.ofSeconds(5))
        .executor(myManagedExecutor)        // optional; defaults to virtual-thread-per-task
        .metrics(micrometerAdapter)         // optional; defaults to no-op
        .build();
```

### Per-call timeout override

```java
String result = coalescer.compute(
    "report:monthly",
    () -> reportService.generateMonthlyReport(),
    Duration.ofSeconds(60)    // overrides the default timeout for this call only
);
```

### Forced invalidation

```java
// Evicts any in-flight or pending entry — next compute() dispatches a fresh request
coalescer.invalidate("cache:stale-key");
```

### Built-in metrics snapshot

```java
CoalescerMetrics m = coalescer.metrics();
System.out.printf("Efficiency: %.2f%% (%d upstream / %d total)%n",
    m.coalescingEfficiency() * 100,
    m.upstreamRequests(),
    m.totalRequests());
// → Efficiency: 99.99% (1 upstream / 10000 total)
```

### Spring / DI container integration

```java
@Bean
public RequestCoalescer<String, ProductDto> productCoalescer() {
    return RequestCoalescer.<String, ProductDto>builder()
        .defaultTimeout(Duration.ofSeconds(3))
        .build();
}

@Service
public class ProductService {
    private final Coalescer<String, ProductDto> coalescer; // inject the interface

    public ProductDto get(String sku) {
        return coalescer.compute(sku, () -> productRepository.findBySku(sku));
    }
}
```

---

## Observability

### `LoggingCoalesceXMetrics` — zero-dependency built-in

Uses `LongAdder` counters for high-throughput accuracy. Call `logSummary()` from a scheduler
for instant observability without pulling in any external metrics library.

```java
LoggingCoalesceXMetrics metrics = new LoggingCoalesceXMetrics();

RequestCoalescer<String, ProductDto> coalescer =
    RequestCoalescer.<String, ProductDto>builder()
        .metrics(metrics)
        .build();

// In a @Scheduled method or ScheduledExecutorService:
metrics.logSummary();
// INFO CoalesceX metrics — total=84231 upstream=12 coalesced=84219 failed=0 efficiency=99.99%

// Or get a typed snapshot:
CoalescerMetrics snapshot = metrics.snapshot();
```

### `CoalesceXMetrics` SPI — Micrometer

```java
MeterRegistry registry = ...;
Counter requests  = registry.counter("coalescex.requests");
Counter coalesced = registry.counter("coalescex.coalesced");
Timer   upstream  = registry.timer("coalescex.upstream.latency");
Counter failures  = registry.counter("coalescex.failures");

RequestCoalescer<String, ProductDto> coalescer =
    RequestCoalescer.<String, ProductDto>builder()
        .metrics(new CoalesceXMetrics() {
            public void onRequest(Object key)                              { requests.increment(); }
            public void onCoalesced(Object key)                            { coalesced.increment(); }
            public void onUpstreamSuccess(Object key, long latencyNanos)   {
                upstream.record(latencyNanos, TimeUnit.NANOSECONDS);
            }
            public void onUpstreamFailure(Object key, Throwable ex, long latencyNanos) {
                failures.increment();
            }
        })
        .build();
```

### `CoalesceXMetrics` SPI — OpenTelemetry

```java
LongCounter otelRequests  = meter.counterBuilder("coalescex.requests").build();
LongCounter otelCoalesced = meter.counterBuilder("coalescex.coalesced").build();

RequestCoalescer<String, ProductDto> coalescer =
    RequestCoalescer.<String, ProductDto>builder()
        .metrics(new CoalesceXMetrics() {
            public void onRequest(Object key)  { otelRequests.add(1); }
            public void onCoalesced(Object key) { otelCoalesced.add(1); }
        })
        .build();
```

---

## Non-blocking: `AsyncRequestCoalescer`

For reactive or event-loop environments (Spring WebFlux, Vert.x, Netty) where blocking a
caller thread is prohibited. Returns `CompletableFuture<V>` immediately — the caller
registers a continuation and the thread is never held.

Same `compute()`/remove-before-complete design and `CoalesceXMetrics` SPI as
`RequestCoalescer`.

```java
AsyncRequestCoalescer<String, ProductDto> coalescer = AsyncRequestCoalescer.create();

// Returns immediately — no thread is blocked
CompletableFuture<ProductDto> future = coalescer.compute(
    "sku:42",
    () -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(r -> parseProduct(r.body()))
);

future.thenAccept(product -> sendResponse(ctx, product));
```

**Spring WebFlux / Project Reactor bridge:**

```java
Mono<ProductDto> mono = Mono.fromFuture(
    coalescer.compute("sku:42", () -> reactorToFuture(productService.findBySku("42")))
);
```

**Builder:**

```java
AsyncRequestCoalescer<String, ProductDto> coalescer =
    AsyncRequestCoalescer.<String, ProductDto>builder()
        .executor(myEventLoopExecutor)
        .metrics(micrometerAdapter)
        .build();
```

---

## Exception Handling

| Loader throws | What caller receives |
|---|---|
| `RuntimeException` | Same exception, rethrown directly — no wrapping |
| `Error` | Same error, rethrown directly |
| Checked exception | `CoalescerException` wrapping the checked cause |
| Timeout exceeded | `CoalescerTimeoutException` (subclass of `CoalescerException`) |
| Thread interrupted | Interrupt flag restored; `CoalescerException` thrown |

Failed upstream futures are **evicted before** `completeExceptionally()` is called, ensuring
the next caller dispatches a fresh retry rather than replaying the error.

---

## Performance

Observed results from the included `SimulationHarness` (Java 21, Apple M-series):

| Scenario | Threads | Upstream calls | Efficiency | Wall time |
|---|---|---|---|---|
| Hot key thundering herd | 10,000 | 1 | 99.99 % | ~217 ms |
| 50 distinct keys × 200 threads | 10,000 | 50 | 99.50 % | — |

---

## Java Version Notes

| JDK | Behavior |
|---|---|
| **Java 21 – 23** | `synchronized` pins the Virtual Thread's OS carrier. CoalesceX avoids this by keeping the `compute()` lambda non-blocking (no I/O, no locks, no yields). |
| **Java 24+** (JEP 491) | `synchronized` no longer pins carriers. CoalesceX benefits automatically while remaining backwards-compatible with Java 21. |

The CI matrix validates both tiers. A dedicated **pinning smoke test** job runs the simulation
with `-Djdk.tracePinnedThreads=full` and fails the build if `reason:MONITOR` appears in the
JVM trace output — a living regression guard against accidental pinning regressions.

---

## Building

```bash
./gradlew build          # compile + test
./gradlew test           # run unit tests only
./gradlew run            # run SimulationHarness (three scenarios)

# Pinning trace (requires Java 21)
./gradlew run -Pjvmargs="-Djdk.tracePinnedThreads=full"
```

---

## License

MIT — see [LICENSE](LICENSE).

---

## Contributing

Pull requests are welcome. For significant changes, please open an issue first to discuss the
approach. All submissions must include tests and must pass `./gradlew test`.