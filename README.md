# CoalesceX

**Enterprise-grade request-collapsing gateway for Java 21+ Virtual Thread architectures.**

Prevents **thundering-herd distributed I/O storms** by coalescing concurrent requests for the
same resource into a single upstream execution. Every caller transparently receives the same
result once it resolves — eliminating redundant database queries, HTTP fetches, and cache-miss
stampedes without ever touching a `synchronized` block or a `ReentrantLock`.

---

## The Problem

In high-concurrency systems, a cache miss or a traffic spike on a hot key causes hundreds or
thousands of threads to race toward the same downstream resource simultaneously:

```
Thread 1 ──► DB query ──► result
Thread 2 ──► DB query ──► result   (identical query)
Thread 3 ──► DB query ──► result   (identical query)
  ⋮              ⋮
Thread N ──► DB query ──► result   (identical query)
```

With Java Virtual Threads the concurrency ceiling is extremely high, making this even worse.
Using `synchronized` or `ReentrantLock` to gate the queries causes **carrier-thread pinning**,
which freezes OS threads and collapses throughput.

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

| Decision | Why |
|---|---|
| Promise pattern (`new CompletableFuture<>()` in map, completed separately) | Prevents any work from occurring inside `computeIfAbsent` — avoids holding the map segment lock longer than a pointer store |
| `map.remove(key, promise)` for cleanup | Atomic compare-and-remove — a late cleanup for a completed future cannot accidentally evict a newer in-flight entry for the same key |
| Failed futures are evicted immediately | Ensures the next wave of callers retries rather than replaying a stale error |
| `future.get(timeout, unit)` instead of `future.join()` | Enables per-call timeouts; Virtual Threads unmount from their carrier during the `get()` call — no OS thread is held |
| No `synchronized` / `ReentrantLock` anywhere | Eliminates the carrier-pinning that makes lock-based solutions dangerous on Virtual Threads |

---

## Requirements

- Java 21 or later (Virtual Thread support)
- Gradle 8+ or Maven 3.8+ (to build from source)

---

## Usage

### Zero-config factory

```java
RequestCoalescer<String, CustomerData> coalescer = RequestCoalescer.create();

// 10,000 concurrent callers for the same key → exactly 1 upstream DB call
CustomerData data = coalescer.compute("customer:42", () -> db.loadCustomer(42));
```

### Builder — custom timeout and executor

```java
RequestCoalescer<String, byte[]> coalescer = RequestCoalescer.<String, byte[]>builder()
    .defaultTimeout(Duration.ofSeconds(5))
    .executor(myManagedExecutor)        // optional; defaults to virtual-thread-per-task
    .build();
```

### Per-call timeout override

```java
String result = coalescer.compute(
    "report:monthly",
    () -> reportService.generateMonthlyReport(),
    Duration.ofSeconds(60)             // this specific call gets 60 s
);
```

### Forced invalidation

```java
// Evicts any in-flight or pending entry — next compute() retries unconditionally
coalescer.invalidate("cache:stale-key");
```

### Metrics

```java
CoalescerMetrics m = coalescer.metrics();
System.out.printf("Efficiency: %.2f%% (%d upstream / %d total)%n",
    m.coalescingEfficiency() * 100,
    m.upstreamRequests(),
    m.totalRequests());
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
    private final RequestCoalescer<String, ProductDto> coalescer;

    public ProductDto get(String sku) {
        return coalescer.compute(sku, () -> productRepository.findBySku(sku));
    }
}
```

---

## Performance

Observed results from the included `SimulationHarness` (Java 21, Apple M-series):

| Scenario | Threads | Upstream calls | Efficiency |
|---|---|---|---|
| Hot key thundering herd | 10,000 | 1 | 99.99 % |
| 50 distinct keys × 200 threads | 10,000 | 50 | 99.50 % |

---

## Exception Handling

| Loader throws | What caller receives |
|---|---|
| `RuntimeException` | Same exception, rethrown directly (no wrapping) |
| `Error` | Same error, rethrown directly |
| Checked exception | `CoalescerException` wrapping the checked cause |
| Timeout exceeded | `CoalescerTimeoutException` (subclass of `CoalescerException`) |
| Thread interrupted | Interrupt flag restored; `CoalescerException` thrown |

Failed upstream futures are **evicted immediately** from the registry so the next caller
dispatches a fresh retry rather than replaying the failure.

---

## Building

```bash
./gradlew build          # compile + test
./gradlew test           # run tests only
./gradlew run            # run SimulationHarness
```

---

## License

MIT — see [LICENSE](LICENSE).

---

## Contributing

Pull requests are welcome. For significant changes, please open an issue first to discuss the
approach. All submissions must include tests and must pass `./gradlew test`.