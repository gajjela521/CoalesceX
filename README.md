# CoalesceX

**Enterprise-grade request-collapsing gateway for Java 21+ Virtual Thread architectures.**

Prevents **thundering-herd distributed I/O storms** by coalescing concurrent requests for the
same resource into a single upstream execution. Every caller transparently receives the same
result once it resolves — eliminating redundant database queries, HTTP fetches, and cache-miss
stampedes without ever touching a `synchronized` block or a `ReentrantLock`.

[![CI](https://github.com/gajjela521/CoalesceX/actions/workflows/ci.yml/badge.svg)](https://github.com/gajjela521/CoalesceX/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-21%2B-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Maven Central](https://img.shields.io/maven-central/v/io.github.gajjela521/coalescex)

---

## Table of Contents

- [Why We Built This](#why-we-built-this)
- [The Problem in Depth](#the-problem-in-depth)
- [Real-World Scenario: With and Without CoalesceX](#real-world-scenario-with-and-without-coalescex)
- [Architecture Deep Dive](#architecture-deep-dive)
- [How It Improves Your System](#how-it-improves-your-system)
- [Installation](#installation)
- [Requirements](#requirements)
- [API Overview](#api-overview)
- [Usage](#usage)
- [Observability](#observability)
- [Non-blocking: AsyncRequestCoalescer](#non-blocking-asyncrequestcoalescer)
- [Exception Handling](#exception-handling)
- [Performance](#performance)
- [Java Version Notes](#java-version-notes)
- [Building](#building)
- [License](#license)

---

## Why We Built This

Java 21 introduced **Virtual Threads** as a production feature — the ability to run millions of
lightweight threads backed by a small pool of OS carrier threads. The promise: write blocking code
(`Thread.sleep`, JDBC, `HttpClient`) and get the scalability of async/reactive without the
complexity.

That promise holds, until you hit a **thundering herd**.

### The Thundering Herd in a Virtual Thread World

In a traditional platform-thread server (e.g., Tomcat with 200 threads), a thundering herd is
naturally self-limiting — you can only have 200 concurrent requests. The stampede burns CPU but
is bounded.

With Virtual Threads the ceiling is gone. A single JVM can schedule hundreds of thousands of
concurrent virtual threads. When a high-traffic endpoint suffers a cache miss, or a downstream
service goes slow, the storm that reaches your database is not 200 queries — it can be 50,000.

The instinctive solution is to gate access behind a `synchronized` block or a `ReentrantLock`:

```java
// ANTI-PATTERN: kills Virtual Thread throughput on Java 21–23
private synchronized CustomerData loadOnce(String id) {
    return db.findCustomer(id); // DB call while holding a monitor
}
```

This introduces a worse failure mode: **carrier-thread pinning**. On Java 21–23, when a Virtual
Thread holds a `synchronized` monitor and calls any blocking operation (I/O, sleep, lock), the JVM
cannot unmount it from its OS carrier thread. The carrier is frozen for the duration of the DB call.
With 200 carriers and 50,000 virtual threads all trying to enter the same monitor, you have
effectively reproduced the worst properties of both worlds: the thundering herd *and* a blocked
thread pool.

CoalesceX was built to solve this correctly — no `synchronized`, no `ReentrantLock`, no pinning.

---

## The Problem in Depth

### The Standard Thundering Herd Pattern

Consider a product detail page that reads from a database through a shared in-memory cache.
When the cache is cold (first request after deployment, TTL expiry, cache eviction), all concurrent
callers see a cache miss simultaneously and race toward the database:

```
[00:00.000] Thread  1 — cache miss for "product:iphone-16" — dispatching DB query
[00:00.001] Thread  2 — cache miss for "product:iphone-16" — dispatching DB query
[00:00.001] Thread  3 — cache miss for "product:iphone-16" — dispatching DB query
         ...
[00:00.003] Thread 10000 — cache miss for "product:iphone-16" — dispatching DB query

[00:00.150] DB receives 10,000 identical SELECT statements simultaneously
[00:00.150] DB connection pool exhausted — remaining queries queue or fail
[00:00.200] DB CPU spikes to 100%
[00:00.500] DB starts returning timeouts
[00:00.501] Application begins throwing 503s to clients
```

**Without CoalesceX — naive code:**

```java
@Service
public class ProductService {
    private final Cache<String, ProductDto> cache;
    private final ProductRepository         db;

    public ProductDto get(String sku) {
        ProductDto cached = cache.getIfPresent(sku);
        if (cached != null) return cached;

        // ❌ Every thread that reached here in the same millisecond issues an
        //    independent DB query. With 10,000 Virtual Threads, that is 10,000 queries.
        ProductDto product = db.findBySku(sku);    // identical query, 9,999 times wasted
        cache.put(sku, product);
        return product;
    }
}
```

**The synchronized "fix" that breaks Virtual Threads:**

```java
public ProductDto get(String sku) {
    ProductDto cached = cache.getIfPresent(sku);
    if (cached != null) return cached;

    synchronized (this) {
        // ❌ Double-checked locking pattern — but synchronized pins the carrier thread
        //    on Java 21–23 while the DB call executes. 200 carrier threads frozen.
        //    Throughput collapses even worse than the original stampede.
        ProductDto check = cache.getIfPresent(sku);
        if (check != null) return check;

        ProductDto product = db.findBySku(sku);
        cache.put(sku, product);
        return product;
    }
}
```

Neither approach works in a Virtual Thread architecture at scale.

---

## Real-World Scenario: With and Without CoalesceX

### Scenario: Black Friday Product Page

Your e-commerce platform serves 50,000 requests/second at peak. A viral social media post
sends 10,000 users to the same product page within 200 ms. Your Redis cache TTL just expired
for that product key, so every request is a miss.

### Without CoalesceX

```java
@Service
public class ProductService {
    private final RedisCache      cache;
    private final ProductDatabase db;

    public ProductDto getProduct(String sku) {
        // 1. Cache check — all 10,000 threads see a miss at the same instant
        Optional<ProductDto> hit = cache.get("product:" + sku);
        if (hit.isPresent()) return hit.get();

        // 2. All 10,000 threads reach here and issue their own DB query
        ProductDto product = db.findBySku(sku);   // × 10,000 identical SELECT

        // 3. All 10,000 threads try to write back to Redis
        cache.put("product:" + sku, product, Duration.ofMinutes(5));

        return product;
    }
}
```

**What happens:**

```
Timeline (wall clock)
  0 ms  — 10,000 Virtual Threads call getProduct("iphone-16")
  0 ms  — 10,000 cache misses
  0 ms  — 10,000 DB connections requested (pool has 50)
  1 ms  — 50 queries running; 9,950 waiting for a pool connection
 50 ms  — DB CPU at 100%, query times climbing from 30ms to 800ms
150 ms  — DB connection pool timeout; 8,000 threads receive SQLException
150 ms  — Application starts returning HTTP 503
200 ms  — AlertManager fires "DB connection pool exhausted"
          PagerDuty wakes up your on-call engineer at 2am
```

**Cost:**
- 9,999 wasted DB round-trips (one result, 10,000 identical queries)
- Service outage visible to end users
- DB overloaded, affecting all other queries on the same instance
- Cache stampede repeats every 5 minutes when the TTL expires

---

### With CoalesceX

```java
@Service
public class ProductService {
    private final RedisCache                       cache;
    private final ProductDatabase                  db;
    private final RequestCoalescer<String, ProductDto> coalescer;

    public ProductService(RedisCache cache, ProductDatabase db) {
        this.cache = cache;
        this.db    = db;
        this.coalescer = RequestCoalescer.<String, ProductDto>builder()
                .defaultTimeout(Duration.ofSeconds(5))
                .build();
    }

    public ProductDto getProduct(String sku) {
        // 1. Fast path: check cache first (still no coalescer overhead for cache hits)
        Optional<ProductDto> hit = cache.get("product:" + sku);
        if (hit.isPresent()) return hit.get();

        // 2. Cache miss: coalesce all concurrent callers for the same SKU
        //    Exactly 1 DB query fires, regardless of how many threads are waiting.
        return coalescer.compute("product:" + sku, () -> {
            ProductDto product = db.findBySku(sku);   // called exactly ONCE
            cache.put("product:" + sku, product, Duration.ofMinutes(5));
            return product;
        });
    }
}
```

**What happens:**

```
Timeline (wall clock)
  0 ms  — 10,000 Virtual Threads call getProduct("iphone-16")
  0 ms  — 10,000 cache misses
  0 ms  — Thread 1 enters coalescer, becomes the "dispatcher", gets a DB connection
  0 ms  — Threads 2–10,000 enter coalescer, find the in-flight promise, suspend (unmount)
  0 ms  — 1 DB query running (not 10,000)
 30 ms  — DB query returns the product
 30 ms  — CoalesceX removes the map entry, completes the shared CompletableFuture
 30 ms  — 9,999 suspended Virtual Threads are scheduled to resume with the result
 35 ms  — All 10,000 callers have their ProductDto, zero errors
```

**Cost:** 1 DB query. 9,999 threads received the result with zero upstream work.

### Side-by-Side Comparison

| Metric | Without CoalesceX | With CoalesceX |
|---|---|---|
| DB queries fired | 10,000 | **1** |
| DB connection pool pressure | Exhausted | Negligible |
| Wall-clock latency (p99) | 800ms+ (timeouts) | ~30ms |
| Error rate | ~80% (pool exhausted) | 0% |
| Cache write-back calls | 10,000 | **1** |
| On-call page? | Yes | No |

---

## Architecture Deep Dive

CoalesceX achieves its guarantees through six interlocking design decisions. Each one is necessary
and they are not interchangeable.

### 1. The In-Flight Registry

At the heart of CoalesceX is a single `ConcurrentHashMap`:

```java
private final ConcurrentHashMap<K, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();
```

This map holds exactly one entry per key that currently has an upstream request in progress.
The value is a `CompletableFuture<V>` — a "promise" that will be completed when the upstream
loader finishes.

When a key is present in this map, it means: *"an upstream call for this key is running right now;
attach to this future instead of starting a new one."*

When a key is absent, it means: *"no in-flight request; you must dispatch one."*

---

### 2. `compute()` Not `computeIfAbsent()` — The Critical Section Choice

The decision of which `ConcurrentHashMap` method to use is the most important design choice in
the entire library, and the least obvious.

**Why `computeIfAbsent()` is wrong:**

`ConcurrentHashMap.computeIfAbsent()` holds a `synchronized` monitor on the internal hash bin
while the mapping function executes. Its contract is: "if key is absent, compute the value."
The mapping function runs while the bin lock is held.

On Java 21–23, if the mapping function could yield (block, I/O, `Thread.sleep`) while holding a
`synchronized` monitor, the JVM **pins** the Virtual Thread to its OS carrier. The carrier thread
is frozen for the duration. With many hot keys all hitting the same bin, or simply under high
concurrency, this collapses the carrier pool.

Even if your mapping function *today* is non-blocking, the pattern invites future bugs. Someone
adds a log statement, a metric push, an exception that allocates lazily — any of these could pin
a carrier.

**Why `compute()` is correct:**

CoalesceX uses `compute()` with a mapping function that is guaranteed non-blocking: it only does
a null check and a pointer assignment. No I/O. No locks. No allocation beyond the
`CompletableFuture` itself. The critical section is held for nanoseconds.

```java
CompletableFuture<V> promise = inFlight.compute(key, (k, existing) -> {
    if (existing != null && !existing.isDone()) {
        return existing;          // reuse the in-flight promise
    }
    shouldDispatch[0] = true;
    return new CompletableFuture<>();  // register a new promise atomically
});
```

This one-line lambda is the entire critical section. The bin lock is released before any actual
work begins.

The `shouldDispatch[0]` flag — a single-element boolean array to be effectively final — communicates
back to the caller whether *this* thread won the dispatch race. Only the thread that created the new
`CompletableFuture` calls `dispatchUpstream()`.

---

### 3. The Promise Pattern — Separating Registration from Execution

After `compute()` returns, only one thread has `shouldDispatch[0] = true`. That thread calls
`dispatchUpstream()`, which submits the loader to the executor *outside* the map's critical section:

```java
CompletableFuture.supplyAsync(loader, executor)
    .whenComplete((value, ex) -> { /* completion handler */ });
```

The loader runs on a Virtual Thread managed by the executor. The calling thread (and all other
coalesced callers) move on to `await()` and suspend by calling `promise.get(timeout, unit)`.

This separation is the **Promise pattern**:

```
compute() call (nanoseconds, critical section)
    └── registers CompletableFuture in map
    └── returns promise reference to all callers

dispatchUpstream() (runs concurrently, outside critical section)
    └── submits loader to executor
    └── loader runs on a Virtual Thread
    └── on completion → completes the promise

await() (all callers, including dispatcher)
    └── promise.get(timeout, unit)
    └── Virtual Thread unmounts from carrier (carrier is freed)
    └── resumes when promise.complete() is called
```

No caller holds any lock while waiting. The OS carrier is free to run other Virtual Threads.

---

### 4. Remove-Before-Complete — The Ordering Invariant

When the upstream loader finishes, CoalesceX removes the map entry **before** calling
`promise.complete()`:

```java
.whenComplete((value, ex) -> {
    if (ex != null) {
        inFlight.remove(key, promise);       // step 1: evict from map
        promise.completeExceptionally(cause); // step 2: notify waiters
    } else {
        inFlight.remove(key, promise);       // step 1: evict from map
        promise.complete(value);             // step 2: notify waiters
    }
});
```

**Why this order matters:**

If you did it the other way — complete first, then remove — there is a race window where:
1. Thread A calls `promise.complete(value)`.
2. Thread B arrives, calls `compute()`, sees the future in the map, finds it is done
   (`isDone() == true`), and creates a new one — dispatching a fresh upstream call.
3. Thread C arrives, calls `compute()`, sees the *original* future still in the map
   (not yet removed), finds it done, and *reuses* it — getting the now-stale result.

By removing first, any thread arriving after the removal creates a fresh coalescing window.
Threads already blocked on `promise.get()` hold a direct reference to the promise object — they
are unaffected by the removal and receive the result normally.

```
Timeline                   Map state
────────────────────────────────────────────────────────────
t=0   Thread 1 creates promise P1        map: {key → P1}
t=0   Threads 2–N join promise P1        map: {key → P1}
t=30  Loader finishes
t=30  inFlight.remove(key, P1)           map: {}         ← new arrivals start fresh
t=30  P1.complete(value)                 → Threads 1–N wake up with value
t=31  Thread N+1 arrives → no entry → creates P2        map: {key → P2}
                                          → fresh upstream call
```

The `remove(key, promise)` form (two-argument remove) is used deliberately. It is a conditional
atomic remove: only removes the entry if the value is still `promise`. This prevents a race where
a new `CompletableFuture` is registered for the same key between the time the old one completes
and the time the remove executes.

---

### 5. Failure Eviction — No Stale Error Replay

The same remove-before-complete ordering applies to failures:

```java
inFlight.remove(key, promise);
promise.completeExceptionally(cause);
```

This ensures that when a loader throws, the failed `CompletableFuture` is not left in the map.
Any caller arriving after the removal will dispatch a fresh upstream attempt — retrying the
operation — rather than receiving the same exception from a cached failed future.

Without this, a transient network error at `t=0` would affect every caller until the failed
entry was manually invalidated.

---

### 6. Virtual Thread Suspension — Free Waiting

When a coalesced caller calls `await()`, it blocks on:

```java
return promise.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
```

On a platform thread, this blocks the OS thread — the thread and its carrier are frozen.
On a Virtual Thread, this is different: the JVM **unmounts** the Virtual Thread from its OS carrier
thread. The carrier is immediately freed to schedule and run other Virtual Threads.

The waiting Virtual Thread is parked (stored as a continuation in heap memory). When
`promise.complete(value)` is called, the JVM reschedules all parked Virtual Threads that
were waiting on that promise.

This is why CoalesceX can handle 10,000 concurrent waiters on a single key with no carrier
starvation — the 9,999 waiting threads consume no OS resources while suspended.

---

### Full Request Lifecycle (Step by Step)

```
COALESCING WINDOW for key "product:iphone-16"

Step 1 — Thread 1 arrives (t=0ms)
  → compute() acquires bin lock (nanoseconds)
  → map has no entry for key
  → shouldDispatch[0] = true
  → registers new CompletableFuture<ProductDto> (P1) in map
  → bin lock released
  → calls dispatchUpstream(key, loader, P1)
  → supplyAsync() submits loader to virtual thread executor
  → Thread 1 calls P1.get(5s) → Virtual Thread unmounts from carrier

Step 2 — Threads 2–10,000 arrive (t=0ms, within microseconds)
  → compute() acquires bin lock (nanoseconds)
  → map has entry: key → P1
  → P1.isDone() == false → return P1
  → bin lock released
  → shouldDispatch[0] = false → no upstream call dispatched
  → each thread calls P1.get(5s) → each Virtual Thread unmounts

Step 3 — Loader executes (t=0ms to t=30ms)
  → A single Virtual Thread runs the DB query
  → Receives ProductDto result
  → Updates Redis cache
  → Returns the ProductDto to supplyAsync()

Step 4 — Remove-before-complete (t=30ms)
  → whenComplete callback fires
  → inFlight.remove("product:iphone-16", P1) → map is now empty
  → P1.complete(productDto)

Step 5 — 10,000 threads resume (t=30ms to t=35ms)
  → JVM reschedules all 10,000 Virtual Threads parked on P1
  → Each thread's P1.get() returns the ProductDto
  → Each thread continues executing normally

RESULT: 1 DB query, 10,000 responses, ~30ms latency, 0 errors
```

---

### Component Map

```
┌───────────────────────────────────────────────────────────────────┐
│                         Your Application                          │
│                                                                   │
│  ProductService.getProduct(sku)                                   │
│       │                                                           │
│       ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                    RequestCoalescer<K,V>                     │  │
│  │                                                             │  │
│  │  compute(key, loader)                                       │  │
│  │      │                                                      │  │
│  │      ▼                                                      │  │
│  │  ConcurrentHashMap.compute()  ←── nanosecond critical sec   │  │
│  │      │                                                      │  │
│  │      ├── [first caller] → dispatchUpstream()                │  │
│  │      │       │                                              │  │
│  │      │       └── supplyAsync(loader, virtualExecutor)       │  │
│  │      │               │                                      │  │
│  │      │               └── Virtual Thread (loader runs here)  │  │
│  │      │                       │                              │  │
│  │      │               whenComplete:                          │  │
│  │      │                   remove(key, promise)  ← evict first│  │
│  │      │                   promise.complete(value)            │  │
│  │      │                                                      │  │
│  │      └── [all callers] → await()                            │  │
│  │              │                                              │  │
│  │              └── promise.get(timeout)                       │  │
│  │                      │                                      │  │
│  │                      └── Virtual Thread suspends (unmounts) │  │
│  │                          Carrier freed for other work       │  │
│  │                          Resumes when promise completes     │  │
│  │                                                             │  │
│  │  CoalesceXMetrics SPI ──── onRequest / onCoalesced /        │  │
│  │                            onUpstreamDispatched /           │  │
│  │                            onUpstreamSuccess /              │  │
│  │                            onUpstreamFailure                │  │
│  └─────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

---

### LongAdder vs AtomicLong

Internal counters use `LongAdder` instead of `AtomicLong`:

```java
private final LongAdder totalRequests    = new LongAdder();
private final LongAdder upstreamRequests = new LongAdder();
private final LongAdder failedRequests   = new LongAdder();
```

`AtomicLong.incrementAndGet()` uses a CAS loop. Under high contention (10,000 threads all
incrementing simultaneously), threads spin repeatedly before succeeding. The cost is proportional
to the number of competing threads.

`LongAdder` maintains per-CPU cells. Each CPU increments its own cell independently — no
contention between CPUs. The true total is computed lazily on `sum()`. Reads are slightly
more expensive (summing all cells), but writes are nearly contention-free. For a metrics counter
that is written on every request and read rarely (monitoring polls), this is the correct trade-off.

---

## How It Improves Your System

| Before | After |
|---|---|
| N concurrent requests for key X → N upstream calls | N concurrent requests for key X → **1 upstream call** |
| Database connection pool exhausted under cache miss | DB sees at most 1 query per key per coalescing window |
| `synchronized` gate pins OS carrier threads (Java 21–23) | No `synchronized` anywhere — carriers always free |
| Cache stampede every TTL cycle | Stampede collapsed to a single refresh per key |
| Manual double-checked locking with race conditions | Lock-free atomic coordination — race-condition free |
| Timeouts silently hang threads | Per-call timeout with `CoalescerTimeoutException` |
| No visibility into coalescing behavior | Full metrics: efficiency %, upstream count, failure count |
| Hard to test (timing-dependent) | `invalidate()` for forced fresh dispatch; metrics for verification |

---

## Installation

CoalesceX is published to Maven Central under `io.github.gajjela521:coalescex`.

**Gradle (Kotlin DSL)**
```kotlin
implementation("io.github.gajjela521:coalescex:1.2.0")
```

**Gradle (Groovy DSL)**
```groovy
implementation 'io.github.gajjela521:coalescex:1.2.0'
```

**Maven**
```xml
<dependency>
    <groupId>io.github.gajjela521</groupId>
    <artifactId>coalescex</artifactId>
    <version>1.2.0</version>
</dependency>
```

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
            public void onRequest(Object key)   { otelRequests.add(1); }
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

| Scenario | Concurrent callers | Upstream calls | Efficiency | Wall time |
|---|---|---|---|---|
| Hot key thundering herd | 10,000 | **1** | 99.99% | ~217 ms |
| 50 distinct keys × 200 threads | 10,000 | **50** | 99.50% | ~120 ms |

Run the harness yourself:

```bash
./gradlew run
```

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
