package io.coalescex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Thread-safe, Virtual-Thread-optimized {@link Coalescer} implementation.
 *
 * <h2>How It Works</h2>
 * <p>When {@code N} concurrent callers request the same key, only the first one dispatches
 * the upstream loader. All others receive a reference to the same in-flight
 * {@link CompletableFuture} and transparently wait for it to resolve — eliminating
 * duplicate network round-trips or database queries.
 *
 * <h2>Design Invariants</h2>
 * <ul>
 *   <li><b>No {@code synchronized} or {@link java.util.concurrent.locks.Lock}</b> — entirely
 *       avoids Virtual Thread carrier-pinning. All coordination is done through lock-free
 *       {@link ConcurrentHashMap} atomics.</li>
 *
 *   <li><b>{@code compute()} not {@code computeIfAbsent()}</b> — {@code ConcurrentHashMap
 *       .computeIfAbsent()} holds a {@code synchronized} monitor on the bin while the mapping
 *       function executes. On Java 21–23 this pins the Virtual Thread to its OS carrier if
 *       anything inside the lambda could yield. This class uses {@code compute()} with a lean,
 *       non-blocking lambda (just a conditional pointer swap) so the critical section is
 *       held for nanoseconds. On Java 24+ (JEP 491) {@code synchronized} no longer pins
 *       carriers, but the leaner critical section remains the right pattern regardless.</li>
 *
 *   <li><b>Promise pattern</b> — {@code compute()} stores a bare {@code CompletableFuture}
 *       (a "promise"). The actual loader is dispatched via {@code supplyAsync()} entirely
 *       outside the map's critical section.</li>
 *
 *   <li><b>Remove-before-complete</b> — the map entry is removed <em>before</em> the promise
 *       is completed. Callers arriving after the removal point start a fresh coalescing window
 *       rather than attaching to a just-finished future and receiving a potentially stale result.
 *       Callers already blocked on {@code promise.get()} are unaffected — they hold a direct
 *       reference and receive the result as normal.</li>
 *
 *   <li><b>Failure eviction</b> — a future that completes exceptionally is removed before
 *       {@code completeExceptionally()} is called, ensuring the next caller dispatches a
 *       fresh retry rather than replaying the error.</li>
 *
 *   <li><b>{@link LongAdder} counters</b> — lower write contention than {@link
 *       java.util.concurrent.atomic.AtomicLong} under high parallelism, at the cost of
 *       a slightly more expensive read via {@link LongAdder#sum()}. The right trade-off
 *       for a high-throughput coalescer where writes vastly outnumber reads.</li>
 * </ul>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * // Zero-config — 30 s timeout, virtual-thread-per-task executor
 * RequestCoalescer<String, CustomerData> coalescer = RequestCoalescer.create();
 *
 * // With custom timeout and Micrometer metrics
 * RequestCoalescer<String, CustomerData> coalescer =
 *     RequestCoalescer.<String, CustomerData>builder()
 *         .defaultTimeout(Duration.ofSeconds(5))
 *         .metrics(micrometerAdapter)
 *         .build();
 *
 * // Usage
 * CustomerData data = coalescer.compute("customer:42", () -> db.loadCustomer(42));
 * }</pre>
 *
 * <h2>Java Version Notes</h2>
 * <ul>
 *   <li><b>Java 21–23</b>: {@code synchronized} inside {@code ConcurrentHashMap} can pin
 *       carriers. This class avoids that by keeping the {@code compute()} lambda non-blocking.</li>
 *   <li><b>Java 24+</b> (JEP 491): synchronized no longer pins carriers. This class benefits
 *       automatically while remaining backwards-compatible with Java 21.</li>
 * </ul>
 *
 * @param <K> type of the lookup key — must implement {@link Object#equals} and
 *            {@link Object#hashCode} correctly
 * @param <V> type of the computed value
 */
public final class RequestCoalescer<K, V> implements Coalescer<K, V>, AutoCloseable {

    private static final Logger   log             = LoggerFactory.getLogger(RequestCoalescer.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Default executor: one Virtual Thread per upstream task, no pool management required.
     * The JVM scheduler multiplexes Virtual Threads onto OS carrier threads.
     */
    private static final Executor VIRTUAL_EXECUTOR = task -> Thread.ofVirtual().start(task);

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Live registry of in-flight upstream requests. */
    private final ConcurrentHashMap<K, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();

    private final Executor         executor;
    private final Duration         defaultTimeout;
    private final CoalesceXMetrics metrics;

    // Internal counters using LongAdder for lower write contention under high parallelism.
    private final LongAdder totalRequests    = new LongAdder();
    private final LongAdder upstreamRequests = new LongAdder();
    private final LongAdder failedRequests   = new LongAdder();

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    private RequestCoalescer(Builder<K, V> builder) {
        this.executor       = builder.executor;
        this.defaultTimeout = builder.defaultTimeout;
        this.metrics        = builder.metrics;
    }

    /**
     * Creates a coalescer with default settings:
     * 30-second timeout, virtual-thread-per-task executor, no-op metrics.
     */
    public static <K, V> RequestCoalescer<K, V> create() {
        return RequestCoalescer.<K, V>builder().build();
    }

    /** Returns a fluent builder for a fully configured {@link RequestCoalescer}. */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    // -----------------------------------------------------------------------
    // Coalescer interface
    // -----------------------------------------------------------------------

    /** {@inheritDoc} */
    @Override
    public V compute(K key, Supplier<V> loader) {
        return compute(key, loader, defaultTimeout);
    }

    /** {@inheritDoc} */
    @Override
    public V compute(K key, Supplier<V> loader, Duration timeout) {
        Objects.requireNonNull(key,     "key must not be null");
        Objects.requireNonNull(loader,  "loader must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");

        totalRequests.increment();
        metrics.onRequest(key);

        final boolean[] shouldDispatch = {false};

        // Use compute() — not computeIfAbsent() — so the critical section is a lean
        // conditional pointer check with no blocking. computeIfAbsent() holds the same
        // synchronized bin lock but was the conventional choice; compute() makes the
        // non-blocking intent explicit and also handles the defensive case where an
        // existing entry is already completed but not yet evicted.
        CompletableFuture<V> promise = inFlight.compute(key, (k, existing) -> {
            if (existing != null && !existing.isDone()) {
                return existing; // reuse the in-flight promise
            }
            shouldDispatch[0] = true;
            return new CompletableFuture<>();
        });

        if (shouldDispatch[0]) {
            upstreamRequests.increment();
            metrics.onUpstreamDispatched(key);
            log.debug("Upstream dispatch — key [{}]", key);
            dispatchUpstream(key, loader, promise);
        } else {
            metrics.onCoalesced(key);
            log.debug("Coalesced — joining in-flight request for key [{}]", key);
        }

        return await(key, promise, timeout);
    }

    /** {@inheritDoc} */
    @Override
    public void invalidate(K key) {
        Objects.requireNonNull(key, "key must not be null");
        CompletableFuture<V> evicted = inFlight.remove(key);
        if (evicted != null && !evicted.isDone()) {
            evicted.cancel(true);
            log.debug("Invalidated in-flight request for key [{}]", key);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int activeFlightCount() {
        return inFlight.size();
    }

    /** {@inheritDoc} */
    @Override
    public CoalescerMetrics metrics() {
        long total    = totalRequests.sum();
        long upstream = upstreamRequests.sum();
        long failed   = failedRequests.sum();
        return new CoalescerMetrics(total, upstream, total - upstream, failed, inFlight.size());
    }

    /**
     * Cancels all in-flight requests and clears the internal registry.
     * Safe to call at application shutdown — wires cleanly into Spring's {@code @PreDestroy}
     * or {@code DisposableBean}. After closing, this instance must not be reused.
     */
    @Override
    public void close() {
        inFlight.forEach((k, f) -> f.cancel(true));
        inFlight.clear();
        log.debug("RequestCoalescer closed — all in-flight requests cancelled");
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Submits the upstream loader to the executor and wires up completion logic.
     *
     * <p><b>Remove-before-complete ordering</b>: the map entry is removed <em>before</em>
     * the promise is completed. This ensures callers arriving after the removal window
     * start a fresh coalescing wave rather than attaching to a just-finished future.
     * Callers already blocked on {@code promise.get()} hold a direct reference and are
     * unaffected by the map removal — they receive the result normally.
     *
     * <p>On failure, the same ordering applies: the entry is removed first so the next
     * wave of callers retries rather than replaying a stale error.
     */
    private void dispatchUpstream(K key, Supplier<V> loader, CompletableFuture<V> promise) {
        final long startNanos = System.nanoTime();
        CompletableFuture.supplyAsync(loader, executor)
                .whenComplete((value, ex) -> {
                    long latencyNanos = System.nanoTime() - startNanos;
                    if (ex != null) {
                        Throwable cause = unwrap(ex);
                        // Remove BEFORE completing so new arrivals start a fresh window.
                        inFlight.remove(key, promise);
                        promise.completeExceptionally(cause);
                        failedRequests.increment();
                        metrics.onUpstreamFailure(key, cause, latencyNanos);
                        log.warn("Upstream request failed for key [{}]: {}", key, cause.toString());
                    } else {
                        // Remove BEFORE completing — same rationale as the failure case.
                        inFlight.remove(key, promise);
                        promise.complete(value);
                        metrics.onUpstreamSuccess(key, latencyNanos);
                        log.debug("Upstream request resolved for key [{}]", key);
                    }
                });
    }

    /**
     * Blocks the calling Virtual Thread until the promise resolves or the timeout elapses.
     *
     * <p>Virtual Threads unmount from their OS carrier during {@code future.get()} —
     * the carrier is freed to run other virtual threads while this one waits. This is
     * fundamentally different from platform-thread blocking.
     *
     * <p>Exception contract:
     * <ul>
     *   <li>{@link RuntimeException} from loader → rethrown directly, no wrapping</li>
     *   <li>{@link Error} from loader → rethrown directly</li>
     *   <li>Checked exception from loader → wrapped in {@link CoalescerException}</li>
     *   <li>Timeout → {@link CoalescerTimeoutException}</li>
     *   <li>Interrupted → interrupt flag restored, {@link CoalescerException} thrown</li>
     * </ul>
     */
    private V await(K key, CompletableFuture<V> promise, Duration timeout) {
        try {
            return promise.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            throw new CoalescerTimeoutException(key, timeout);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CoalescerException("Interrupted while waiting for coalesced result — key: " + key, ex);
        } catch (ExecutionException ex) {
            Throwable cause = unwrap(ex);
            if (cause instanceof RuntimeException rte) throw rte;
            if (cause instanceof Error err)             throw err;
            throw new CoalescerException("Upstream loader failed for key: " + key, cause);
        }
    }

    /** Peels one layer of {@link CompletionException} or {@link ExecutionException} wrapper. */
    private static Throwable unwrap(Throwable ex) {
        return (ex instanceof CompletionException || ex instanceof ExecutionException)
                && ex.getCause() != null ? ex.getCause() : ex;
    }

    // -----------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------

    /**
     * Fluent builder for {@link RequestCoalescer}.
     *
     * <pre>{@code
     * RequestCoalescer<String, byte[]> coalescer =
     *     RequestCoalescer.<String, byte[]>builder()
     *         .executor(myExecutor)
     *         .defaultTimeout(Duration.ofSeconds(10))
     *         .metrics(micrometerAdapter)
     *         .build();
     * }</pre>
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static final class Builder<K, V> {

        private Executor         executor       = VIRTUAL_EXECUTOR;
        private Duration         defaultTimeout = DEFAULT_TIMEOUT;
        private CoalesceXMetrics metrics        = CoalesceXMetrics.NOOP;

        private Builder() {}

        /**
         * Executor used to dispatch upstream loader calls.
         *
         * <p>Defaults to a virtual-thread-per-task executor. Override when integrating
         * with a managed pool (e.g., a platform-thread pool for CPU-bound loaders, or
         * an application-server executor for lifecycle control).
         */
        public Builder<K, V> executor(Executor executor) {
            this.executor = Objects.requireNonNull(executor, "executor must not be null");
            return this;
        }

        /**
         * Default timeout applied when {@link #compute(Object, Supplier)} is called
         * without an explicit timeout. Defaults to {@code 30 seconds}.
         *
         * <p>Set this to match your SLA for the slowest possible upstream response.
         * Callers that exceed this limit receive a {@link CoalescerTimeoutException}
         * without affecting other in-flight waiters.
         */
        public Builder<K, V> defaultTimeout(Duration timeout) {
            this.defaultTimeout = Objects.requireNonNull(timeout, "timeout must not be null");
            return this;
        }

        /**
         * Pluggable metrics hook (Micrometer, OpenTelemetry, {@link LoggingCoalesceXMetrics}, …).
         * Defaults to {@link CoalesceXMetrics#NOOP}.
         *
         * <p>See {@link CoalesceXMetrics} for a ready-to-paste Micrometer example.
         */
        public Builder<K, V> metrics(CoalesceXMetrics metrics) {
            this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
            return this;
        }

        /** Constructs the configured {@link RequestCoalescer}. */
        public RequestCoalescer<K, V> build() {
            return new RequestCoalescer<>(this);
        }
    }
}