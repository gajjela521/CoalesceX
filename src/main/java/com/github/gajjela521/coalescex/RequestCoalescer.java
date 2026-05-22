package com.github.gajjela521.coalescex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
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
 *   <li><b>Promise pattern</b> — the map stores a bare {@code CompletableFuture} (a "promise")
 *       that is completed asynchronously. This cleanly separates registration from execution
 *       and prevents any work from happening inside a {@code computeIfAbsent} call.</li>
 *   <li><b>Safe cleanup via {@code remove(key, value)}</b> — cleanup uses the two-arg
 *       {@link ConcurrentHashMap#remove(Object, Object)}, which only removes the entry if
 *       the value still matches the completing future. A late-arriving cleanup can never
 *       accidentally evict a newer in-flight entry for the same key.</li>
 *   <li><b>Failure eviction</b> — a future that completes exceptionally is immediately evicted
 *       so the next caller dispatches a fresh upstream request rather than replaying the error.</li>
 * </ul>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * // Zero-config factory (30 s timeout, virtual-thread executor)
 * RequestCoalescer<String, CustomerData> coalescer = RequestCoalescer.create();
 *
 * // With a custom timeout
 * RequestCoalescer<String, CustomerData> coalescer = RequestCoalescer.<String, CustomerData>builder()
 *     .defaultTimeout(Duration.ofSeconds(5))
 *     .build();
 *
 * // Usage
 * CustomerData data = coalescer.compute("customer:42", () -> db.loadCustomer(42));
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>All public methods are fully thread-safe with no external synchronization required.
 *
 * @param <K> type of the lookup key — should implement {@link Object#equals} and
 *            {@link Object#hashCode} correctly
 * @param <V> type of the computed value
 */
public final class RequestCoalescer<K, V> implements Coalescer<K, V>, AutoCloseable {

    private static final Logger   log             = LoggerFactory.getLogger(RequestCoalescer.class);
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Default executor: one virtual thread per upstream task.
     * No pool management — the JVM scheduler handles multiplexing onto OS threads.
     */
    private static final Executor VIRTUAL_EXECUTOR = task -> Thread.ofVirtual().start(task);

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Live registry of in-flight upstream requests. */
    private final ConcurrentHashMap<K, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();

    private final Executor executor;
    private final Duration defaultTimeout;

    // Metrics — updated with plain volatile-like semantics via AtomicLong.
    private final AtomicLong totalRequests    = new AtomicLong();
    private final AtomicLong upstreamRequests = new AtomicLong();
    private final AtomicLong failedRequests   = new AtomicLong();

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    private RequestCoalescer(Builder<K, V> builder) {
        this.executor       = builder.executor;
        this.defaultTimeout = builder.defaultTimeout;
    }

    /**
     * Creates a coalescer with default settings: 30-second timeout and a
     * virtual-thread-per-task executor.
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

        totalRequests.incrementAndGet();

        // Track whether this call is responsible for dispatching the upstream work.
        final boolean[] isUpstreamCaller = {false};

        // Atomically: if no in-flight entry exists for this key, register a new promise
        // and mark this thread as the upstream caller. All concurrent callers for the
        // same key receive the identical promise reference.
        CompletableFuture<V> promise = inFlight.computeIfAbsent(key, k -> {
            isUpstreamCaller[0] = true;
            return new CompletableFuture<>();
        });

        if (isUpstreamCaller[0]) {
            upstreamRequests.incrementAndGet();
            log.debug("Upstream dispatch — key [{}]", key);
            dispatchUpstream(key, loader, promise);
        } else {
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
        long total    = totalRequests.get();
        long upstream = upstreamRequests.get();
        long failed   = failedRequests.get();
        return new CoalescerMetrics(total, upstream, total - upstream, failed, inFlight.size());
    }

    /**
     * Cancels all in-flight requests and clears the internal registry.
     * Safe to call at application shutdown. After closing, this instance
     * must not be used.
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
     * Submits the upstream loader to the executor. On completion, the promise is
     * resolved and the key is evicted from the registry.
     *
     * <p>Failure eviction is intentional: a future that completed exceptionally is
     * removed so the next wave of callers triggers a fresh retry rather than
     * replaying a stale error.
     */
    private void dispatchUpstream(K key, Supplier<V> loader, CompletableFuture<V> promise) {
        CompletableFuture.supplyAsync(loader, executor)
                .whenComplete((value, ex) -> {
                    if (ex != null) {
                        Throwable cause = unwrap(ex);
                        promise.completeExceptionally(cause);
                        failedRequests.incrementAndGet();
                        // Evict immediately on failure — next callers must retry, not replay the error.
                        inFlight.remove(key, promise);
                        log.warn("Upstream request failed for key [{}]: {}", key, cause.toString());
                    } else {
                        promise.complete(value);
                        // Conditional remove: only evicts if the map still holds THIS promise.
                        // A later in-flight entry for the same key is left untouched.
                        inFlight.remove(key, promise);
                        log.debug("Upstream request resolved for key [{}]", key);
                    }
                });
    }

    /**
     * Blocks the calling thread until the promise resolves or the timeout elapses.
     * Virtual Threads unmount from their carrier during the wait — no OS thread is held.
     *
     * <p>Exception handling contract:
     * <ul>
     *   <li>{@link RuntimeException} from the loader → rethrown directly</li>
     *   <li>{@link Error} from the loader → rethrown directly</li>
     *   <li>Checked exception from the loader → wrapped in {@link CoalescerException}</li>
     *   <li>Timeout → throws {@link CoalescerTimeoutException}</li>
     *   <li>Thread interrupted → restores interrupt flag, throws {@link CoalescerException}</li>
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

    /** Peels off {@link CompletionException}/{@link ExecutionException} wrappers. */
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
     * RequestCoalescer<String, byte[]> coalescer = RequestCoalescer.<String, byte[]>builder()
     *     .executor(myCustomExecutor)
     *     .defaultTimeout(Duration.ofSeconds(10))
     *     .build();
     * }</pre>
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static final class Builder<K, V> {

        private Executor executor       = VIRTUAL_EXECUTOR;
        private Duration defaultTimeout = DEFAULT_TIMEOUT;

        private Builder() {}

        /**
         * Executor used to dispatch upstream loader calls.
         *
         * <p>Defaults to a virtual-thread-per-task executor. Override when you need
         * to integrate with a managed thread pool (e.g., a platform-thread pool for
         * CPU-bound loaders, or an application-server executor for lifecycle control).
         */
        public Builder<K, V> executor(Executor executor) {
            this.executor = Objects.requireNonNull(executor, "executor must not be null");
            return this;
        }

        /**
         * Default timeout applied when {@link #compute(Object, Supplier)} is called
         * without an explicit timeout argument. Defaults to {@code 30 seconds}.
         *
         * <p>Set this to a value that reflects your SLA for the slowest possible
         * upstream response. Callers exceeding this limit receive a
         * {@link CoalescerTimeoutException} without affecting other waiters.
         */
        public Builder<K, V> defaultTimeout(Duration timeout) {
            this.defaultTimeout = Objects.requireNonNull(timeout, "timeout must not be null");
            return this;
        }

        /** Constructs the configured {@link RequestCoalescer}. */
        public RequestCoalescer<K, V> build() {
            return new RequestCoalescer<>(this);
        }
    }
}