package com.github.gajjela521.coalescex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Non-blocking variant of {@link RequestCoalescer} that returns a {@link CompletableFuture}
 * instead of blocking the calling thread.
 *
 * <p>Designed for reactive or event-loop environments — Spring WebFlux, Vert.x, Netty —
 * where blocking a caller thread is prohibited. The caller receives a {@code CompletableFuture}
 * immediately and registers a continuation on it via {@code thenApply}, {@code thenAccept}, etc.
 *
 * <p>The same coalescing semantics apply: if an identical upstream request is already in-flight,
 * all concurrent callers receive the <em>same</em> {@code CompletableFuture} instance — no
 * additional upstream call is dispatched.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * AsyncRequestCoalescer<String, ProductDto> coalescer = AsyncRequestCoalescer.create();
 *
 * // Returns immediately — no thread is blocked
 * CompletableFuture<ProductDto> future = coalescer.compute(
 *     "sku:42",
 *     () -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
 *                     .thenApply(r -> parseProduct(r.body()))
 * );
 *
 * future.thenAccept(product -> sendResponse(ctx, product));
 * }</pre>
 *
 * <h2>Spring WebFlux / Project Reactor bridge</h2>
 * <pre>{@code
 * Mono<ProductDto> mono = Mono.fromFuture(
 *     coalescer.compute("sku:42", () -> reactorToFuture(productService.findBySku("42")))
 * );
 * }</pre>
 *
 * <h2>Design Notes</h2>
 * <p>Applies the same {@code compute()} (not {@code computeIfAbsent()}) pattern and
 * remove-before-complete ordering as {@link RequestCoalescer}. See that class's Javadoc
 * for the full rationale regarding Virtual Thread carrier-pinning on Java 21–23.
 *
 * @param <K> type of the lookup key
 * @param <V> type of the computed value
 */
public final class AsyncRequestCoalescer<K, V> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncRequestCoalescer.class);

    private static final Executor VIRTUAL_EXECUTOR = task -> Thread.ofVirtual().start(task);

    private final ConcurrentHashMap<K, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();
    private final Executor         executor;
    private final CoalesceXMetrics metrics;

    private AsyncRequestCoalescer(Builder<K, V> builder) {
        this.executor = builder.executor;
        this.metrics  = builder.metrics;
    }

    // -----------------------------------------------------------------------
    // Factory
    // -----------------------------------------------------------------------

    /** Creates an instance with a virtual-thread-per-task executor and no-op metrics. */
    public static <K, V> AsyncRequestCoalescer<K, V> create() {
        return AsyncRequestCoalescer.<K, V>builder().build();
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    // -----------------------------------------------------------------------
    // Core API
    // -----------------------------------------------------------------------

    /**
     * Returns a {@link CompletableFuture} that resolves to the value for {@code key}.
     *
     * <p>If a request for {@code key} is already in-flight, the same {@code CompletableFuture}
     * is returned — the {@code asyncLoader} is not called. Otherwise, {@code asyncLoader} is
     * invoked once to obtain a future representing the upstream operation.
     *
     * <p>This method never blocks. The returned future completes on whichever thread the
     * upstream operation completes on.
     *
     * @param key         resource identifier; must not be {@code null}
     * @param asyncLoader supplier that returns the upstream {@link CompletableFuture};
     *                    must not be {@code null}; called at most once per coalescing window
     * @return a shared future that resolves when the upstream request completes
     * @throws NullPointerException if {@code key} or {@code asyncLoader} is {@code null}
     */
    public CompletableFuture<V> compute(K key, Supplier<CompletableFuture<V>> asyncLoader) {
        Objects.requireNonNull(key,         "key must not be null");
        Objects.requireNonNull(asyncLoader, "asyncLoader must not be null");

        metrics.onRequest(key);

        final boolean[] shouldDispatch = {false};

        CompletableFuture<V> promise = inFlight.compute(key, (k, existing) -> {
            if (existing != null && !existing.isDone()) {
                return existing;
            }
            shouldDispatch[0] = true;
            return new CompletableFuture<>();
        });

        if (shouldDispatch[0]) {
            metrics.onUpstreamDispatched(key);
            log.debug("Async upstream dispatch — key [{}]", key);
            dispatchAsync(key, asyncLoader, promise);
        } else {
            metrics.onCoalesced(key);
            log.debug("Async coalesced — joining in-flight request for key [{}]", key);
        }

        return promise;
    }

    /** Number of keys currently awaiting upstream resolution. */
    public int activeFlightCount() {
        return inFlight.size();
    }

    /**
     * Cancels all in-flight futures and clears the registry.
     * Wire into application shutdown hooks to avoid resource leaks.
     */
    @Override
    public void close() {
        inFlight.forEach((k, f) -> f.cancel(true));
        inFlight.clear();
        log.debug("AsyncRequestCoalescer closed — all in-flight futures cancelled");
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private void dispatchAsync(K key, Supplier<CompletableFuture<V>> asyncLoader,
                                CompletableFuture<V> promise) {
        final long startNanos = System.nanoTime();

        CompletableFuture<V> upstream;
        try {
            upstream = asyncLoader.get();
        } catch (Throwable t) {
            // asyncLoader.get() itself threw synchronously
            inFlight.remove(key, promise);
            promise.completeExceptionally(t);
            metrics.onUpstreamFailure(key, t, System.nanoTime() - startNanos);
            return;
        }

        if (upstream == null) {
            NullPointerException npe = new NullPointerException("asyncLoader returned null future for key: " + key);
            inFlight.remove(key, promise);
            promise.completeExceptionally(npe);
            metrics.onUpstreamFailure(key, npe, System.nanoTime() - startNanos);
            return;
        }

        upstream.whenCompleteAsync((value, ex) -> {
            long latencyNanos = System.nanoTime() - startNanos;
            if (ex != null) {
                Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                        ? ex.getCause() : ex;
                inFlight.remove(key, promise);
                promise.completeExceptionally(cause);
                metrics.onUpstreamFailure(key, cause, latencyNanos);
                log.warn("Async upstream failed for key [{}]: {}", key, cause.toString());
            } else {
                inFlight.remove(key, promise);
                promise.complete(value);
                metrics.onUpstreamSuccess(key, latencyNanos);
                log.debug("Async upstream resolved for key [{}]", key);
            }
        }, executor);
    }

    // -----------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------

    public static final class Builder<K, V> {

        private Executor         executor = VIRTUAL_EXECUTOR;
        private CoalesceXMetrics metrics  = CoalesceXMetrics.NOOP;

        private Builder() {}

        /** Executor used for {@code whenCompleteAsync} callbacks. Defaults to virtual threads. */
        public Builder<K, V> executor(Executor executor) {
            this.executor = Objects.requireNonNull(executor, "executor must not be null");
            return this;
        }

        /** Pluggable metrics hook. Defaults to {@link CoalesceXMetrics#NOOP}. */
        public Builder<K, V> metrics(CoalesceXMetrics metrics) {
            this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
            return this;
        }

        public AsyncRequestCoalescer<K, V> build() {
            return new AsyncRequestCoalescer<>(this);
        }
    }
}
