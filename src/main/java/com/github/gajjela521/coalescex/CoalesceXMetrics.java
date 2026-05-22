package com.github.gajjela521.coalescex;

/**
 * SPI for plugging custom metrics collection (Micrometer, OpenTelemetry, etc.)
 * into a {@link RequestCoalescer} or {@link AsyncRequestCoalescer}.
 *
 * <p>All methods carry empty default implementations — implement only the hooks you need.
 * The built-in {@link #NOOP} constant has zero overhead and is the default when no
 * metrics implementation is supplied.
 *
 * <h2>Micrometer example</h2>
 * <pre>{@code
 * MeterRegistry registry = ...;
 * Counter requests  = registry.counter("coalescex.requests");
 * Counter coalesced = registry.counter("coalescex.coalesced");
 * Timer   upstream  = registry.timer("coalescex.upstream.latency");
 * Counter failures  = registry.counter("coalescex.failures");
 *
 * RequestCoalescer<String, ProductDto> coalescer =
 *     RequestCoalescer.<String, ProductDto>builder()
 *         .metrics(new CoalesceXMetrics() {
 *             public void onRequest(Object key)                           { requests.increment(); }
 *             public void onCoalesced(Object key)                         { coalesced.increment(); }
 *             public void onUpstreamSuccess(Object key, long latencyNanos) {
 *                 upstream.record(latencyNanos, TimeUnit.NANOSECONDS);
 *             }
 *             public void onUpstreamFailure(Object key, Throwable ex, long latencyNanos) {
 *                 failures.increment();
 *             }
 *         })
 *         .build();
 * }</pre>
 *
 * <h2>OpenTelemetry example</h2>
 * <pre>{@code
 * LongCounter otelRequests = meter.counterBuilder("coalescex.requests").build();
 * // wire the other hooks similarly
 * }</pre>
 */
public interface CoalesceXMetrics {

    /**
     * Called on every invocation of {@code compute()}, including coalesced ones.
     * @param key the resource key being requested
     */
    default void onRequest(Object key) {}

    /**
     * Called when a request is coalesced — i.e., an identical upstream call is already
     * in-flight and this caller will receive its result.
     * @param key the resource key
     */
    default void onCoalesced(Object key) {}

    /**
     * Called when a new upstream loader is dispatched (i.e., this is the first caller
     * for {@code key} in the current coalescing window).
     * @param key the resource key
     */
    default void onUpstreamDispatched(Object key) {}

    /**
     * Called when an upstream loader completes successfully.
     * @param key          the resource key
     * @param latencyNanos wall-clock duration of the upstream call in nanoseconds
     */
    default void onUpstreamSuccess(Object key, long latencyNanos) {}

    /**
     * Called when an upstream loader fails. The failed entry is evicted from the
     * coalescer so the next caller can retry.
     * @param key          the resource key
     * @param cause        the exception thrown by the loader
     * @param latencyNanos wall-clock duration until failure in nanoseconds
     */
    default void onUpstreamFailure(Object key, Throwable cause, long latencyNanos) {}

    /**
     * Zero-overhead no-op implementation (the default when no metrics are configured).
     * The JIT eliminates all method calls through this instance.
     */
    CoalesceXMetrics NOOP = new CoalesceXMetrics() {};
}
