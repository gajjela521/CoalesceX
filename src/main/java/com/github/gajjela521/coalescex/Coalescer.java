package com.github.gajjela521.coalescex;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Contract for a request-coalescing gateway that collapses concurrent requests
 * for the same key into a single upstream execution.
 *
 * <p>Typical use-case: a cache miss triggers thousands of identical database queries.
 * A {@code Coalescer} ensures only one query runs; every other caller transparently
 * receives the same result once it resolves.
 *
 * @param <K> type of the lookup key
 * @param <V> type of the computed value
 */
public interface Coalescer<K, V> {

    /**
     * Returns the value for {@code key}, executing {@code loader} at most once across
     * all concurrent callers for that key. Uses the implementation's default timeout.
     *
     * @param key    resource identifier; must not be {@code null}
     * @param loader upstream computation to execute on a cache miss; must not be {@code null}
     * @return the computed value
     * @throws CoalescerException        if the loader throws a checked exception
     * @throws CoalescerTimeoutException if the default timeout is exceeded
     * @throws RuntimeException          if the loader throws an unchecked exception (rethrown as-is)
     */
    V compute(K key, Supplier<V> loader);

    /**
     * Variant of {@link #compute(Object, Supplier)} with a per-call timeout override.
     *
     * @param key     resource identifier; must not be {@code null}
     * @param loader  upstream computation; must not be {@code null}
     * @param timeout maximum time to wait for the result; must not be {@code null}
     * @return the computed value
     * @throws CoalescerTimeoutException if {@code timeout} is exceeded
     */
    V compute(K key, Supplier<V> loader, Duration timeout);

    /**
     * Forcibly evicts any in-flight or pending entry for {@code key}. The next
     * call to {@link #compute} for that key will dispatch a fresh upstream request.
     * An in-flight future is cancelled; callers already blocked on it receive a
     * {@link java.util.concurrent.CancellationException}.
     *
     * @param key resource identifier to evict; must not be {@code null}
     */
    void invalidate(K key);

    /**
     * @return number of keys currently awaiting upstream resolution
     */
    int activeFlightCount();

    /**
     * @return a point-in-time snapshot of coalescing statistics
     */
    CoalescerMetrics metrics();
}