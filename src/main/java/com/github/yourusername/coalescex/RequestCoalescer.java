package com.github.yourusername.coalescex;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoalesceX: An enterprise-grade request-collapsing gateway utility
 * optimized for high-throughput Virtual Thread applications.
 *
 * This class prevents thundering herd distributed I/O storms by coalescing
 * concurrent requests for identical resources. When multiple threads request
 * the same resource, only one upstream request is made, and all threads
 * transparently share the result.
 */
public class RequestCoalescer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(RequestCoalescer.class);

    // Tracks active, in-flight data computations transparently
    private final ConcurrentHashMap<K, CompletableFuture<V>> inFlightRequests = new ConcurrentHashMap<>();

    /**
     * Executes the loader logic. If an identical key is already in flight,
     * the calling thread suspends until the existing flight finishes, preventing
     * duplicate upstream requests.
     *
     * @param key          The unique identifier for the specific business asset.
     * @param heavyFetcher The expensive network/DB lookup function.
     * @return The computed result payload.
     */
    public V compute(K key, Supplier<V> heavyFetcher) {
        CompletableFuture<V> future = inFlightRequests.computeIfAbsent(key, k -> {
            log.debug("No active flight for key [{}]. Launching upstream request.", k);

            // CompletableFuture.supplyAsync executes cleanly within the
            // current execution engine without pinning underlying carrier threads.
            return CompletableFuture.supplyAsync(heavyFetcher);
        });

        try {
            // Join suspends the Virtual Thread cleanly, unmounting it from the OS carrier.
            V result = future.join();
            return result;
        } finally {
            // Ensure the cleanup is strictly localized so subsequent requests hit a fresh loader cycle
            // only after the initial flight completely resolves.
            inFlightRequests.computeIfPresent(key, (k, existingFuture) -> {
                if (existingFuture.isDone()) {
                    log.debug("Cleaning completed flight reference for key [{}].", k);
                    return null;
                }
                return existingFuture;
            });
        }
    }

    /**
     * Inspects current system load metrics.
     * @return count of active database/network flights currently executing.
     */
    public int getActiveFlightCount() {
        return inFlightRequests.size();
    }
}

