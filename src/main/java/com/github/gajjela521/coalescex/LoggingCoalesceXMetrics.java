package com.github.gajjela521.coalescex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;

/**
 * Built-in {@link CoalesceXMetrics} implementation that accumulates counters using
 * {@link LongAdder}.
 *
 * <p>{@code LongAdder} outperforms {@link java.util.concurrent.atomic.AtomicLong} under high
 * write concurrency by maintaining a set of per-CPU cells that are summed lazily on read —
 * ideal for high-throughput coalescing workloads where many threads increment counters
 * simultaneously.
 *
 * <p>Call {@link #logSummary()} from a scheduled task for lightweight observability with no
 * external dependency:
 * <pre>{@code
 * LoggingCoalesceXMetrics metrics = new LoggingCoalesceXMetrics();
 *
 * RequestCoalescer<String, ProductDto> coalescer =
 *     RequestCoalescer.<String, ProductDto>builder()
 *         .metrics(metrics)
 *         .build();
 *
 * // In a Spring @Scheduled method or ScheduledExecutorService:
 * metrics.logSummary();
 * }</pre>
 */
public final class LoggingCoalesceXMetrics implements CoalesceXMetrics {

    private static final Logger log = LoggerFactory.getLogger(LoggingCoalesceXMetrics.class);

    private final LongAdder totalRequests      = new LongAdder();
    private final LongAdder coalescedRequests  = new LongAdder();
    private final LongAdder upstreamDispatched = new LongAdder();
    private final LongAdder upstreamFailures   = new LongAdder();

    @Override
    public void onRequest(Object key) {
        totalRequests.increment();
    }

    @Override
    public void onCoalesced(Object key) {
        coalescedRequests.increment();
    }

    @Override
    public void onUpstreamDispatched(Object key) {
        upstreamDispatched.increment();
    }

    @Override
    public void onUpstreamFailure(Object key, Throwable cause, long latencyNanos) {
        upstreamFailures.increment();
    }

    /**
     * Returns a point-in-time snapshot of accumulated counters.
     *
     * <p>Uses {@link LongAdder#sum()}, which is eventually consistent — a small number of
     * in-flight increments may not be reflected immediately. For monitoring and dashboards
     * this is always acceptable.
     */
    public CoalescerMetrics snapshot() {
        long total    = totalRequests.sum();
        long upstream = upstreamDispatched.sum();
        long coalesced = coalescedRequests.sum();
        long failed   = upstreamFailures.sum();
        return new CoalescerMetrics(total, upstream, coalesced, failed, 0);
    }

    /**
     * Logs current counters at INFO level via SLF4J.
     * Safe to call from any thread at any frequency.
     */
    public void logSummary() {
        CoalescerMetrics m = snapshot();
        log.info("CoalesceX metrics — total={} upstream={} coalesced={} failed={} efficiency={}%",
                m.totalRequests(), m.upstreamRequests(), m.coalescedRequests(),
                m.failedRequests(),
                String.format("%.2f", m.coalescingEfficiency() * 100));
    }

    /**
     * Resets all counters to zero. Useful in tests or after a periodic metrics flush
     * where you want rate-based rather than cumulative counters.
     */
    public void reset() {
        totalRequests.reset();
        coalescedRequests.reset();
        upstreamDispatched.reset();
        upstreamFailures.reset();
    }
}