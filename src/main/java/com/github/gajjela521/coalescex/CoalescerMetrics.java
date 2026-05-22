package com.github.gajjela521.coalescex;

/**
 * Immutable, point-in-time snapshot of {@link RequestCoalescer} runtime statistics.
 *
 * <p>Obtain via {@link RequestCoalescer#metrics()}.
 *
 * @param totalRequests    total calls to {@code compute} since construction
 * @param upstreamRequests number of times an actual upstream loader was dispatched
 * @param coalescedRequests requests that joined an existing in-flight upstream call
 * @param failedRequests   upstream loader invocations that completed exceptionally
 * @param activeFlights    keys currently awaiting upstream resolution (instantaneous)
 */
public record CoalescerMetrics(
        long totalRequests,
        long upstreamRequests,
        long coalescedRequests,
        long failedRequests,
        int  activeFlights
) {
    /**
     * Fraction of total requests served without an upstream call (0.0–1.0).
     * A value of {@code 0.99} means 99 % of requests were coalesced.
     */
    public double coalescingEfficiency() {
        return totalRequests == 0 ? 0.0 : (double) coalescedRequests / totalRequests;
    }

    @Override
    public String toString() {
        return String.format(
                "CoalescerMetrics{total=%d, upstream=%d, coalesced=%d, failed=%d, active=%d, efficiency=%.2f%%}",
                totalRequests, upstreamRequests, coalescedRequests, failedRequests, activeFlights,
                coalescingEfficiency() * 100);
    }
}