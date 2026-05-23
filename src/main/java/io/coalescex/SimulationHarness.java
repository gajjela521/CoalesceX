package io.coalescex;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstration harness showcasing CoalesceX thundering-herd prevention across
 * three realistic scenarios.
 *
 * <p>Run via: {@code ./gradlew run}
 */
public class SimulationHarness {

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║   CoalesceX — Request Coalescing Demo        ║");
        System.out.println("╚══════════════════════════════════════════════╝\n");

        scenario1_ThunderingHerd();
        scenario2_MultipleDistinctKeys();
        scenario3_FailureEviction();
    }

    // ------------------------------------------------------------------
    // Scenario 1 — Classic thundering herd on one hot key
    // ------------------------------------------------------------------

    static void scenario1_ThunderingHerd() throws Exception {
        System.out.println("━━━ Scenario 1: Thundering Herd (10,000 threads, one hot key) ━━━\n");

        RequestCoalescer<String, String> coalescer = RequestCoalescer.create();
        AtomicInteger upstreamHits = new AtomicInteger();
        int threadCount = 10_000;

        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>(threadCount);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return coalescer.compute("HOT_KEY:customer:metadata:tier", () -> {
                        upstreamHits.incrementAndGet();
                        try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                        return "{\"tier\":\"ENTERPRISE\",\"status\":\"ACTIVE\"}";
                    });
                }));
            }

            Instant start = Instant.now();
            startGate.countDown();
            for (Future<String> f : futures) f.get(10, TimeUnit.SECONDS);
            long elapsed = Duration.between(start, Instant.now()).toMillis();

            CoalescerMetrics m = coalescer.metrics();
            System.out.printf("  Concurrent callers     : %,d%n", threadCount);
            System.out.printf("  Actual upstream hits   : %d%n",  upstreamHits.get());
            System.out.printf("  Coalescing efficiency  : %.2f%%%n", m.coalescingEfficiency() * 100);
            System.out.printf("  Total wall-clock time  : %d ms%n%n", elapsed);
        }
    }

    // ------------------------------------------------------------------
    // Scenario 2 — Multiple distinct keys (independent upstream calls)
    // ------------------------------------------------------------------

    static void scenario2_MultipleDistinctKeys() throws Exception {
        System.out.println("━━━ Scenario 2: 50 distinct keys × 200 threads each ━━━\n");

        RequestCoalescer<String, String> coalescer = RequestCoalescer.create();
        AtomicInteger upstreamHits = new AtomicInteger();
        int keyCount   = 50;
        int perKey     = 200;
        int totalCalls = keyCount * perKey;

        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>(totalCalls);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int k = 0; k < keyCount; k++) {
                String key = "product:sku:" + k;
                for (int t = 0; t < perKey; t++) {
                    futures.add(pool.submit(() -> {
                        startGate.await();
                        return coalescer.compute(key, () -> {
                            upstreamHits.incrementAndGet();
                            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                            return "sku-data";
                        });
                    }));
                }
            }

            startGate.countDown();
            for (Future<String> f : futures) f.get(10, TimeUnit.SECONDS);

            System.out.printf("  Total compute() calls  : %,d%n", totalCalls);
            System.out.printf("  Actual upstream hits   : %d  (expected ≈ %d)%n",
                    upstreamHits.get(), keyCount);
            System.out.printf("  Coalescing efficiency  : %.2f%%%n%n",
                    coalescer.metrics().coalescingEfficiency() * 100);
        }
    }

    // ------------------------------------------------------------------
    // Scenario 3 — Failure eviction: failed requests are retried
    // ------------------------------------------------------------------

    static void scenario3_FailureEviction() throws Exception {
        System.out.println("━━━ Scenario 3: Failure Eviction (failed entries are retried) ━━━\n");

        RequestCoalescer<String, String> coalescer = RequestCoalescer.create();
        AtomicInteger attempt = new AtomicInteger();

        // First call — loader throws; the failed future must be evicted.
        try {
            coalescer.compute("flaky-service", () -> {
                attempt.incrementAndGet();
                throw new RuntimeException("Service temporarily unavailable");
            });
        } catch (RuntimeException ex) {
            System.out.println("  Attempt 1 failed (expected): " + ex.getMessage());
        }

        // Second call — a fresh loader is dispatched because the failed future was evicted.
        String result = coalescer.compute("flaky-service", () -> {
            attempt.incrementAndGet();
            return "recovered-payload";
        });

        System.out.printf("  Attempt 2 succeeded: %s%n", result);
        System.out.printf("  Total loader invocations: %d (failure was correctly evicted)%n%n",
                attempt.get());

        CoalescerMetrics m = coalescer.metrics();
        System.out.println("  Final metrics: " + m);
        System.out.println();
    }
}