package com.github.gajjela521.coalescex;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SimulationHarness: Demonstrates the CoalesceX request-collapsing
 * utility in action with 10,000 concurrent virtual threads.
 *
 * Shows the dramatic reduction in actual upstream database hits
 * when identical requests are coalesced rather than executed
 * in parallel.
 */
public class SimulationHarness {
    public static void main(String[] args) throws InterruptedException {
        RequestCoalescer<String, String> gateway = new RequestCoalescer<>();
        AtomicInteger databaseHitCounter = new AtomicInteger(0);

        int totalConcurrentUsers = 10_000;
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch completionSignal = new CountDownLatch(totalConcurrentUsers);

        System.out.println("🚀 CoalesceX Simulation Harness");
        System.out.println("===============================");
        System.out.println("Initializing " + totalConcurrentUsers + " virtual threads simulating a thundering herd...\n");

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < totalConcurrentUsers; i++) {
                executor.submit(() -> {
                    try {
                        startSignal.await(); // Synchronize all virtual threads to assault simultaneously

                        String payload = gateway.compute("SQL_QUERY_CUSTOMER_METADATA_CA_94107", () -> {
                            // This block simulates the expensive network bottleneck
                            databaseHitCounter.incrementAndGet();
                            try {
                                Thread.sleep(200); // Simulate network latency
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return "{ \"status\": \"ACTIVE\", \"tier\": \"ENTERPRISE\" }";
                        });

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        completionSignal.countDown();
                    }
                });
            }

            Instant start = Instant.now();
            startSignal.countDown(); // Release the floodgates!
            completionSignal.await();
            Instant end = Instant.now();

            System.out.println("=== PERFORMANCE REPORT ===\n");
            System.out.println("Total Simulated Incoming Requests : " + totalConcurrentUsers);
            System.out.println("Actual Physical Upstream DB Hits   : " + databaseHitCounter.get());
            System.out.println("Request Coalescing Efficiency      : " +
                String.format("%.2f%%", (1.0 - (double) databaseHitCounter.get() / totalConcurrentUsers) * 100));
            System.out.println("Total Execution Time               : " +
                Duration.between(start, end).toMillis() + " ms\n");

            System.out.println("✅ CoalesceX successfully prevented " + (totalConcurrentUsers - databaseHitCounter.get()) +
                " redundant upstream requests!");
        }
    }
}

