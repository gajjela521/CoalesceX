package io.coalescex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class RequestCoalescerTest {

    private RequestCoalescer<String, String> coalescer;

    @BeforeEach
    void setUp() {
        coalescer = RequestCoalescer.create();
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ------------------------------------------------------------------
    // Basic correctness
    // ------------------------------------------------------------------

    @Test
    void returnsLoaderResult() {
        assertEquals("hello", coalescer.compute("k", () -> "hello"));
    }

    @Test
    void separateKeysReceiveIndependentResults() {
        assertEquals("a", coalescer.compute("key-a", () -> "a"));
        assertEquals("b", coalescer.compute("key-b", () -> "b"));
    }

    @Test
    void nullKeyThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> coalescer.compute(null, () -> "v"));
    }

    @Test
    void nullLoaderThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> coalescer.compute("k", null));
    }

    // ------------------------------------------------------------------
    // Thundering herd coalescing
    // ------------------------------------------------------------------

    @Test
    void coalescesConcurrentRequestsForSameKey() throws Exception {
        AtomicInteger upstreamCalls = new AtomicInteger();
        int threadCount = 500;
        CountDownLatch startGate  = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>(threadCount);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return coalescer.compute("thundering-herd", () -> {
                        upstreamCalls.incrementAndGet();
                        sleep(200); // hold the gate long enough for all threads to arrive
                        return "payload";
                    });
                }));
            }

            startGate.countDown();
            for (Future<String> f : futures) {
                assertEquals("payload", f.get(10, TimeUnit.SECONDS));
            }
        }

        assertEquals(1, upstreamCalls.get(),
                "Expected exactly 1 upstream call for a synchronous thundering herd");
    }

    @Test
    void distinctKeysProduceIndependentUpstreamCalls() throws Exception {
        int keyCount = 10;
        int perKey   = 50;
        AtomicInteger upstreamCalls = new AtomicInteger();
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>(keyCount * perKey);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int k = 0; k < keyCount; k++) {
                String key = "key-" + k;
                for (int t = 0; t < perKey; t++) {
                    futures.add(pool.submit(() -> {
                        startGate.await();
                        return coalescer.compute(key, () -> {
                            upstreamCalls.incrementAndGet();
                            sleep(100);
                            return "result";
                        });
                    }));
                }
            }

            startGate.countDown();
            for (Future<String> f : futures) f.get(10, TimeUnit.SECONDS);
        }

        assertEquals(keyCount, upstreamCalls.get(),
                "Expected exactly one upstream call per distinct key");
    }

    // ------------------------------------------------------------------
    // Failure handling and eviction
    // ------------------------------------------------------------------

    @Test
    void runtimeExceptionFromLoaderIsPropagatedDirectly() {
        RuntimeException boom = new RuntimeException("db unavailable");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> coalescer.compute("fail-key", () -> { throw boom; }));
        assertSame(boom, thrown, "Original exception should be rethrown without wrapping");
    }

    @Test
    void failedFutureIsEvictedSoNextCallRetries() {
        AtomicInteger calls = new AtomicInteger();

        assertThrows(RuntimeException.class, () ->
                coalescer.compute("flaky", () -> {
                    calls.incrementAndGet();
                    throw new RuntimeException("first failure");
                }));

        String result = coalescer.compute("flaky", () -> {
            calls.incrementAndGet();
            return "recovered";
        });

        assertEquals("recovered", result);
        assertEquals(2, calls.get(),
                "Failed entry should have been evicted, allowing a second upstream attempt");
    }

    @Test
    void allConcurrentWaitersReceiveSameExceptionOnFailure() throws Exception {
        int threadCount = 50;
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>(threadCount);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return coalescer.compute("all-fail", () -> {
                        sleep(50);
                        throw new RuntimeException("all fail");
                    });
                }));
            }

            startGate.countDown();
            int failCount = 0;
            for (Future<String> f : futures) {
                try {
                    f.get(5, TimeUnit.SECONDS);
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof RuntimeException) failCount++;
                }
            }
            assertEquals(threadCount, failCount,
                    "All concurrent waiters should have received the exception");
        }
    }

    // ------------------------------------------------------------------
    // Timeout
    // ------------------------------------------------------------------

    @Test
    void throwsCoalescerTimeoutExceptionWhenDefaultTimeoutExceeded() {
        RequestCoalescer<String, String> tight = RequestCoalescer.<String, String>builder()
                .defaultTimeout(Duration.ofMillis(50))
                .build();

        assertThrows(CoalescerTimeoutException.class, () ->
                tight.compute("slow-key", () -> {
                    sleep(500);
                    return "too late";
                }));
    }

    @Test
    void perCallTimeoutOverridesDefault() {
        assertThrows(CoalescerTimeoutException.class, () ->
                coalescer.compute("slow", () -> {
                    sleep(500);
                    return "nope";
                }, Duration.ofMillis(50)));
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    @Test
    void metricsReflectCoalescingAccurately() throws Exception {
        int threadCount = 100;
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>(threadCount);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return coalescer.compute("metrics-key", () -> {
                        sleep(150);
                        return "ok";
                    });
                }));
            }
            startGate.countDown();
            for (Future<String> f : futures) f.get(10, TimeUnit.SECONDS);
        }

        CoalescerMetrics m = coalescer.metrics();
        assertEquals(threadCount, m.totalRequests());
        assertEquals(1,           m.upstreamRequests());
        assertEquals(threadCount - 1, m.coalescedRequests());
        assertEquals(0,           m.failedRequests());
        assertTrue(m.coalescingEfficiency() > 0.98,
                "Efficiency should be close to 99% for a pure thundering herd");
    }

    // ------------------------------------------------------------------
    // Invalidate
    // ------------------------------------------------------------------

    @Test
    void invalidateAllowsFreshUpstreamCallAfterEviction() {
        AtomicInteger calls = new AtomicInteger();

        coalescer.compute("inv-key", () -> {
            calls.incrementAndGet();
            return "v1";
        });

        coalescer.invalidate("inv-key");

        coalescer.compute("inv-key", () -> {
            calls.incrementAndGet();
            return "v2";
        });

        assertEquals(2, calls.get(),
                "After invalidation, a second upstream call should have been dispatched");
    }

    // ------------------------------------------------------------------
    // AutoCloseable
    // ------------------------------------------------------------------

    @Test
    void closeIsSafeToCallWhenIdle() {
        assertDoesNotThrow(() -> coalescer.close());
    }
}