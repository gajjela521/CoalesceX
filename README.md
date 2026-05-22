# CoalesceX 🚀

An enterprise-grade request-collapsing gateway utility for Java, engineered explicitly to prevent **Thundering Herd distributed I/O storms** in high-throughput Virtual Thread architectures.

## The Problem

When highly concurrent systems experience a cache miss or a spike in traffic for an identical data asset, thousands of threads execute duplicate downstream database queries or HTTP calls. In a modern Java environment using Virtual Threads, this completely floods network connection pools and spikes core CPU usage.

Using standard `synchronized` keywords or reentrant locks to solve this results in **thread pinning**, which freezes underlying OS Carrier Threads and causes severe application latency.

## The Solution

`CoalesceX` utilizes atomic map computations and non-blocking `CompletableFuture` handling to intercept concurrent lookups. Subsequent matching threads safely hook into the context of an existing, in-flight calculation rather than establishing duplicate downstream requests.

## Key Features

- **Non-blocking**: Uses `CompletableFuture` instead of locks to prevent Virtual Thread pinning
- **Thread-safe**: Leverages `ConcurrentHashMap` for safe concurrent access
- **Transparent coalescing**: Multiple threads requesting the same resource transparently share a single upstream request
- **Minimal overhead**: Efficient cleanup ensures no stale references accumulate
- **Virtual Thread optimized**: Designed specifically for high-throughput Virtual Thread applications

## Architecture

```
Multiple Concurrent Requests (Same Key)
           ↓
    RequestCoalescer
           ↓
    inFlightRequests Map
           ↓
  One Upstream Request Executed
           ↓
All Threads Receive Coalesced Result
```

## Usage

```java
RequestCoalescer<String, String> gateway = new RequestCoalescer<>();

String result = gateway.compute("cache_key", () -> {
    // Expensive network/database operation
    return expensiveDataFetch();
});

// If 10,000 threads call compute with the same key,
// only ONE upstream request is executed!
```

## Performance Impact

Based on the included `SimulationHarness`:
- **10,000 concurrent virtual threads** requesting the same resource
- **Traditional approach**: 10,000 database hits
- **CoalesceX approach**: 1 database hit
- **Efficiency**: ~99.99% reduction in redundant requests

## Building

```bash
./gradlew build
```

## Running the Simulation

```bash
./gradlew run
```

This will execute the `SimulationHarness` demonstrating the thundering herd prevention in action.

## Requirements

- Java 21+ (for Virtual Thread support)
- Gradle 7.0+

## Dependencies

- SLF4J API 2.0.9
- SLF4J Simple (logging implementation) 2.0.9
- JUnit 6.0.0 (testing)

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Created for high-throughput distributed systems using Java Virtual Threads.

