package io.coalescex;

import java.time.Duration;

/**
 * Thrown when a caller exceeds the configured timeout waiting for a coalesced result.
 */
public final class CoalescerTimeoutException extends CoalescerException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    private final transient Object   key;
    private final transient Duration timeout;

    public CoalescerTimeoutException(Object key, Duration timeout) {
        super(String.format("Timed out after %s waiting for coalesced result — key: %s", timeout, key), null);
        this.key     = key;
        this.timeout = timeout;
    }

    /** The key that was being resolved when the timeout occurred. */
    public Object key() {
        return key;
    }

    /** The timeout that was exceeded. */
    public Duration timeout() {
        return timeout;
    }
}