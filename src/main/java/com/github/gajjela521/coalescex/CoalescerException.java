package com.github.gajjela521.coalescex;

/**
 * Unchecked exception thrown when a coalesced upstream loader fails with a checked exception.
 *
 * <p>Unchecked exceptions and {@link Error}s thrown by the loader are propagated directly
 * without wrapping, so callers only see this type when the root cause is a checked exception.
 */
public class CoalescerException extends RuntimeException {

    @java.io.Serial
    private static final long serialVersionUID = 1L;

    public CoalescerException(String message, Throwable cause) {
        super(message, cause);
    }
}