package com.playrewards.flowengine.core.retry;

import java.time.Duration;
import java.util.Objects;

/** Immutable finite retry policy with capped exponential backoff. */
public final class RetryPolicy {

    private final int maxRetries;
    private final Duration initialBackoff;
    private final Duration maxBackoff;

    /** Creates a global retry cap and bounded backoff schedule. */
    public RetryPolicy(int maxRetries, Duration initialBackoff, Duration maxBackoff) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        this.maxRetries = maxRetries;
        this.initialBackoff = positive(initialBackoff, "initialBackoff");
        this.maxBackoff = positive(maxBackoff, "maxBackoff");
        if (this.initialBackoff.compareTo(this.maxBackoff) > 0) {
            throw new IllegalArgumentException("initialBackoff must not exceed maxBackoff");
        }
    }

    /** @return configured node retries clipped to the engine-wide cap */
    public int boundedRetries(int configuredRetries) {
        if (configuredRetries < 0) {
            throw new IllegalArgumentException("configured retries must not be negative");
        }
        return Math.min(configuredRetries, maxRetries);
    }

    /** @return finite backoff before the supplied next attempt (attempt numbering starts at two) */
    public Duration backoffBeforeAttempt(int nextAttempt) {
        if (nextAttempt < 2) {
            throw new IllegalArgumentException("retry attempt must be at least two");
        }
        long multiplier = 1L << Math.min(30, nextAttempt - 2);
        long nanos;
        try {
            nanos = Math.multiplyExact(initialBackoff.toNanos(), multiplier);
        } catch (ArithmeticException failure) {
            nanos = Long.MAX_VALUE;
        }
        return Duration.ofNanos(Math.min(nanos, maxBackoff.toNanos()));
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
