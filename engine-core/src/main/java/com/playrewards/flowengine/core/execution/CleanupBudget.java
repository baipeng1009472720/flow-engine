package com.playrewards.flowengine.core.execution;

import java.time.Duration;
import java.util.Objects;

/**
 * One-shot monotonic budget for bounded cleanup after business execution has stopped.
 *
 * <p>Cleanup must not inherit a far-future business deadline or an injected wall clock that can
 * stand still. Repeated interruption consumes this same budget instead of starting a fresh wait.</p>
 */
public final class CleanupBudget {

    private static final Duration STANDARD_GRACE = Duration.ofSeconds(5);

    private final long startedNanos;
    private final long budgetNanos;

    private CleanupBudget(long startedNanos, long budgetNanos) {
        this.startedNanos = startedNanos;
        this.budgetNanos = budgetNanos;
    }

    /** @return a new independent cleanup budget using the engine's finite standard grace */
    public static CleanupBudget standard() {
        return start(STANDARD_GRACE);
    }

    /**
     * Starts a finite monotonic budget.
     *
     * @param duration positive duration, independent of the execution's wall-clock deadline
     * @return one-shot budget whose remaining time never resets
     */
    public static CleanupBudget start(Duration duration) {
        Objects.requireNonNull(duration, "cleanup duration must not be null");
        long nanos;
        try {
            nanos = duration.toNanos();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("cleanup duration is too large", failure);
        }
        if (nanos < 1) {
            throw new IllegalArgumentException("cleanup duration must be positive");
        }
        return new CleanupBudget(System.nanoTime(), nanos);
    }

    /** @return remaining monotonic nanoseconds, or zero once this budget is exhausted */
    public long remainingNanos() {
        long elapsed = System.nanoTime() - startedNanos;
        if (elapsed <= 0) {
            return budgetNanos;
        }
        return Math.max(0, budgetNanos - elapsed);
    }
}
