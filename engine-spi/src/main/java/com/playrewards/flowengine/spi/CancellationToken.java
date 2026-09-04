package com.playrewards.flowengine.spi;

import java.util.concurrent.CancellationException;

/**
 * Thread-safe view of cancellation requested for one execution.
 *
 * <p>The Core or dispatcher owns cancellation. Components may poll this token at safe interruption
 * points but must perform their own cleanup for already-started side effects.</p>
 */
public interface CancellationToken {

    /**
     * Reports whether cancellation was requested by the runtime.
     *
     * @return {@code true} once cancellation has been requested; implementations must be safe to
     *         call concurrently
     */
    boolean isCancellationRequested();

    /**
     * Throws when the current invocation should stop its normal work.
     *
     * @throws CancellationException when cancellation has been requested
     */
    default void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new CancellationException("flow execution was cancelled");
        }
    }

    /**
     * Registers cleanup which must run promptly when cancellation is requested.
     *
     * <p>Simple polling-only tokens may keep the no-op default. Runtime-owned tokens override this
     * hook so adapters can cancel in-flight I/O without exposing their future or transport type.</p>
     *
     * @param action idempotent transport interruption action
     * @return registration to remove after normal completion
     */
    default CancellationRegistration onCancellation(Runnable action) {
        java.util.Objects.requireNonNull(action, "cancellation action must not be null");
        if (isCancellationRequested()) {
            action.run();
        }
        return () -> { };
    }

    /** Removable cancellation callback registration. */
    @FunctionalInterface
    interface CancellationRegistration extends AutoCloseable {

        /** Removes the callback after normal resource cleanup. */
        @Override
        void close();
    }
}
