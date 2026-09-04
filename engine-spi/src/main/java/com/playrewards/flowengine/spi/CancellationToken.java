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
}
