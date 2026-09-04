package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionEvent;
import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.List;

/**
 * Ordered durable observation port for execution events and resumable event cursors.
 *
 * <p>Events are contract values rather than adapter records. Implementations must preserve event
 * order within one execution and make returned lists immutable snapshots.</p>
 */
public interface ExecutionObservationStore {

    /**
     * Appends one reliable lifecycle observation.
     *
     * @param event immutable event to persist
     * @throws IllegalStateException when the event cannot be accepted in its sequence
     */
    void append(ExecutionEvent event);

    /**
     * Returns events strictly after a per-execution sequence cursor.
     *
     * @param executionId non-blank stable execution identity
     * @param afterSequence last observed sequence, or {@code -1} before the first event
     * @return immutable ordered event snapshot
     * @throws IllegalArgumentException when the identity or cursor is invalid
     */
    List<ExecutionEvent> readAfter(String executionId, long afterSequence);

    /** Validates a zero-based cursor while allowing the initial {@code -1} sentinel. */
    static long requireCursor(long afterSequence) {
        if (afterSequence < -1) {
            throw new IllegalArgumentException("afterSequence must be at least -1");
        }
        return afterSequence;
    }

    /** Validates an execution identity for default implementations. */
    static String requireExecutionId(String executionId) {
        return JsonValues.requireText(executionId, "executionId");
    }
}
