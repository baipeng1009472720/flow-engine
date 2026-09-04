package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Optional;

/**
 * Port for durable execution checkpoints, isolated from persistence technology and transactions.
 *
 * <p>Implementations must make {@link #save(ExecutionState, long)} atomic for one execution identity.
 * The Core owns the serialized state shape; adapters only store and retrieve it.</p>
 */
public interface ExecutionStateStore {

    /**
     * Loads the latest checkpoint for an execution.
     *
     * @param executionId non-blank stable execution identity
     * @return latest immutable state, or empty when no checkpoint exists
     */
    Optional<ExecutionState> find(String executionId);

    /**
     * Stores a checkpoint only if the current revision matches the caller's expectation.
     *
     * @param state immutable checkpoint to persist
     * @param expectedRevision non-negative current revision expected by the caller
     * @return persisted checkpoint with its next revision
     * @throws IllegalStateException when another writer changed the checkpoint first
     */
    ExecutionState save(ExecutionState state, long expectedRevision);

    /** Validates an execution identity for default implementations. */
    static String requireExecutionId(String executionId) {
        return JsonValues.requireText(executionId, "executionId");
    }
}
