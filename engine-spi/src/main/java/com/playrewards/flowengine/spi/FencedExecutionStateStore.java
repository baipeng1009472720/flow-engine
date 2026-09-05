package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionStatus;

import java.util.*;

/** Strong recovery state. Fence validation and revision CAS must share one database transaction. */
public interface FencedExecutionStateStore extends ExecutionStateStore {
    /**
     * Atomically checks the live fence and expected revision; stale writers fail without partial
     * state.
     */
    ExecutionState save(ExecutionState state, long expectedRevision, ExecutionLease lease);

    /**
     * Returns false for version/from-state conflicts; expired ownership throws instead of
     * authorizing retry.
     */
    boolean transition(
            String id,
            long expectedRevision,
            ExecutionStatus from,
            ExecutionStatus to,
            Map<String, Object> checkpoint,
            ExecutionLease lease);

    /**
     * Bounded tenant-scoped scan of runnable strong states; WAITING_MANUAL is deliberately
     * excluded.
     */
    List<ExecutionState> recoverable(int limit);

    @Override
    default ExecutionState save(ExecutionState state, long revision) {
        throw new IllegalStateException("EXECUTION_LEASE_REQUIRED");
    }
}
