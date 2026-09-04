package com.playrewards.flowengine.core.execution;

/**
 * Package-local observation seam for deterministic verification of execution-result arbitration.
 * It does not expose mutable scheduler state and is not part of the public embedding API.
 */
interface ExecutionLifecycleObserver {

    ExecutionLifecycleObserver NOOP = new ExecutionLifecycleObserver() { };

    /**
     * Observes a submitted controller immediately before it attempts to own the execution.
     */
    default void beforeControllerStart() {
    }

    /**
     * Observes the winning cancellation reason immediately before its result is published.
     *
     * @param reason winning execution reason, or null for a non-cancelling completion
     */
    default void beforeResultPublication(CancellationHandle.Reason reason) {
    }

    /**
     * Observes release of the execution admission after the registration leaves the active set.
     */
    default void afterExecutionAdmissionReleased() {
    }
}
