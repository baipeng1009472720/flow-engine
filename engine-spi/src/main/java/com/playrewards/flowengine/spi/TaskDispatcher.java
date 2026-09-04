package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionRequest;

/**
 * Local-first extension point for accepting and cancelling flow execution work.
 *
 * <p>The initial SPI intentionally says nothing about remote workers, queues, HTTP callbacks, or
 * scheduling policy. A host may execute locally now and add a separate adapter later without changing
 * Core-facing request and execution identities.</p>
 */
public interface TaskDispatcher {

    /**
     * Accepts an execution request for local-first dispatch.
     *
     * @param request immutable caller request
     * @return stable execution identity assigned to the accepted task
     * @throws IllegalStateException when the dispatcher cannot accept the task
     */
    DispatchReceipt submit(ExecutionRequest request);

    /**
     * Requests cancellation of an accepted execution.
     *
     * @param executionId non-blank stable execution identity
     * @return {@code true} when cancellation was accepted for an active task
     */
    boolean cancel(String executionId);
}
