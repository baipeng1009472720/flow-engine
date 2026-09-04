package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionRequest;

/**
 * Resolves trusted external read-only scopes for a request carrying a context reference.
 *
 * <p>The Core must not call this port when {@link ExecutionRequest#contextReference()} is absent.
 * Implementations may authorize the reference using request tenant data, but never expose a mutable
 * host object to a component.</p>
 */
public interface ExecutionContextLoader {

    /**
     * Loads immutable external scopes for a request with a non-null context reference.
     *
     * @param request execution request containing a context reference
     * @return immutable external scopes
     * @throws IllegalArgumentException when the request has no context reference or it cannot be resolved
     */
    ReadOnlyExecutionContext load(ExecutionRequest request);
}
