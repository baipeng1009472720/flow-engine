package com.playrewards.flowengine.spi;

/**
 * Resolves trusted external read-only scopes for a request carrying a context reference.
 *
 * <p>The Core constructs the narrow {@link ContextLoadRequest} only when an execution request has a
 * context reference. Implementations authorize it by tenant identity, but never expose a mutable host
 * object to a component.</p>
 */
public interface ExecutionContextLoader {

    /**
     * Loads immutable external scopes from stable context identity only.
     *
     * @param request non-null context reference and tenant identity
     * @return immutable external scopes
     * @throws IllegalArgumentException when the identity cannot be resolved
     */
    ReadOnlyExecutionContext load(ContextLoadRequest request);
}
