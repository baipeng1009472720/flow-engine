package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;

/**
 * Loads the complete immutable bundle addressed by one supported flow-reference scheme.
 *
 * <p>The Core selects a loader by {@link #scheme()} before invoking {@link #load(String)}. A loader
 * must return the full static bundle in one operation and must not hand the Core a mutable host model.</p>
 */
public interface FlowBundleLoader {

    /**
     * Returns the non-blank URI-like scheme this loader owns, for example {@code bundle}.
     *
     * @return stable lowercase-or-host-defined scheme identifier
     */
    String scheme();

    /**
     * Resolves a flow reference using this loader's scheme.
     *
     * @param flowReference non-blank reference whose scheme matches {@link #scheme()}
     * @return complete immutable executable bundle
     * @throws IllegalArgumentException when the reference is malformed, unsupported or cannot be resolved
     */
    ExecutableFlowBundle load(String flowReference);
}
