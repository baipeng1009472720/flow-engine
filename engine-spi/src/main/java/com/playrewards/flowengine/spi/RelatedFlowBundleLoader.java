package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;

/**
 * Loads a root bundle and its same-request related flows from one immutable reference.
 *
 * <p>The root reference is passed on every lookup so implementations do not need process-global or
 * thread-local state. Implementations must fail closed when the requested child is not in the same
 * tenant and version snapshot as its parent.</p>
 */
public interface RelatedFlowBundleLoader extends FlowBundleLoader {

    /**
     * Resolves one related flow from the exact reference that loaded the root bundle.
     *
     * @param rootReference immutable root reference selected for the current execution request
     * @param parentIdentity parent identity whose tenant and version constrain the lookup
     * @param targetFlowId exact child flow identity configured by the SUB_FLOW node
     * @return immutable related executable bundle
     * @throws IllegalArgumentException when the reference or target is unavailable
     * @throws SecurityException when the related bundle crosses a trust boundary
     */
    ExecutableFlowBundle loadRelated(
            String rootReference, FlowIdentity parentIdentity, String targetFlowId);
}
