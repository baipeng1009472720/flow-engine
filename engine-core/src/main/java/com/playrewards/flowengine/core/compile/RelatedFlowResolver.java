package com.playrewards.flowengine.core.compile;

import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;

/** Resolves a same-tenant, same-version sub-flow without embedding host reference rules in Core. */
@FunctionalInterface
public interface RelatedFlowResolver {

    /**
     * Loads the immutable target bundle relative to its parent identity.
     *
     * @param parent parent flow identity and source boundary
     * @param targetFlowId configured target flow id
     * @return exact target bundle snapshot
     */
    ExecutableFlowBundle resolve(FlowIdentity parent, String targetFlowId);
}
