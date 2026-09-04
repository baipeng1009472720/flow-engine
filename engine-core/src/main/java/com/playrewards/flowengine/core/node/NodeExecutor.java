package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.component.ComponentType;

/** Executes one normalized component type without owning graph scheduling or shared state. */
public interface NodeExecutor {

    /** @return the single component type handled by this executor */
    ComponentType supports();

    /**
     * Starts one node invocation in the supplied isolated scope.
     *
     * @param context compiled node, resolved inputs, execution values and stop signal
     * @return non-null invocation whose completion controls downstream readiness
     */
    NodeInvocation execute(NodeExecutionContext context);
}
