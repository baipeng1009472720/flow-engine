package com.playrewards.flowengine.core.node.structure;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import java.util.Map;
import java.util.Set;

/** Activates a compiled PARALLEL fan-out; branch scope assignment remains scheduler-owned. */
public final class ParallelCoordinator implements NodeExecutor {

    @Override
    public ComponentType supports() {
        return ComponentType.PARALLEL;
    }

    /** Activates the single semantic BRANCH port after honoring cancellation and deadline. */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("BRANCH")));
    }
}
