package com.playrewards.flowengine.core.node.structure;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Maps a scheduler-completed child END result onto the parent's declared SUB_FLOW outputs. */
public final class SubFlowNodeExecutor implements NodeExecutor {

    @Override
    public ComponentType supports() {
        return ComponentType.SUB_FLOW;
    }

    /** SUB_FLOW scheduling requires a child plan and therefore cannot execute as an isolated leaf. */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        throw new NodeExecutionException("SUB_FLOW_COORDINATOR_REQUIRED",
                "SUB_FLOW must execute through the scoped scheduler coordinator");
    }

    /**
     * Maps one successful child business result without inventing missing multi-output keys.
     * Unknown keys fail closed before the scheduler's normal output validation pass.
     */
    public NodeResult mapSuccessfulResult(NodeExecutionContext context, Object childResult) {
        var declared = context.node().componentContract().outputSlots();
        if (declared.isEmpty()) {
            if (childResult != null) {
                throw new NodeExecutionException("SUB_FLOW_OUTPUT_INVALID",
                        "child returned a value but SUB_FLOW declares no output");
            }
            return NodeResult.continueWith(Map.of(), Set.of("NEXT"));
        }
        if (declared.size() == 1) {
            String key = declared.keySet().iterator().next();
            Map<String, Object> output = new LinkedHashMap<>();
            output.put(key, childResult);
            return NodeResult.continueWith(output, Set.of("NEXT"));
        }
        if (!(childResult instanceof Map<?, ?> values)) {
            throw new NodeExecutionException("SUB_FLOW_OUTPUT_INVALID",
                    "multi-output SUB_FLOW child result must be a JSON object");
        }
        Map<String, Object> output = new LinkedHashMap<>();
        for (Map.Entry<?, ?> value : values.entrySet()) {
            String key = String.valueOf(value.getKey());
            if (!declared.containsKey(key)) {
                throw new NodeExecutionException("SUB_FLOW_OUTPUT_UNKNOWN",
                        "child returned an undeclared SUB_FLOW output: " + key);
            }
            output.put(key, value.getValue());
        }
        return NodeResult.continueWith(output, Set.of("NEXT"));
    }
}
