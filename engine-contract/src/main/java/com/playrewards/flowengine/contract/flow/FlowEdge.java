package com.playrewards.flowengine.contract.flow;

import com.playrewards.flowengine.contract.json.JsonValues;

/**
 * Directed FlowDefinition 2.0 edge carrying semantic source and target ports.
 *
 * <p>Ports remain strings because the contract must carry both existing semantic values and future
 * host-defined values without reducing them during JSON parsing.</p>
 */
public record FlowEdge(String id, String sourceNodeId, String targetNodeId, String sourcePort,
                       String targetPort) {

    public FlowEdge {
        id = JsonValues.requireText(id, "edge id");
        sourceNodeId = JsonValues.requireText(sourceNodeId, "edge sourceNodeId");
        targetNodeId = JsonValues.requireText(targetNodeId, "edge targetNodeId");
        sourcePort = JsonValues.requireText(sourcePort, "edge sourcePort");
        targetPort = JsonValues.requireText(targetPort, "edge targetPort");
    }
}
