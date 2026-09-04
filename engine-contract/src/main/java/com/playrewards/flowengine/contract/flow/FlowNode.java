package com.playrewards.flowengine.contract.flow;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;

/**
 * A node in FlowDefinition 2.0 without management-side component snapshots.
 *
 * <p>Component resolution belongs to the immutable bundle, while {@code config} is a JSON-only node
 * instance value. Its nested values are copied to prevent a caller from mutating compiled input.</p>
 */
public record FlowNode(String id, String componentCode, String nodeCode, String nodeName,
                       Map<String, Object> config) {

    public FlowNode {
        id = JsonValues.requireText(id, "node id");
        componentCode = JsonValues.requireText(componentCode, "node componentCode");
        nodeCode = JsonValues.requireText(nodeCode, "node nodeCode");
        nodeName = JsonValues.requireText(nodeName, "node nodeName");
        config = JsonValues.copyObject(config, "node config");
    }
}
