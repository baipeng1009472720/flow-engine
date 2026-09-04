package com.playrewards.flowengine.contract.flow;

import java.util.List;
import java.util.Objects;

/**
 * Immutable standard process definition consumed by the runtime compiler.
 *
 * <p>Only FlowDefinition 2.0 is accepted in this major contract. Historical node types are retained
 * as values for later compiler diagnostics; parsing must never silently remove them.</p>
 */
public record FlowDefinition(String schemaVersion, List<FlowNode> nodes, List<FlowEdge> edges) {

    public static final String CURRENT_SCHEMA_VERSION = "2.0";

    public FlowDefinition {
        Objects.requireNonNull(schemaVersion, "flow schemaVersion must not be null");
        if (!CURRENT_SCHEMA_VERSION.equals(schemaVersion.strip())) {
            throw new IllegalArgumentException("unsupported FlowDefinition schemaVersion: " + schemaVersion);
        }
        schemaVersion = CURRENT_SCHEMA_VERSION;
        nodes = List.copyOf(Objects.requireNonNull(nodes, "flow nodes must not be null"));
        edges = List.copyOf(Objects.requireNonNull(edges, "flow edges must not be null"));
    }
}
