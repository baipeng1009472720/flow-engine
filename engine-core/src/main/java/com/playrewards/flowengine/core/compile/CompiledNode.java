package com.playrewards.flowengine.core.compile;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.flow.FlowNode;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;
import java.util.LinkedHashMap;

/** Immutable pairing of a flow node, its resolved component contract and parsed input bindings. */
public record CompiledNode(FlowNode node, CompiledComponentContract componentContract,
                           Map<String, InputBinding> inputBindings) {

    public CompiledNode {
        Objects.requireNonNull(node, "compiled node must not be null");
        Objects.requireNonNull(componentContract, "component contract must not be null");
        inputBindings = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(inputBindings, "input bindings must not be null")));
    }

    /** Stable parsed reference from a target input slot to an upstream output slot. */
    public record InputBinding(String inputKey, String sourceNodeId, String sourceOutputKey) {
    }
}
