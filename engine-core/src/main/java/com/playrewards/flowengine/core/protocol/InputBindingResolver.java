package com.playrewards.flowengine.core.protocol;

import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.CompiledNode.InputBinding;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.execution.ExecutionContext;
import com.playrewards.flowengine.core.execution.ScopePath;
import com.playrewards.flowengine.core.node.JsonValueValidator;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.spi.ComponentInputs;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Resolves compiled named input bindings with strict protocol and runtime-value validation. */
public final class InputBindingResolver {

    private final JsonSchemaCompatibility schemaCompatibility = new JsonSchemaCompatibility();

    /**
     * Resolves inputs through a node context, suitable for executor-side use.
     *
     * @param node compiled target node
     * @param context current execution attempt
     * @return immutable required and present optional input maps
     */
    public ComponentInputs resolve(CompiledNode node, NodeExecutionContext context) {
        Objects.requireNonNull(context, "node execution context must not be null");
        if (node != context.node()) {
            throw new IllegalArgumentException("input resolver node must match execution context node");
        }
        return resolve(node, context::requiredSourceOutputContract, context::lookupSource);
    }

    /**
     * Resolves inputs before a node context is handed to an executor.
     *
     * <p>The scheduler uses this overload so every node type shares the same validation rules as
     * BUSINESS. Missing optional bindings remain absent rather than becoming JSON null.</p>
     */
    public ComponentInputs resolve(ExecutionPlan plan, CompiledNode node,
            ExecutionContext executionContext, ScopePath scopePath) {
        Objects.requireNonNull(plan, "execution plan must not be null");
        Objects.requireNonNull(executionContext, "execution context must not be null");
        Objects.requireNonNull(scopePath, "scope path must not be null");
        return resolve(node,
                (sourceNodeId, outputKey) -> sourceOutput(plan, sourceNodeId, outputKey),
                (sourceNodeId, outputKey) -> sourceValue(
                        plan, executionContext, scopePath, sourceNodeId, outputKey));
    }

    private ComponentInputs resolve(CompiledNode node, ContractLookup contracts, ValueLookup values) {
        Objects.requireNonNull(node, "compiled node must not be null");
        Map<String, Object> required = new LinkedHashMap<>();
        Map<String, Object> optional = new LinkedHashMap<>();
        for (Map.Entry<String, InputSlotContract> slot
                : new TreeMap<>(node.componentContract().inputSlots()).entrySet()) {
            InputBinding binding = node.inputBindings().get(slot.getKey());
            if (binding == null) {
                if (slot.getValue().required()) {
                    throw new NodeExecutionException("NODE_REQUIRED_INPUT_MISSING",
                            "required input has no binding: " + slot.getKey());
                }
                continue;
            }
            OutputSlotContract source = contracts.find(binding.sourceNodeId(), binding.sourceOutputKey());
            if (!source.protocol().code().equals(slot.getValue().protocol().code())) {
                throw new NodeExecutionException("NODE_INPUT_PROTOCOL_MISMATCH",
                        "input protocol code does not match: " + slot.getKey());
            }
            if (!schemaCompatibility.isAssignable(
                    source.protocol().schema(), slot.getValue().protocol().schema())) {
                throw new NodeExecutionException("NODE_INPUT_SCHEMA_MISMATCH",
                        "input source schema is not assignable: " + slot.getKey());
            }
            ExecutionContext.ContextValue value = values.find(
                    binding.sourceNodeId(), binding.sourceOutputKey());
            if (!value.present()) {
                if (slot.getValue().required()) {
                    throw new NodeExecutionException("NODE_REQUIRED_INPUT_MISSING",
                            "required input source produced no value: " + slot.getKey());
                }
                continue;
            }
            JsonValueValidator.validate(value.value(), slot.getValue().protocol(),
                    "NODE_INPUT_INVALID", "inputs." + slot.getKey());
            (slot.getValue().required() ? required : optional).put(slot.getKey(), value.value());
        }
        return new ComponentInputs(required, optional);
    }

    private static OutputSlotContract sourceOutput(ExecutionPlan plan, String sourceNodeId, String outputKey) {
        CompiledNode source = plan.nodesById().get(sourceNodeId);
        if (source == null) {
            throw new NodeExecutionException("NODE_SOURCE_NOT_FOUND", "source node not found: " + sourceNodeId);
        }
        OutputSlotContract output = source.componentContract().outputSlots().get(outputKey);
        if (output == null) {
            throw new NodeExecutionException("NODE_SOURCE_OUTPUT_NOT_FOUND",
                    "source output not found: " + sourceNodeId + "." + outputKey);
        }
        return output;
    }

    private static ExecutionContext.ContextValue sourceValue(ExecutionPlan plan,
            ExecutionContext context, ScopePath scopePath, String sourceNodeId, String outputKey) {
        CompiledNode source = plan.nodesById().get(sourceNodeId);
        if (source == null) {
            throw new NodeExecutionException("NODE_SOURCE_NOT_FOUND", "source node not found: " + sourceNodeId);
        }
        return context.lookup(source.node().nodeCode(), scopePath, outputKey);
    }

    @FunctionalInterface
    private interface ContractLookup {
        OutputSlotContract find(String sourceNodeId, String outputKey);
    }

    @FunctionalInterface
    private interface ValueLookup {
        ExecutionContext.ContextValue find(String sourceNodeId, String outputKey);
    }
}
