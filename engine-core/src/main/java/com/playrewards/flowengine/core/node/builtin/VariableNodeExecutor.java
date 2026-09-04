package com.playrewards.flowengine.core.node.builtin;

import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.execution.ExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.core.protocol.JsonSchemaCompatibility;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolves FIXED VARIABLE sources in configured order and writes its unique declared output slot. */
public final class VariableNodeExecutor implements NodeExecutor {

    private static final JsonSchemaCompatibility SCHEMA_COMPATIBILITY = new JsonSchemaCompatibility();

    @Override
    public ComponentType supports() {
        return ComponentType.VARIABLE;
    }

    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        String mode = text(context.node().node().config().get("valueMode"));
        if ("GROOVY".equals(mode)) {
            throw new NodeExecutionException("NODE_MODE_UNSUPPORTED", "VARIABLE GROOVY is implemented by Task 6");
        }
        if (!"FIXED".equals(mode)) {
            throw new NodeExecutionException("VARIABLE_CONFIG_INVALID",
                    "VARIABLE valueMode must be FIXED or GROOVY");
        }
        if (context.node().componentContract().outputSlots().size() != 1) {
            throw new NodeExecutionException("VARIABLE_OUTPUT_CONTRACT_INVALID",
                    "VARIABLE must declare exactly one output slot");
        }
        String outputKey = context.node().componentContract().outputSlots().keySet().iterator().next();
        OutputSlotContract targetOutput = context.node().componentContract().outputSlots().get(outputKey);
        Object rawSources = context.node().node().config().get("valueSources");
        if (!(rawSources instanceof List<?> sources) || sources.isEmpty()) {
            throw new NodeExecutionException("VARIABLE_CONFIG_INVALID", "FIXED VARIABLE requires valueSources");
        }
        List<Source> validatedSources = new ArrayList<>(sources.size());
        for (Object rawSource : sources) {
            Source source = source(rawSource);
            OutputSlotContract sourceOutput = context.requiredSourceOutputContract(
                    source.nodeId(), source.outputKey());
            if (!SCHEMA_COMPATIBILITY.isAssignable(
                    sourceOutput.protocol().schema(), targetOutput.protocol().schema())) {
                throw new NodeExecutionException("VARIABLE_SOURCE_PROTOCOL_MISMATCH",
                        "VARIABLE source schema is not assignable to its output: "
                                + source.nodeId() + "." + source.outputKey());
            }
            validatedSources.add(source);
        }
        Object selected = null;
        for (Source source : validatedSources) {
            ExecutionContext.ContextValue value = context.lookupSource(source.nodeId(), source.outputKey());
            if (value.present() && !isEmpty(value.value())) {
                selected = value.value();
                break;
            }
        }
        Map<String, Object> outputs = new LinkedHashMap<>();
        outputs.put(outputKey, selected);
        return NodeInvocation.completed(NodeResult.continueWith(outputs, Set.of("NEXT")));
    }

    private static Source source(Object value) {
        if (!(value instanceof Map<?, ?> binding)) {
            throw new NodeExecutionException("VARIABLE_CONFIG_INVALID", "valueSources items must be objects");
        }
        String nodeId = text(binding.get("sourceNodeId"));
        String outputKey = text(binding.get("sourceOutputKey"));
        if (nodeId == null || outputKey == null) {
            throw new NodeExecutionException("VARIABLE_CONFIG_INVALID",
                    "valueSources items require sourceNodeId and sourceOutputKey");
        }
        return new Source(nodeId, outputKey);
    }

    private static boolean isEmpty(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String text) {
            return text.isEmpty();
        }
        if (value instanceof Collection<?> collection) {
            return collection.isEmpty();
        }
        if (value instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        return value.getClass().isArray() && Array.getLength(value) == 0;
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private record Source(String nodeId, String outputKey) {
    }
}
