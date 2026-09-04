package com.playrewards.flowengine.core.node.builtin;

import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.core.config.EngineReservedConfig;
import com.playrewards.flowengine.core.node.JsonValueValidator;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates caller inputs against the START instance contract and exposes them as named outputs. */
public final class StartNodeExecutor implements NodeExecutor {

    @Override
    public ComponentType supports() {
        return ComponentType.START;
    }

    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        Map<String, Object> inputs = context.request().startInputs();
        Map<String, OutputSlotContract> outputs = context.node().componentContract().outputSlots();
        Map<String, Declaration> declarations = declarations(context, outputs);
        for (Map.Entry<String, Declaration> declaration : declarations.entrySet()) {
            if (declaration.getValue().required() && !inputs.containsKey(declaration.getKey())) {
                throw new NodeExecutionException("START_INPUT_REQUIRED",
                        "required START input is missing: " + declaration.getKey());
            }
        }
        for (Map.Entry<String, Object> input : inputs.entrySet()) {
            if (!declarations.containsKey(input.getKey())) {
                throw new NodeExecutionException("START_INPUT_UNKNOWN",
                        "START input is not declared: " + input.getKey());
            }
            OutputSlotContract slot = outputs.get(input.getKey());
            if (slot == null && !outputs.isEmpty()) {
                throw new NodeExecutionException("START_INPUT_UNKNOWN",
                        "START output contract is missing input key: " + input.getKey());
            }
            if (slot != null) {
                JsonValueValidator.validate(input.getValue(), slot.protocol(), "START_INPUT_INVALID",
                        "startInputs." + input.getKey());
            }
        }
        return NodeInvocation.completed(NodeResult.continueWith(inputs, Set.of("NEXT")));
    }

    private static Map<String, Declaration> declarations(NodeExecutionContext context,
            Map<String, OutputSlotContract> outputs) {
        Map<String, Object> config = context.node().node().config();
        Object raw = config.get("inputProtocols");
        if (!config.containsKey("inputProtocols")) {
            Object legacy = legacyInputProtocols(context.node().componentContract().configSchema());
            if (legacy != null) {
                return parseDeclarations(legacy, outputs, "legacy input protocol");
            }
            if (outputs.isEmpty()) {
                return Map.of();
            }
            throw new NodeExecutionException("START_CONFIG_INVALID",
                    "START without inputProtocols requires legacy input protocol declarations");
        }
        return parseDeclarations(raw, outputs, "inputProtocols");
    }

    private static Map<String, Declaration> parseDeclarations(Object raw,
            Map<String, OutputSlotContract> outputs, String field) {
        if (!(raw instanceof List<?> values)) {
            throw new NodeExecutionException("START_CONFIG_INVALID", field + " must be an array");
        }
        Map<String, Declaration> declarations = new LinkedHashMap<>();
        for (Object value : values) {
            if (!(value instanceof Map<?, ?> item)) {
                throw new NodeExecutionException("START_CONFIG_INVALID", "inputProtocols items must be objects");
            }
            String key = text(item.get("key"));
            String name = text(item.get("name"));
            String protocolCode = text(item.get("protocolCode"));
            if (key == null || name == null || protocolCode == null) {
                throw new NodeExecutionException("START_CONFIG_INVALID",
                        "each inputProtocols item requires key, name and protocolCode");
            }
            Object required = item.get("required");
            if (!(required instanceof Boolean requiredValue)) {
                throw new NodeExecutionException("START_CONFIG_INVALID", "input protocol required must be boolean");
            }
            if (!(item.get("schema") instanceof Map<?, ?> rawSchema)) {
                throw new NodeExecutionException("START_CONFIG_INVALID", "input protocol schema must be an object");
            }
            Map<String, Object> schema;
            try {
                schema = JsonValues.copyObject(stringMap(rawSchema), "START input protocol schema");
            } catch (IllegalArgumentException exception) {
                throw new NodeExecutionException("START_CONFIG_INVALID", exception.getMessage());
            }
            if (declarations.putIfAbsent(key, new Declaration(
                    requiredValue, name, protocolCode, schema)) != null) {
                throw new NodeExecutionException("START_CONFIG_INVALID", "duplicate START input key: " + key);
            }
        }
        if (!declarations.keySet().equals(outputs.keySet())) {
            throw new NodeExecutionException("START_CONFIG_INVALID",
                    "START inputProtocols keys must exactly match compiled output slots");
        }
        declarations.forEach((key, declaration) -> {
            OutputSlotContract output = outputs.get(key);
            if (!declaration.name().equals(output.name())
                    || !declaration.protocolCode().equals(output.protocol().code())
                    || !declaration.schema().equals(output.protocol().schema())) {
                throw new NodeExecutionException("START_CONFIG_INVALID",
                        "START input protocol differs from compiled output slot: " + key);
            }
        });
        return declarations;
    }

    private static Object legacyInputProtocols(Map<String, Object> configSchema) {
        Object rawProperties = configSchema.get("properties");
        Map<?, ?> container;
        if (rawProperties == null) {
            container = configSchema;
        } else if (rawProperties instanceof Map<?, ?> properties) {
            container = properties;
        } else {
            throw new NodeExecutionException("START_CONFIG_INVALID",
                    "legacy configSchema.properties must be an object");
        }
        Object rawBindings = container.get(EngineReservedConfig.INPUT_BINDINGS);
        if (rawBindings == null) {
            return null;
        }
        if (!(rawBindings instanceof Map<?, ?> bindings)) {
            throw new NodeExecutionException("START_CONFIG_INVALID",
                    "legacy configSchema inputBindings must be an object");
        }
        if (!bindings.containsKey("x-playrewards-protocols")) {
            throw new NodeExecutionException("START_CONFIG_INVALID",
                    "legacy inputBindings requires x-playrewards-protocols");
        }
        return bindings.get("x-playrewards-protocols");
    }

    private static Map<String, Object> stringMap(Map<?, ?> raw) {
        Map<String, Object> converted = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("START input protocol schema keys must be strings");
            }
            converted.put(key, entry.getValue());
        }
        return converted;
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private record Declaration(boolean required, String name, String protocolCode, Map<String, Object> schema) {
    }
}
