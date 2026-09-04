package com.playrewards.flowengine.contract.bundle;

import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Fully resolved component metadata used by a compiler instead of a host-side component snapshot.
 *
 * <p>{@code executorRef} is absent for built-in nodes and must never contain a Java class name from an
 * untrusted client. Input and output maps are keyed by the stable slot key.</p>
 */
public record CompiledComponentContract(String componentCode, ComponentType componentType,
                                        String executorRef, Map<String, Object> configSchema,
                                        Map<String, ProtocolSchema> inputSlots,
                                        Map<String, ProtocolSchema> outputSlots,
                                        ComponentCapabilities capabilities) {

    public CompiledComponentContract {
        componentCode = JsonValues.requireText(componentCode, "componentCode");
        Objects.requireNonNull(componentType, "componentType must not be null");
        if (executorRef != null) {
            executorRef = JsonValues.requireText(executorRef, "executorRef");
        }
        configSchema = JsonValues.copyObject(configSchema, "configSchema");
        inputSlots = copySchemas(inputSlots, "inputSlots");
        outputSlots = copySchemas(outputSlots, "outputSlots");
        Objects.requireNonNull(capabilities, "capabilities must not be null");
    }

    private static Map<String, ProtocolSchema> copySchemas(Map<String, ProtocolSchema> schemas,
                                                             String fieldName) {
        Objects.requireNonNull(schemas, fieldName + " must not be null");
        Map<String, ProtocolSchema> copied = new LinkedHashMap<>();
        for (Map.Entry<String, ProtocolSchema> entry : schemas.entrySet()) {
            copied.put(JsonValues.requireText(entry.getKey(), fieldName + " key"),
                    Objects.requireNonNull(entry.getValue(), fieldName + " value must not be null"));
        }
        return Map.copyOf(copied);
    }
}
