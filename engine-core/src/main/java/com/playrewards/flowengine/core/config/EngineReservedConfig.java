package com.playrewards.flowengine.core.config;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Central registry for node config fields owned by the engine rather than business components.
 *
 * <p>Compiler and scheduler may interpret these fields, but the BUSINESS SPI boundary must remove
 * them so component implementations cannot accidentally depend on orchestration metadata.</p>
 */
public final class EngineReservedConfig {

    public static final String INPUT_BINDINGS = "inputBindings";
    public static final String RESOURCE_KEY = "resourceKey";

    private static final Set<String> FIELDS = Set.of(INPUT_BINDINGS, RESOURCE_KEY);

    private EngineReservedConfig() {
    }

    /**
     * Copies only component-owned fields into an immutable JSON object.
     *
     * @param nodeConfig immutable or mutable node configuration
     * @return deeply immutable component-owned configuration
     */
    public static Map<String, Object> withoutEngineFields(Map<String, Object> nodeConfig) {
        Objects.requireNonNull(nodeConfig, "node config must not be null");
        Map<String, Object> componentConfig = new LinkedHashMap<>();
        nodeConfig.forEach((key, value) -> {
            if (!FIELDS.contains(key)) {
                componentConfig.put(key, value);
            }
        });
        return JsonValues.copyObject(componentConfig, "business component config");
    }
}
