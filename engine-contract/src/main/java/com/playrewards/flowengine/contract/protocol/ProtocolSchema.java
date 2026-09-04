package com.playrewards.flowengine.contract.protocol;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;

/**
 * Versioned JSON schema for a named business protocol or external scope.
 *
 * <p>Schema data remains JSON rather than a Java class so hosts cannot change protocol semantics by
 * classpath choice.</p>
 */
public record ProtocolSchema(String code, String version, Map<String, Object> schema) {

    public ProtocolSchema {
        code = JsonValues.requireText(code, "protocol code");
        version = JsonValues.requireText(version, "protocol version");
        schema = JsonValues.copyObject(schema, "protocol schema");
    }
}
