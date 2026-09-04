package com.playrewards.flowengine.core.protocol;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Documents the intentionally conservative first compatibility subset. */
class JsonSchemaCompatibilityTest {

    private final JsonSchemaCompatibility compatibility = new JsonSchemaCompatibility();

    @Test
    void proves_required_object_property_and_rejects_weaker_or_unknown_source_constraints() {
        Map<String, Object> target = Map.of(
                "type", "object", "required", List.of("id"),
                "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> stronger = Map.of(
                "type", "object", "required", List.of("id", "name"),
                "properties", Map.of("id", Map.of("type", "string"),
                        "name", Map.of("type", "string")));

        assertTrue(compatibility.isAssignable(stronger, target));
        assertFalse(compatibility.isAssignable(Map.of("type", "object"), target));
        assertFalse(compatibility.isAssignable(Map.of("type", "string", "pattern", "[a-z]+"),
                Map.of("type", "string")));
    }

    @Test
    void supports_safe_type_enum_array_and_numeric_narrowing() {
        assertTrue(compatibility.isAssignable(Map.of("type", "integer", "minimum", 10),
                Map.of("type", "number", "minimum", 0)));
        assertTrue(compatibility.isAssignable(Map.of("type", "string", "enum", List.of("A")),
                Map.of("type", "string", "enum", List.of("A", "B"))));
        assertTrue(compatibility.isAssignable(
                Map.of("type", "array", "items", Map.of("type", "string")),
                Map.of("type", "array", "items", Map.of("type", "string"))));
        assertFalse(compatibility.isAssignable(Map.of("type", "number"), Map.of("type", "integer")));
    }
}
