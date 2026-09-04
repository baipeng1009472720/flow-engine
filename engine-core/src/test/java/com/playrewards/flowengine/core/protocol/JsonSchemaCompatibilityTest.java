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

    @Test
    void union_types_cannot_bypass_object_constraints_through_null() {
        Map<String, Object> target = Map.of(
                "type", List.of("object", "null"), "required", List.of("id"),
                "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> valid = Map.of(
                "type", List.of("object", "null"), "required", List.of("id"),
                "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> weak = Map.of("type", List.of("object", "null"));

        assertTrue(compatibility.isAssignable(valid, target));
        assertFalse(compatibility.isAssignable(weak, target));
        assertFalse(compatibility.isAssignable(Map.of("type", List.of("object", "string")), target));
    }

    @Test
    void union_types_cannot_bypass_array_item_constraints_through_null() {
        Map<String, Object> target = Map.of(
                "type", List.of("array", "null"), "items", Map.of("type", "string"));

        assertTrue(compatibility.isAssignable(
                Map.of("type", List.of("array", "null"), "items", Map.of("type", "string")), target));
        assertFalse(compatibility.isAssignable(
                Map.of("type", List.of("array", "null"), "items", Map.of("type", "number")), target));
        assertFalse(compatibility.isAssignable(Map.of("type", List.of("array", "number")), target));
    }

    @Test
    void schema_without_explicit_type_can_still_prove_enum_narrowing_without_throwing() {
        assertTrue(compatibility.isAssignable(Map.of("enum", List.of("A")),
                Map.of("enum", List.of("A", "B"))));
    }

    @Test
    void unconstrained_source_cannot_satisfy_untyped_structural_target_constraints() {
        Map<String, Object> objectTarget = Map.of(
                "required", List.of("id"),
                "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> arrayTarget = Map.of("items", Map.of("type", "string"));

        assertFalse(compatibility.isAssignable(Map.of(), objectTarget));
        assertFalse(compatibility.isAssignable(Map.of(), arrayTarget));
    }

    @Test
    void explicit_structural_source_is_checked_against_untyped_target_recursively() {
        Map<String, Object> objectTarget = Map.of(
                "required", List.of("id"),
                "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> arrayTarget = Map.of("items", Map.of("type", "string"));

        assertTrue(compatibility.isAssignable(
                Map.of("type", "object", "required", List.of("id"),
                        "properties", Map.of("id", Map.of("type", "string"))), objectTarget));
        assertFalse(compatibility.isAssignable(Map.of("type", "object"), objectTarget));
        assertTrue(compatibility.isAssignable(
                Map.of("type", "array", "items", Map.of("type", "string")), arrayTarget));
        assertFalse(compatibility.isAssignable(
                Map.of("type", "array", "items", Map.of("type", "number")), arrayTarget));
    }

    @Test
    void exactly_equal_untyped_structural_schemas_remain_provably_compatible() {
        Map<String, Object> objectSchema = Map.of(
                "required", List.of("id"),
                "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> arraySchema = Map.of("items", Map.of("type", "string"));

        assertTrue(compatibility.isAssignable(objectSchema, objectSchema));
        assertTrue(compatibility.isAssignable(arraySchema, arraySchema));
    }
}
