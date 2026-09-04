package com.playrewards.flowengine.core.protocol;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Proves source-to-target JSON Schema assignability for a deliberately conservative subset.
 *
 * <p>Unknown keywords, combinators and ambiguous open-object cases return {@code false}; the compiler
 * must reject uncertainty rather than guess that a runtime value will be accepted.</p>
 */
public final class JsonSchemaCompatibility {

    private static final Set<String> SUPPORTED = Set.of(
            "type", "properties", "required", "additionalProperties", "items", "enum", "const",
            "minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems");
    private static final Set<String> OBJECT_CONSTRAINTS = Set.of(
            "properties", "required", "additionalProperties");

    /**
     * Returns whether every value admitted by {@code source} is guaranteed to satisfy {@code target}.
     *
     * @param source upstream output schema
     * @param target downstream input schema
     * @return true only when assignability can be proven by the supported subset
     */
    public boolean isAssignable(Map<String, Object> source, Map<String, Object> target) {
        Objects.requireNonNull(source, "source schema must not be null");
        Objects.requireNonNull(target, "target schema must not be null");
        if (source.equals(target) || target.isEmpty()) {
            return true;
        }
        if (!SUPPORTED.containsAll(source.keySet()) || !SUPPORTED.containsAll(target.keySet())) {
            return false;
        }
        Set<String> sourceTypes = typeSet(source.get("type"));
        Set<String> targetTypes = typeSet(target.get("type"));
        if (!typeAssignable(sourceTypes, targetTypes)) {
            return false;
        }
        if (!enumAssignable(source, target) || !boundsAssignable(source, target)) {
            return false;
        }
        if (mayContain(sourceTypes, "object") && hasAny(target, OBJECT_CONSTRAINTS)
                && !objectAssignable(source, target)) {
            return false;
        }
        if (mayContain(sourceTypes, "array") && target.containsKey("items")) {
            if (!(source.get("items") instanceof Map<?, ?> sourceItems
                    && target.get("items") instanceof Map<?, ?> targetItems
                    && isAssignable(stringMap(sourceItems), stringMap(targetItems)))) {
                return false;
            }
        }
        return true;
    }

    private static boolean mayContain(Set<String> sourceTypes, String type) {
        return sourceTypes == null || sourceTypes.contains(type);
    }

    private static boolean hasAny(Map<String, Object> schema, Set<String> keys) {
        return keys.stream().anyMatch(schema::containsKey);
    }

    private boolean objectAssignable(Map<String, Object> source, Map<String, Object> target) {
        Set<String> sourceRequired = strings(source.get("required"));
        Set<String> targetRequired = strings(target.get("required"));
        if (sourceRequired == null || targetRequired == null || !sourceRequired.containsAll(targetRequired)) {
            return false;
        }
        Map<String, Object> sourceProperties = object(source.get("properties"));
        Map<String, Object> targetProperties = object(target.get("properties"));
        if (sourceProperties == null || targetProperties == null) {
            return false;
        }
        for (Map.Entry<String, Object> targetProperty : targetProperties.entrySet()) {
            Object sourceProperty = sourceProperties.get(targetProperty.getKey());
            if (sourceProperty == null) {
                if (sourceRequired.contains(targetProperty.getKey()) || !Boolean.FALSE.equals(source.get("additionalProperties"))) {
                    return false;
                }
                continue;
            }
            if (!(sourceProperty instanceof Map<?, ?> sourceSchema)
                    || !(targetProperty.getValue() instanceof Map<?, ?> targetSchema)
                    || !isAssignable(stringMap(sourceSchema), stringMap(targetSchema))) {
                return false;
            }
        }
        if (Boolean.FALSE.equals(target.get("additionalProperties"))) {
            if (!Boolean.FALSE.equals(source.get("additionalProperties"))
                    || !targetProperties.keySet().containsAll(sourceProperties.keySet())) {
                return false;
            }
        } else if (target.get("additionalProperties") instanceof Map<?, ?> targetAdditional) {
            Map<String, Object> additionalTargetSchema = stringMap(targetAdditional);
            for (Map.Entry<String, Object> sourceProperty : sourceProperties.entrySet()) {
                if (!targetProperties.containsKey(sourceProperty.getKey())
                        && (!(sourceProperty.getValue() instanceof Map<?, ?> sourcePropertySchema)
                        || !isAssignable(stringMap(sourcePropertySchema), additionalTargetSchema))) {
                    return false;
                }
            }
            Object sourceAdditional = source.get("additionalProperties");
            if (!(Boolean.FALSE.equals(sourceAdditional)
                    || sourceAdditional instanceof Map<?, ?> sourceAdditionalSchema
                    && isAssignable(stringMap(sourceAdditionalSchema), additionalTargetSchema))) {
                return false;
            }
        }
        return true;
    }

    private static boolean typeAssignable(Set<String> sourceTypes, Set<String> targetTypes) {
        if (targetTypes == null) {
            return true;
        }
        if (sourceTypes == null) {
            return false;
        }
        return sourceTypes.stream().allMatch(sourceType -> targetTypes.contains(sourceType)
                || "integer".equals(sourceType) && targetTypes.contains("number"));
    }

    private static Set<String> typeSet(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String text) {
            return Set.of(text);
        }
        return strings(value);
    }

    private static boolean enumAssignable(Map<String, Object> source, Map<String, Object> target) {
        if (target.containsKey("const")) {
            return Objects.equals(source.get("const"), target.get("const"));
        }
        if (target.get("enum") instanceof Collection<?> targetValues) {
            if (source.containsKey("const")) {
                return targetValues.contains(source.get("const"));
            }
            return source.get("enum") instanceof Collection<?> sourceValues
                    && targetValues.containsAll(sourceValues);
        }
        return true;
    }

    private static boolean boundsAssignable(Map<String, Object> source, Map<String, Object> target) {
        return lowerBound(source, target, "minimum")
                && lowerBound(source, target, "minLength")
                && lowerBound(source, target, "minItems")
                && upperBound(source, target, "maximum")
                && upperBound(source, target, "maxLength")
                && upperBound(source, target, "maxItems");
    }

    private static boolean lowerBound(Map<String, Object> source, Map<String, Object> target, String key) {
        if (!(target.get(key) instanceof Number targetValue)) {
            return true;
        }
        return source.get(key) instanceof Number sourceValue
                && decimal(sourceValue).compareTo(decimal(targetValue)) >= 0;
    }

    private static boolean upperBound(Map<String, Object> source, Map<String, Object> target, String key) {
        if (!(target.get(key) instanceof Number targetValue)) {
            return true;
        }
        return source.get(key) instanceof Number sourceValue
                && decimal(sourceValue).compareTo(decimal(targetValue)) <= 0;
    }

    private static BigDecimal decimal(Number value) {
        return new BigDecimal(value.toString());
    }

    private static Set<String> strings(Object value) {
        if (value == null) {
            return Set.of();
        }
        if (!(value instanceof Collection<?> values)) {
            return null;
        }
        Set<String> result = new HashSet<>();
        for (Object item : values) {
            if (!(item instanceof String text)) {
                return null;
            }
            result.add(text);
        }
        return result;
    }

    private static Map<String, Object> object(Object value) {
        return value == null ? Map.of() : value instanceof Map<?, ?> map ? stringMap(map) : null;
    }

    private static Map<String, Object> stringMap(Map<?, ?> source) {
        java.util.LinkedHashMap<String, Object> result = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                return Map.of("<invalid>", entry.getValue());
            }
            result.put(key, entry.getValue());
        }
        return result;
    }
}
