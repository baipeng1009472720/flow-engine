package com.playrewards.flowengine.contract.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Creates immutable copies of the JSON value subset accepted by runtime contracts.
 *
 * <p>The boundary intentionally accepts only JSON-compatible maps, arrays, scalars and {@code null};
 * arbitrary Java objects must be converted by an adapter before they enter a contract. This prevents a
 * mutable host object from changing a bundle after it has been compiled or hashed.</p>
 */
public final class JsonValues {

    private JsonValues() {
    }

    /**
     * Copies an object-shaped JSON value recursively.
     *
     * @param value source object; keys must be non-blank strings
     * @param fieldName field used in validation messages
     * @return immutable object whose nested arrays and objects are also immutable
     */
    public static Map<String, Object> copyObject(Map<String, ?> value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        Map<String, Object> copied = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : value.entrySet()) {
            String key = requireText(entry.getKey(), fieldName + " key");
            copied.put(key, copyValue(entry.getValue(), fieldName + "." + key));
        }
        return Collections.unmodifiableMap(copied);
    }

    /**
     * Copies a JSON value recursively, rejecting host-specific mutable or executable values.
     *
     * @param value source JSON value
     * @param fieldName field used in validation messages
     * @return immutable JSON-compatible value
     */
    public static Object copyValue(Object value, String fieldName) {
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Integer
                || value instanceof Long || value instanceof Short || value instanceof Byte
                || value instanceof Double || value instanceof Float || value instanceof BigDecimal
                || value instanceof BigInteger) {
            return value;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copied = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException(fieldName + " object keys must be strings");
                }
                copied.put(requireText(key, fieldName + " key"), copyValue(entry.getValue(), fieldName + "." + key));
            }
            return Collections.unmodifiableMap(copied);
        }
        if (value instanceof List<?> values) {
            List<Object> copied = new ArrayList<>(values.size());
            for (int index = 0; index < values.size(); index++) {
                copied.add(copyValue(values.get(index), fieldName + "[" + index + "]"));
            }
            return Collections.unmodifiableList(copied);
        }
        throw new IllegalArgumentException(fieldName + " must contain JSON-compatible values");
    }

    /** Validates and normalizes a required textual identifier. */
    public static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        String normalized = value.strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return normalized;
    }
}
