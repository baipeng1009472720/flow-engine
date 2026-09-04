package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Immutable named inputs for one component, preserving required and optional slot declarations.
 *
 * <p>The separate maps prevent a missing optional input from being mistaken for a required binding.
 * Nested JSON objects and arrays are defensively copied and cannot be changed by a component.</p>
 */
public record ComponentInputs(Map<String, Object> requiredValues, Map<String, Object> optionalValues) {

    public ComponentInputs {
        requiredValues = JsonValues.copyObject(requiredValues, "required component inputs");
        optionalValues = JsonValues.copyObject(optionalValues, "optional component inputs");
    }

    /**
     * Returns a value from a required input slot.
     *
     * @param key non-blank slot key
     * @return the bound JSON value, which can itself be {@code null}
     * @throws IllegalArgumentException when the key is blank or no required slot was bound
     */
    public Object requiredJson(String key) {
        String normalized = JsonValues.requireText(key, "required component input key");
        if (!requiredValues.containsKey(normalized)) {
            throw new IllegalArgumentException("missing required component input: " + normalized);
        }
        return requiredValues.get(normalized);
    }

    /**
     * Returns an optional input when a non-null JSON value was bound.
     *
     * <p>A missing input is represented as {@link Optional#empty()}; callers must not expect a
     * fabricated {@code null} placeholder.</p>
     *
     * @param key non-blank slot key
     * @return the bound non-null JSON value, or empty when absent or JSON-null
     * @throws IllegalArgumentException when the key is blank
     */
    public Optional<Object> optionalJson(String key) {
        return Optional.ofNullable(optionalValues.get(JsonValues.requireText(key, "optional component input key")));
    }

    /** Returns slot keys and counts without exposing bound values. */
    @Override
    public String toString() {
        return "ComponentInputs[requiredKeys=" + new TreeSet<>(requiredValues.keySet())
                + ", requiredCount=" + requiredValues.size()
                + ", optionalKeys=" + new TreeSet<>(optionalValues.keySet())
                + ", optionalCount=" + optionalValues.size() + "]";
    }
}
