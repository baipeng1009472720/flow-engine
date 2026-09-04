package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable external scopes available to a component for the duration of one execution.
 *
 * <p>Loaders supply this data only after resolving a trusted context reference. Components cannot
 * mutate it or use it to persist state.</p>
 */
public record ReadOnlyExecutionContext(Map<String, Map<String, Object>> scopes) {

    public ReadOnlyExecutionContext {
        scopes = copyScopes(scopes);
    }

    /**
     * Returns a named external scope if it was supplied for this invocation.
     *
     * @param scopeCode non-blank stable scope code
     * @return immutable scope values, or empty when the scope is unavailable
     * @throws IllegalArgumentException when the scope code is blank
     */
    public Optional<Map<String, Object>> optionalScope(String scopeCode) {
        return Optional.ofNullable(scopes.get(JsonValues.requireText(scopeCode, "external scope code")));
    }

    /**
     * Returns a named external scope and fails explicitly when the caller did not supply it.
     *
     * @param scopeCode non-blank stable scope code
     * @return immutable scope values
     * @throws IllegalArgumentException when the scope code is blank or unavailable
     */
    public Map<String, Object> requiredScope(String scopeCode) {
        String normalized = JsonValues.requireText(scopeCode, "external scope code");
        return optionalScope(normalized)
                .orElseThrow(() -> new IllegalArgumentException("missing external scope: " + normalized));
    }

    private static Map<String, Map<String, Object>> copyScopes(Map<String, Map<String, Object>> source) {
        if (source == null) {
            throw new NullPointerException("external scopes must not be null");
        }
        Map<String, Map<String, Object>> copied = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : source.entrySet()) {
            String code = JsonValues.requireText(entry.getKey(), "external scope code");
            copied.put(code, JsonValues.copyObject(entry.getValue(), "external scope " + code));
        }
        return Map.copyOf(copied);
    }
}
