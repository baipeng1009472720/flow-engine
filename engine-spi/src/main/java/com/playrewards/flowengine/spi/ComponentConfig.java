package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable JSON configuration resolved for one business-component invocation.
 *
 * <p>Values are copied recursively at the SPI boundary. Components may inspect them but must not use
 * this object as mutable execution state.</p>
 */
public record ComponentConfig(Map<String, Object> values) {

    public ComponentConfig {
        values = JsonValues.copyObject(values, "component config");
    }

    /**
     * Returns a required configuration value.
     *
     * @param key non-blank configuration key
     * @return JSON-compatible configuration value, which can itself be {@code null}
     * @throws IllegalArgumentException when the key is blank or absent
     */
    public Object requiredJson(String key) {
        String normalized = JsonValues.requireText(key, "component config key");
        if (!values.containsKey(normalized)) {
            throw new IllegalArgumentException("missing required component config: " + normalized);
        }
        return values.get(normalized);
    }

    /**
     * Returns an optional configuration value without inventing a value for an absent key.
     *
     * @param key non-blank configuration key
     * @return present JSON value when non-null, otherwise empty
     * @throws IllegalArgumentException when the key is blank
     */
    public Optional<Object> optionalJson(String key) {
        return Optional.ofNullable(values.get(JsonValues.requireText(key, "component config key")));
    }
}
