package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;
import java.util.TreeSet;

/**
 * Immutable named JSON outputs completed by a business component.
 *
 * <p>Output keys are stable contract slot names. This type deliberately carries no execution or
 * adapter metadata, so the Core can validate and route it without learning implementation details.</p>
 */
public record ComponentOutputs(Map<String, Object> values) {

    public ComponentOutputs {
        values = JsonValues.copyObject(values, "component outputs");
    }

    /** Returns output slot keys and count without exposing result values. */
    @Override
    public String toString() {
        return "ComponentOutputs[keys=" + new TreeSet<>(values.keySet())
                + ", count=" + values.size() + "]";
    }
}
