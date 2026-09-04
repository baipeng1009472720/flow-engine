package com.playrewards.flowengine.core.component;

import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.spi.FlowBusinessComponent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Startup-built immutable index of business components by their compiled executor reference.
 *
 * <p>The Core consults this registry only after bundle validation. Construction validates all entries
 * once, so {@link #required(String, String)} is a map lookup and never scans a bean container or list.
 * The registry is thread-safe after construction because its index is immutable.</p>
 */
public final class FlowBusinessComponentRegistry {

    private final Map<String, FlowBusinessComponent> componentsByExecutorRef;

    /**
     * Validates component startup registrations and creates the immutable lookup index.
     *
     * @param components non-null component instances supplied by a host bootstrapper
     * @throws NullPointerException when the list or an entry is null
     * @throws IllegalArgumentException when an executor reference is blank or registered more than once
     */
    public FlowBusinessComponentRegistry(List<? extends FlowBusinessComponent> components) {
        Objects.requireNonNull(components, "business components must not be null");
        Map<String, FlowBusinessComponent> index = new LinkedHashMap<>();
        for (FlowBusinessComponent component : components) {
            FlowBusinessComponent nonNullComponent = Objects.requireNonNull(component,
                    "business component must not be null");
            String executorRef = JsonValues.requireText(nonNullComponent.executorRef(), "business executorRef");
            FlowBusinessComponent duplicate = index.putIfAbsent(executorRef, nonNullComponent);
            if (duplicate != null) {
                throw new IllegalArgumentException("duplicate business executorRef: " + executorRef);
            }
        }
        componentsByExecutorRef = Map.copyOf(index);
    }

    /**
     * Resolves a component by exact compiled reference.
     *
     * @param executorRef non-blank compiled component reference
     * @param nodeCode non-blank node code used only for actionable missing-component diagnostics
     * @return registered component instance
     * @throws IllegalArgumentException when identifiers are blank or no matching component is registered
     */
    public FlowBusinessComponent required(String executorRef, String nodeCode) {
        String normalizedRef = JsonValues.requireText(executorRef, "business executorRef");
        String normalizedNodeCode = JsonValues.requireText(nodeCode, "business nodeCode");
        FlowBusinessComponent component = componentsByExecutorRef.get(normalizedRef);
        if (component == null) {
            throw new IllegalArgumentException("missing business component executorRef=" + normalizedRef
                    + ", nodeCode=" + normalizedNodeCode);
        }
        return component;
    }
}
