package com.playrewards.flowengine.core.component;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.spi.ComponentExecutionContext;
import com.playrewards.flowengine.spi.ComponentInvocation;
import com.playrewards.flowengine.spi.FlowBusinessComponent;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies deterministic startup validation and constant-time business component resolution. */
class FlowBusinessComponentRegistryTest {

    @Test
    void rejects_duplicate_executor_reference_during_construction() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new FlowBusinessComponentRegistry(List.of(component("crm.lookup"), component("crm.lookup"))));

        assertTrue(failure.getMessage().contains("crm.lookup"));
    }

    @Test
    void returns_the_component_registered_for_an_exact_executor_reference() {
        FlowBusinessComponent expected = component("crm.lookup");
        FlowBusinessComponentRegistry registry = new FlowBusinessComponentRegistry(List.of(expected));

        assertSame(expected, registry.required("crm.lookup", "loadCustomer"));
    }

    @Test
    void names_executor_reference_and_node_code_when_component_is_missing() {
        FlowBusinessComponentRegistry registry = new FlowBusinessComponentRegistry(List.of(component("crm.lookup")));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> registry.required("billing.charge", "chargeCustomer"));

        assertTrue(failure.getMessage().contains("billing.charge"));
        assertTrue(failure.getMessage().contains("chargeCustomer"));
    }

    private static FlowBusinessComponent component(String executorRef) {
        return new FlowBusinessComponent() {
            @Override
            public String executorRef() {
                return executorRef;
            }

            @Override
            public ComponentCapabilities capabilities() {
                return new ComponentCapabilities(ComponentEffect.PURE, true, false, true, 1);
            }

            @Override
            public ComponentInvocation execute(ComponentExecutionContext context) {
                throw new UnsupportedOperationException("The registry test never invokes a component");
            }
        };
    }
}
