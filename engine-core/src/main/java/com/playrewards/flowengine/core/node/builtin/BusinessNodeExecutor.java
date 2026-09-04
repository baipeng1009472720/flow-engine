package com.playrewards.flowengine.core.node.builtin;

import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.config.EngineReservedConfig;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.protocol.InputBindingResolver;
import com.playrewards.flowengine.core.protocol.OutputValidator;
import com.playrewards.flowengine.spi.ComponentConfig;
import com.playrewards.flowengine.spi.ComponentExecutionContext;
import com.playrewards.flowengine.spi.ComponentInvocation;
import com.playrewards.flowengine.spi.FlowBusinessComponent;
import com.playrewards.flowengine.spi.ReadOnlyExecutionContext;
import java.util.Map;
import java.util.Objects;

/**
 * Executes every BUSINESS node through the startup-built component registry.
 *
 * <p>This executor owns the SPI boundary only: it resolves strict inputs, strips engine-reserved
 * configuration, checks runtime capabilities and validates final named outputs. It never scans a
 * framework container and preserves a component's content publisher unchanged.</p>
 */
public final class BusinessNodeExecutor implements NodeExecutor {

    private final FlowBusinessComponentRegistry components;
    private final InputBindingResolver bindingResolver;
    private final OutputValidator outputValidator;

    /**
     * Creates the unique BUSINESS executor from immutable startup dependencies.
     *
     * @param components startup business component index
     * @param bindingResolver shared strict input resolver
     * @param outputValidator shared named output validator
     */
    public BusinessNodeExecutor(FlowBusinessComponentRegistry components,
            InputBindingResolver bindingResolver, OutputValidator outputValidator) {
        this.components = Objects.requireNonNull(components, "business components must not be null");
        this.bindingResolver = Objects.requireNonNull(bindingResolver, "input binding resolver must not be null");
        this.outputValidator = Objects.requireNonNull(outputValidator, "output validator must not be null");
    }

    @Override
    public ComponentType supports() {
        return ComponentType.BUSINESS;
    }

    /**
     * Resolves, invokes and validates one business component while preserving its streaming content.
     *
     * @param context current compiled BUSINESS attempt
     * @return content publisher plus validated final node completion
     */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        var inputs = bindingResolver.resolve(context.node(), context);
        FlowBusinessComponent component;
        try {
            component = components.required(
                    context.node().componentContract().executorRef(), context.node().node().nodeCode());
        } catch (IllegalArgumentException failure) {
            throw new NodeExecutionException("BUSINESS_COMPONENT_NOT_FOUND",
                    "business component is not registered for node " + context.node().node().nodeCode());
        }
        checkCapabilities(context, component.capabilities());
        ComponentInvocation invocation = Objects.requireNonNull(component.execute(new ComponentExecutionContext(
                componentConfig(context.node().node().config()),
                inputs,
                new ReadOnlyExecutionContext(context.externalScopes()),
                context.executionLogger(),
                context.cancellationToken(),
                context.request().purpose())), "business component invocation must not be null");
        return new NodeInvocation(invocation.completion().thenApply(
                result -> outputValidator.validate(context.node(), result)), invocation.content());
    }

    private static ComponentConfig componentConfig(Map<String, Object> nodeConfig) {
        return new ComponentConfig(EngineReservedConfig.withoutEngineFields(nodeConfig));
    }

    private static void checkCapabilities(NodeExecutionContext context, ComponentCapabilities runtime) {
        ComponentCapabilities compiled = context.node().componentContract().capabilities();
        if (!compiled.equals(runtime)) {
            throw new NodeExecutionException("BUSINESS_CAPABILITIES_MISMATCH",
                    "runtime business capabilities differ from the compiled bundle");
        }
        if (context.request().purpose() == ExecutionPurpose.TRIAL && !runtime.supportsTrial()) {
            throw new NodeExecutionException("BUSINESS_TRIAL_UNSUPPORTED",
                    "business component does not support trial execution");
        }
    }
}
