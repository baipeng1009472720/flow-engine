package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.component.ComponentCapabilities;

/**
 * Host-provided implementation of a compiled BUSINESS component contract.
 *
 * <p>Components are registered once during startup and addressed by {@link #executorRef()}; they
 * must not depend on Core internals or framework-managed request state. An implementation may execute
 * asynchronously and must honor its declared capabilities.</p>
 */
public interface FlowBusinessComponent {

    /**
     * Returns the non-blank stable reference selected by a compiled component contract.
     *
     * @return startup-unique business component reference
     */
    String executorRef();

    /**
     * Returns scheduling and trial guarantees advertised by this implementation.
     *
     * @return non-null component capability declaration
     */
    ComponentCapabilities capabilities();

    /**
     * Starts one component invocation without exposing Core scheduling internals.
     *
     * @param context immutable config, inputs, read-only scopes and cooperative services
     * @return non-null asynchronous outputs and a non-null content publisher
     * @throws RuntimeException when invocation cannot be started before an asynchronous result exists
     */
    ComponentInvocation execute(ComponentExecutionContext context);
}
