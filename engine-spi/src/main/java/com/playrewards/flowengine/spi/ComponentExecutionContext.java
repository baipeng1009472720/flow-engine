package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import java.util.Objects;

/**
 * Immutable invocation dependencies supplied by the Core to a business component.
 *
 * <p>This context carries data and cooperative services only. It intentionally does not expose the
 * Core scheduler, node executor, database handles, or host framework objects.</p>
 */
public record ComponentExecutionContext(ComponentConfig config, ComponentInputs inputs,
                                        ReadOnlyExecutionContext context, ExecutionLogger logger,
                                        CancellationToken cancellationToken, ExecutionPurpose purpose) {

    public ComponentExecutionContext {
        Objects.requireNonNull(config, "component config must not be null");
        Objects.requireNonNull(inputs, "component inputs must not be null");
        Objects.requireNonNull(context, "read-only execution context must not be null");
        Objects.requireNonNull(logger, "execution logger must not be null");
        Objects.requireNonNull(cancellationToken, "cancellation token must not be null");
        Objects.requireNonNull(purpose, "execution purpose must not be null");
    }
}
