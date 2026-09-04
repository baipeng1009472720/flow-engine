package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.execution.CancellationHandle;
import com.playrewards.flowengine.core.execution.ExecutionContext;
import com.playrewards.flowengine.core.execution.ExecutionDeadlineExceededException;
import com.playrewards.flowengine.core.execution.ScopePath;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.ComponentInputs;
import java.time.Clock;
import java.util.Objects;

/** Immutable per-attempt view supplied to a node executor. */
public final class NodeExecutionContext {

    private final String executionId;
    private final ExecutionPlan plan;
    private final CompiledNode node;
    private final ExecutionContext executionContext;
    private final ScopePath scopePath;
    private final ComponentInputs inputs;
    private final CancellationHandle cancellation;
    private final ExecutionRequest request;
    private final Clock clock;

    /**
     * Creates the complete context after scheduler-side input binding resolution.
     *
     * @param executionId unique run identity
     * @param plan immutable plan containing source contracts
     * @param node compiled node being invoked
     * @param executionContext isolated append-only value store
     * @param scopePath active structured-execution scope
     * @param inputs scheduler-resolved component inputs
     * @param cancellation execution-wide cooperative stop source
     * @param request original dynamic request
     * @param clock deadline time source
     */
    public NodeExecutionContext(String executionId, ExecutionPlan plan, CompiledNode node,
            ExecutionContext executionContext, ScopePath scopePath, ComponentInputs inputs,
            CancellationHandle cancellation, ExecutionRequest request, Clock clock) {
        this.executionId = Objects.requireNonNull(executionId, "executionId must not be null");
        this.plan = Objects.requireNonNull(plan, "plan must not be null");
        this.node = Objects.requireNonNull(node, "node must not be null");
        this.executionContext = Objects.requireNonNull(executionContext, "execution context must not be null");
        this.scopePath = Objects.requireNonNull(scopePath, "scope path must not be null");
        this.inputs = Objects.requireNonNull(inputs, "inputs must not be null");
        this.cancellation = Objects.requireNonNull(cancellation, "cancellation must not be null");
        this.request = Objects.requireNonNull(request, "request must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /** @return execution identity */
    public String executionId() {
        return executionId;
    }

    /** @return compiled node being invoked */
    public CompiledNode node() {
        return node;
    }

    /** @return resolved required and optional declared input slots */
    public ComponentInputs inputs() {
        return inputs;
    }

    /** @return current immutable scope path */
    public ScopePath scopePath() {
        return scopePath;
    }

    /** @return cancellation token safe to pass across the SPI boundary */
    public CancellationToken cancellationToken() {
        return cancellation;
    }

    /** @return original dynamic request for START and execution metadata */
    public ExecutionRequest request() {
        return request;
    }

    /** @return isolated execution value store */
    public ExecutionContext executionContext() {
        return executionContext;
    }

    /**
     * Resolves one source node ID/output slot in the current scope, preserving missing versus null.
     */
    public ExecutionContext.ContextValue lookupSource(String sourceNodeId, String outputKey) {
        CompiledNode source = plan.nodesById().get(sourceNodeId);
        if (source == null) {
            throw new NodeExecutionException("NODE_SOURCE_NOT_FOUND", "source node not found: " + sourceNodeId);
        }
        return executionContext.lookup(source.node().nodeCode(), scopePath, outputKey);
    }

    /**
     * Resolves the compiled output contract for a configured source binding.
     *
     * @throws NodeExecutionException when the source node or output key is absent from the plan
     */
    public OutputSlotContract requiredSourceOutputContract(String sourceNodeId, String outputKey) {
        CompiledNode source = plan.nodesById().get(sourceNodeId);
        if (source == null) {
            throw new NodeExecutionException("NODE_SOURCE_NOT_FOUND", "source node not found: " + sourceNodeId);
        }
        OutputSlotContract output = source.componentContract().outputSlots().get(outputKey);
        if (output == null) {
            throw new NodeExecutionException("NODE_SOURCE_OUTPUT_NOT_FOUND",
                    "source output not found: " + sourceNodeId + "." + outputKey);
        }
        return output;
    }

    /**
     * Stops the current invocation on explicit cancellation or deadline expiry.
     *
     * @throws java.util.concurrent.CancellationException on explicit cancellation
     * @throws ExecutionDeadlineExceededException on deadline expiry
     */
    public void throwIfStopped() {
        if (!clock.instant().isBefore(request.deadline())) {
            cancellation.cancelForDeadline();
            throw new ExecutionDeadlineExceededException("execution deadline exceeded");
        }
        cancellation.throwIfCancellationRequested();
    }
}
