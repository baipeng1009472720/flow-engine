package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import java.time.Clock;
import java.util.Objects;

/** Public embedded entry point for asynchronous EPHEMERAL flow execution. */
public final class FlowEngine implements AutoCloseable {

    private final DagScheduler scheduler;

    /** Creates an engine containing the Task 5 built-in node executors and finite default permits. */
    public FlowEngine() {
        this(NodeExecutorRegistry.builtIns(), ConcurrencyGovernor.defaults(), Clock.systemUTC());
    }

    /**
     * Creates an engine with explicit startup dependencies.
     *
     * @param executors immutable component-type registry
     * @param governor hierarchical concurrency gate
     * @param clock deadline and lifecycle time source
     */
    public FlowEngine(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock) {
        this.scheduler = new DagScheduler(Objects.requireNonNull(executors, "executors must not be null"),
                Objects.requireNonNull(governor, "governor must not be null"),
                Objects.requireNonNull(clock, "clock must not be null"));
    }

    /**
     * Starts one isolated EPHEMERAL execution without blocking the caller.
     *
     * @param plan immutable compiled topology
     * @param request per-run input and execution metadata
     * @return handle used to await or explicitly cancel the run
     */
    public ExecutionHandle execute(ExecutionPlan plan, ExecutionRequest request) {
        return scheduler.schedule(Objects.requireNonNull(plan, "plan must not be null"),
                Objects.requireNonNull(request, "request must not be null"));
    }

    /** Interrupts scheduler work owned by this embedded engine. */
    @Override
    public void close() {
        scheduler.close();
    }
}
