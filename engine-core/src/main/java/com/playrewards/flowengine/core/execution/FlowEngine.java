package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.ExecutionLogger;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.ScriptRuntime;

import java.time.Clock;
import java.util.Objects;

/**
 * Public embedded entry point sharing one scheduler across EPHEMERAL and configured DURABLE
 * execution.
 */
public final class FlowEngine implements AutoCloseable {

    private static final ConcurrencyGovernor PROCESS_GOVERNOR = ConcurrencyGovernor.defaults();

    private final DagScheduler scheduler;
    private com.playrewards.flowengine.core.durable.DurableExecutionCoordinator durable;

    /** Composes the same scheduler with optional technology-neutral durable ownership. */
    public FlowEngine(
            NodeExecutorRegistry executors,
            ConcurrencyGovernor governor,
            Clock clock,
            com.playrewards.flowengine.core.observation.ExecutionRecorder recorder,
            com.playrewards.flowengine.core.durable.DurableExecutionCoordinator durable) {
        this.scheduler = new DagScheduler(executors, governor, clock);
        this.durable = durable;
        this.scheduler.durable(durable, recorder);
    }

    /** Resumes the saved immutable plan using the caller's already authenticated tenant. */
    public ExecutionHandle resume(String id, String tenant) {
        if (durable == null) throw new IllegalStateException("DURABLE_NOT_CONFIGURED");
        return scheduler.resume(durable.resume(id, tenant));
    }

    /** Indicates whether this host configured strongly fenced durable storage. */
    public boolean supportsDurable() {
        return durable != null;
    }

    /** Cancels local executions, including those admitted by automatic durable recovery. */
    public boolean cancel(String executionId) {
        return scheduler.cancel(executionId);
    }

    /**
     * Creates a production runtime with real execution/node observation and bounded event delivery.
     */
    public FlowEngine(
            NodeExecutorRegistry executors,
            ConcurrencyGovernor governor,
            Clock clock,
            com.playrewards.flowengine.core.observation.ExecutionRecorder recorder) {
        this.scheduler = new DagScheduler(executors, governor, clock, recorder);
    }

    /** Creates an engine containing built-ins and process-shared finite concurrency resources. */
    public FlowEngine() {
        this(NodeExecutorRegistry.builtIns());
    }

    /**
     * Creates an engine with custom executors and process-shared finite concurrency resources.
     *
     * @param executors immutable component-type registry
     */
    public FlowEngine(NodeExecutorRegistry executors) {
        this(executors, PROCESS_GOVERNOR, Clock.systemUTC());
    }

    /**
     * Creates an engine with explicit startup dependencies.
     *
     * <p>The supplied governor defines the sharing boundary. Production hosts must share one
     * governor across engine instances; supplying independent governors deliberately isolates
     * concurrency accounting and is intended only for controlled embedding or tests.
     *
     * @param executors immutable component-type registry
     * @param governor hierarchical concurrency gate
     * @param clock deadline and lifecycle time source
     */
    public FlowEngine(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock) {
        this(executors, governor, clock, ExecutionLogger.NOOP);
    }

    /**
     * Creates an engine with explicit startup dependencies and a structured node log sink.
     *
     * @param executors immutable component-type registry
     * @param governor hierarchical concurrency gate
     * @param clock deadline and lifecycle time source
     * @param executionLogger structured sink supplied to every node attempt
     */
    public FlowEngine(
            NodeExecutorRegistry executors,
            ConcurrencyGovernor governor,
            Clock clock,
            ExecutionLogger executionLogger) {
        this.scheduler =
                new DagScheduler(
                        Objects.requireNonNull(executors, "executors must not be null"),
                        Objects.requireNonNull(governor, "governor must not be null"),
                        Objects.requireNonNull(clock, "clock must not be null"),
                        Objects.requireNonNull(
                                executionLogger, "executionLogger must not be null"));
    }

    /**
     * Creates a complete Task 7 embedded runtime from technology-neutral host adapters.
     *
     * @param scripts bounded sandbox runtime
     * @param businessComponents immutable business component registry
     * @param locks distributed lease provider with fencing support
     * @param clock shared execution and lease clock
     */
    public FlowEngine(
            ScriptRuntime scripts,
            FlowBusinessComponentRegistry businessComponents,
            DistributedLockProvider locks,
            Clock clock) {
        this(
                NodeExecutorRegistry.runtimeBuiltIns(scripts, businessComponents, locks, clock),
                PROCESS_GOVERNOR,
                clock);
    }

    /**
     * Creates the complete Task 8 embedded runtime, including a host-owned bounded HTTP adapter.
     *
     * @param scripts bounded sandbox runtime
     * @param businessComponents immutable business component registry
     * @param locks distributed lease provider
     * @param httpClient transport enforcing production/trial target policy
     * @param clock shared deadline clock
     */
    public FlowEngine(
            ScriptRuntime scripts,
            FlowBusinessComponentRegistry businessComponents,
            DistributedLockProvider locks,
            HttpNodeClient httpClient,
            Clock clock) {
        this(
                NodeExecutorRegistry.runtimeBuiltIns(
                        scripts, businessComponents, locks, httpClient, clock),
                PROCESS_GOVERNOR,
                clock);
    }

    /**
     * Starts one isolated execution without blocking the caller; DURABLE first reserves strong
     * ownership.
     *
     * @param plan immutable compiled topology
     * @param request per-run input and execution metadata
     * @return handle used to await or explicitly cancel the run
     */
    public ExecutionHandle execute(ExecutionPlan plan, ExecutionRequest request) {
        return scheduler.schedule(
                Objects.requireNonNull(plan, "plan must not be null"),
                Objects.requireNonNull(request, "request must not be null"));
    }

    /**
     * Cancels active executions and closes only the scheduler work owned by this embedded engine.
     */
    @Override
    public void close() {
        if (durable != null) durable.suspend();
        scheduler.close();
        if (durable != null) durable.close();
    }
}
