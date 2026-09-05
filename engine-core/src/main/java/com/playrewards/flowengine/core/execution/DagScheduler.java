package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.config.EngineReservedConfig;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.core.node.structure.LockNodeExecutor;
import com.playrewards.flowengine.core.node.structure.LoopCoordinator;
import com.playrewards.flowengine.core.node.structure.SubFlowNodeExecutor;
import com.playrewards.flowengine.core.protocol.InputBindingResolver;
import com.playrewards.flowengine.core.protocol.OutputValidator;
import com.playrewards.flowengine.spi.ComponentInputs;
import com.playrewards.flowengine.spi.ExecutionLogger;
import com.playrewards.flowengine.spi.LockLease;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Virtual-thread DAG scheduler whose readiness state belongs to exactly one execution.
 *
 * <p>Incoming edges are resolved as active or inactive. A node runs only after every incoming edge
 * has been resolved and at least one is active; skipped nodes recursively resolve their outgoing edges
 * as inactive, so an unselected IF branch cannot permanently block a later natural join.</p>
 */
public final class DagScheduler implements AutoCloseable {

    private static final int DEFAULT_MAX_IN_FLIGHT_NODE_TASKS = 1024;
    private static final int DEFAULT_MAX_IN_FLIGHT_EXECUTIONS = 1024;
    private static final int DEFAULT_MAX_IN_FLIGHT_LOCK_WAITS = 1024;
    private static final long ADMISSION_POLL_MILLIS = 25;
    private static final Semaphore PROCESS_NODE_TASK_ADMISSION =
            new Semaphore(DEFAULT_MAX_IN_FLIGHT_NODE_TASKS, true);
    private static final Semaphore PROCESS_EXECUTION_ADMISSION =
            new Semaphore(DEFAULT_MAX_IN_FLIGHT_EXECUTIONS, true);
    private static final Semaphore PROCESS_LOCK_WAIT_ADMISSION =
            new Semaphore(DEFAULT_MAX_IN_FLIGHT_LOCK_WAITS, true);

    private final NodeExecutorRegistry executors;
    private final ConcurrencyGovernor governor;
    private final Clock clock;
    private final Semaphore nodeTaskAdmission;
    private final Semaphore executionAdmission;
    private final Semaphore lockWaitAdmission;
    private final ExecutionLifecycleObserver lifecycleObserver;
    private final InputBindingResolver inputBindingResolver = new InputBindingResolver();
    private final OutputValidator outputValidator = new OutputValidator();
    private final ExecutionLogger executionLogger;
    private com.playrewards.flowengine.core.observation.ExecutionRecorder recorder;

    /** Creates a production scheduler whose observer receives transitions before public completion. */
    public DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            com.playrewards.flowengine.core.observation.ExecutionRecorder recorder) {
        this(executors, governor, clock);
        this.recorder = Objects.requireNonNull(recorder);
    }
    private final ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final Object lifecycleMonitor = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<CancellationHandle, ExecutionRegistration> activeExecutions =
            new ConcurrentHashMap<>();

    /**
     * Creates a scheduler using process-shared finite admissions and immutable registries.
     *
     * @param executors startup executor registry
     * @param governor shared hierarchical concurrency gate
     * @param clock deadline and result timestamp source
     */
    public DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock) {
        this(executors, governor, clock, PROCESS_NODE_TASK_ADMISSION, PROCESS_EXECUTION_ADMISSION,
                PROCESS_LOCK_WAIT_ADMISSION,
                ExecutionLifecycleObserver.NOOP, ExecutionLogger.NOOP);
    }

    /**
     * Creates a scheduler with an explicit node log sink and process-shared finite admissions.
     *
     * @param executors startup executor registry
     * @param governor shared hierarchical concurrency gate
     * @param clock deadline and result timestamp source
     * @param executionLogger structured sink supplied to every node attempt
     */
    public DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            ExecutionLogger executionLogger) {
        this(executors, governor, clock, PROCESS_NODE_TASK_ADMISSION, PROCESS_EXECUTION_ADMISSION,
                PROCESS_LOCK_WAIT_ADMISSION,
                ExecutionLifecycleObserver.NOOP, executionLogger);
    }

    /**
     * Creates a scheduler with an isolated explicit bound on submitted node tasks.
     *
     * <p>The bound is acquired before virtual-thread submission, preventing a wide DAG or a burst
     * of executions from accumulating an unbounded number of parked node tasks. This advanced
     * constructor intentionally does not share admissions with other scheduler instances.</p>
     *
     * @param executors startup executor registry
     * @param governor shared hierarchical concurrency gate
     * @param clock deadline and result timestamp source
     * @param maxInFlightNodeTasks maximum submitted or running node tasks across this scheduler
     */
    public DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            int maxInFlightNodeTasks) {
        this(executors, governor, clock, maxInFlightNodeTasks, DEFAULT_MAX_IN_FLIGHT_EXECUTIONS);
    }

    /**
     * Creates a scheduler with explicit finite execution and node-task admissions.
     *
     * <p>This advanced path intentionally isolates admission accounting from other scheduler
     * instances. The supplied governor separately defines whether component permits are shared.</p>
     *
     * @param executors startup executor registry
     * @param governor shared hierarchical concurrency gate
     * @param clock deadline and result timestamp source
     * @param maxInFlightNodeTasks maximum submitted or running node tasks across this scheduler
     * @param maxInFlightExecutions maximum controller executions before immediate capacity rejection
     */
    public DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            int maxInFlightNodeTasks, int maxInFlightExecutions) {
        this(executors, governor, clock,
                new Semaphore(positive(maxInFlightNodeTasks, "maxInFlightNodeTasks"), true),
                new Semaphore(positive(maxInFlightExecutions, "maxInFlightExecutions"), true),
                new Semaphore(positive(maxInFlightNodeTasks, "maxInFlightNodeTasks"), true),
                ExecutionLifecycleObserver.NOOP, ExecutionLogger.NOOP);
    }

    private DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            Semaphore nodeTaskAdmission, Semaphore executionAdmission, Semaphore lockWaitAdmission,
            ExecutionLifecycleObserver lifecycleObserver, ExecutionLogger executionLogger) {
        this.executors = Objects.requireNonNull(executors, "executors must not be null");
        this.governor = Objects.requireNonNull(governor, "governor must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.nodeTaskAdmission = Objects.requireNonNull(nodeTaskAdmission, "nodeTaskAdmission must not be null");
        this.executionAdmission = Objects.requireNonNull(
                executionAdmission, "executionAdmission must not be null");
        this.lockWaitAdmission = Objects.requireNonNull(
                lockWaitAdmission, "lockWaitAdmission must not be null");
        this.lifecycleObserver = Objects.requireNonNull(lifecycleObserver, "lifecycleObserver must not be null");
        this.executionLogger = Objects.requireNonNull(executionLogger, "executionLogger must not be null");
    }

    /**
     * Creates an isolated scheduler with a package-local lifecycle observation seam.
     *
     * @param executors startup executor registry
     * @param governor hierarchical concurrency gate
     * @param clock deadline and result timestamp source
     * @param maxInFlightNodeTasks isolated submitted/running node-task bound
     * @param maxInFlightExecutions isolated controller bound
     * @param lifecycleObserver read-only pre-publication observer used by deterministic lifecycle tests
     */
    DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            int maxInFlightNodeTasks, int maxInFlightExecutions,
            ExecutionLifecycleObserver lifecycleObserver) {
        this(executors, governor, clock,
                new Semaphore(positive(maxInFlightNodeTasks, "maxInFlightNodeTasks"), true),
                new Semaphore(positive(maxInFlightExecutions, "maxInFlightExecutions"), true),
                new Semaphore(positive(maxInFlightNodeTasks, "maxInFlightNodeTasks"), true),
                lifecycleObserver, ExecutionLogger.NOOP);
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least one");
        }
        return value;
    }

    /**
     * Schedules one plan with an isolated context, cancellation source and readiness graph.
     *
     * @param plan immutable compiled topology
     * @param request per-run input, deadline and tenant metadata
     * @return immediately usable asynchronous execution handle; a closed scheduler returns a completed rejection
     */
    public ExecutionHandle schedule(ExecutionPlan plan, ExecutionRequest request) {
        Objects.requireNonNull(plan, "plan must not be null");
        Objects.requireNonNull(request, "request must not be null");
        Instant scheduledAt = clock.instant();
        String executionId = UUID.randomUUID().toString();
        ExecutionContext context = new ExecutionContext(request.externalScopes());
        CancellationHandle cancellation = new CancellationHandle();
        CompletableFuture<ExecutionResult> completion = new CompletableFuture<>();
        ExecutionHandle handle = new ExecutionHandle(executionId, completion, context, cancellation);
        if (recorder != null) recorder.created(executionId, plan, request, null);
        synchronized (lifecycleMonitor) {
            if (closed.get()) {
                cancellation.cancelForFailure();
                completion.complete(observedResult(result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_SCHEDULER_CLOSED", "execution scheduler is closed", scheduledAt)));
                return handle;
            }
            if (!executionAdmission.tryAcquire()) {
                cancellation.cancelForFailure();
                completion.complete(observedResult(result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_CAPACITY_EXCEEDED", "execution capacity exceeded", scheduledAt)));
                return handle;
            }
            AtomicReference<Thread> controller = new AtomicReference<>();
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes = new ConcurrentHashMap<>();
            StructuralState structuralState = new StructuralState();
            CancellationHandle.CancellationRegistration controllerRegistration = cancellation.onCancellation(() -> {
                runningNodes.forEach(DagScheduler::cancelRunningTask);
                Thread thread = controller.get();
                if (thread != null) {
                    thread.interrupt();
                }
            });
            ExecutionRegistration registration = new ExecutionRegistration(
                    executionId, scheduledAt, cancellation, completion);
            activeExecutions.put(cancellation, registration);
            try {
                Future<?> controllerFuture = virtualThreads.submit(() -> {
                    lifecycleObserver.beforeControllerStart();
                    if (!registration.start()) {
                        return;
                    }
                    controller.set(Thread.currentThread());
                    try {
                        if (recorder != null) recorder.started(executionId);
                        ExecutionResult executionResult =
                                run(executionId, plan, request, context, cancellation, registration,
                                        runningNodes, structuralState);
                        lifecycleObserver.beforeResultPublication(cancellation.reason());
                        registration.publishFromController(executionResult);
                    } catch (Throwable failure) {
                        registration.publishUnexpectedControllerFailure();
                    } finally {
                        controllerRegistration.close();
                        controller.set(null);
                        runningNodes.forEach(DagScheduler::cancelRunningTask);
                        runningNodes.clear();
                        registration.finish();
                    }
                });
                registration.controllerFuture(controllerFuture);
            } catch (RejectedExecutionException failure) {
                controllerRegistration.close();
                registration.selectReason(CancellationHandle.Reason.FAILURE);
                registration.finishBeforeStart();
                registration.publishFromController(result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_SCHEDULING_REJECTED", "execution scheduling was rejected", scheduledAt));
            } catch (RuntimeException failure) {
                controllerRegistration.close();
                registration.finishBeforeStart();
                throw failure;
            }
        }
        return handle;
    }

    private ExecutionResult run(String executionId, ExecutionPlan plan, ExecutionRequest request,
            ExecutionContext context, CancellationHandle cancellation, ExecutionRegistration registration,
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes, StructuralState structuralState) {
        ExecutionResult candidate = null;
        List<LockAddress> cleanupFailures;
        try {
            candidate = runOutcome(executionId, plan, request, context, cancellation, registration,
                    runningNodes, structuralState);
        } finally {
            cleanupFailures = structuralState.cleanupAll();
            logCleanupFailures(executionId, cleanupFailures);
        }
        if (!cleanupFailures.isEmpty() && candidate.status() == ExecutionStatus.SUCCEEDED) {
            return result(executionId, ExecutionStatus.FAILED, null,
                    "LOCK_LEASE_CLEANUP_FAILED", "LOCK lease cleanup failed", candidate.startedAt());
        }
        return candidate;
    }

    private ExecutionResult runOutcome(String executionId, ExecutionPlan plan, ExecutionRequest request,
            ExecutionContext context, CancellationHandle cancellation, ExecutionRegistration registration,
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes, StructuralState structuralState) {
        Instant startedAt = clock.instant();
        try {
            validateExecutionBoundary(plan, request);
            checkStop(request, cancellation);
            NodeResult.TerminalOutcome outcome = executeGraph(
                    executionId, plan, request, context, cancellation, registration,
                    runningNodes, structuralState);
            return result(executionId, outcome.status(), outcome.result(), outcome.errorCode(),
                    outcome.errorMessage(), startedAt, outcome);
        } catch (ExecutionDeadlineExceededException exception) {
            lifecycleObserver.beforeControllerReasonSelection(CancellationHandle.Reason.DEADLINE);
            registration.selectReason(CancellationHandle.Reason.DEADLINE);
            return result(executionId, ExecutionStatus.FAILED, null, "EXECUTION_DEADLINE_EXCEEDED",
                    exception.getMessage(), startedAt);
        } catch (CancellationException exception) {
            if (cancellation.reason() == CancellationHandle.Reason.DEADLINE) {
                return result(executionId, ExecutionStatus.FAILED, null, "EXECUTION_DEADLINE_EXCEEDED",
                        "execution deadline exceeded", startedAt);
            }
            return result(executionId, ExecutionStatus.CANCELLED, null, "EXECUTION_CANCELLED",
                    "execution was cancelled", startedAt);
        } catch (InterruptedException exception) {
            Thread.interrupted();
            if (cancellation.reason() == CancellationHandle.Reason.DEADLINE) {
                return result(executionId, ExecutionStatus.FAILED, null, "EXECUTION_DEADLINE_EXCEEDED",
                        "execution deadline exceeded", startedAt);
            }
            return result(executionId, ExecutionStatus.CANCELLED, null, "EXECUTION_CANCELLED",
                    "execution was cancelled", startedAt);
        } catch (NodeExecutionException exception) {
            lifecycleObserver.beforeControllerReasonSelection(CancellationHandle.Reason.FAILURE);
            registration.selectReason(CancellationHandle.Reason.FAILURE);
            return result(executionId, exception.terminalStatus(), null,
                    exception.code(), exception.getMessage(), startedAt, exception);
        } catch (RuntimeException exception) {
            lifecycleObserver.beforeControllerReasonSelection(CancellationHandle.Reason.FAILURE);
            registration.selectReason(CancellationHandle.Reason.FAILURE);
            return result(executionId, ExecutionStatus.FAILED, null, "NODE_EXECUTION_FAILED",
                    "node execution failed", startedAt, exception);
        }
    }

    private void logCleanupFailures(String executionId, List<LockAddress> failures) {
        failures.forEach(address -> {
            try {
                executionLogger.error("LOCK lease cleanup failed", null, Map.of(
                        "executionId", executionId,
                        "flowId", address.flowId(),
                        "nodeId", address.acquireNodeId(),
                        "scopePath", address.scopePath().value()));
            } catch (RuntimeException ignored) {
                // Observation failure must not replace the already selected business outcome.
            }
        });
    }

    private NodeResult.TerminalOutcome executeGraph(String executionId, ExecutionPlan plan,
            ExecutionRequest request, ExecutionContext context, CancellationHandle cancellation,
            ExecutionRegistration registration,
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes,
            StructuralState structuralState) throws InterruptedException {
        Set<String> starts = plan.nodesById().entrySet().stream()
                .filter(entry -> entry.getValue().componentContract().componentType() == ComponentType.START)
                .map(Map.Entry::getKey).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        RegionOutcome region = executeRegion(executionId, plan, request, context, cancellation,
                runningNodes, structuralState, ScopePath.root(), starts, null, 0, true);
        if (region.terminalOutcome() == null) {
            throw new NodeExecutionException("FLOW_TERMINATED_WITHOUT_END",
                    "active execution graph completed without reaching END");
        }
        if (registration.selectReason(CancellationHandle.Reason.TERMINAL)
                || cancellation.reason() == CancellationHandle.Reason.TERMINAL) {
            return region.terminalOutcome();
        }
        throw new CancellationException("cancellation won the terminal outcome race");
    }

    private RegionOutcome executeRegion(String executionId, ExecutionPlan plan,
            ExecutionRequest request, ExecutionContext context, CancellationHandle cancellation,
            Map<Future<NodeCompletion>, AdmissionLease> allRunningNodes,
            StructuralState structuralState, ScopePath seedScope, Set<String> seeds,
            String boundaryNodeId, int subFlowDepth, boolean terminalAllowed) throws InterruptedException {
        Set<String> included = collectRegion(plan, seeds, boundaryNodeId);
        Map<String, Readiness> readiness = initializeReadiness(plan, included);
        PriorityQueue<ReadyNode> ready = new PriorityQueue<>(java.util.Comparator.comparing(ReadyNode::nodeId));
        for (String seed : new java.util.TreeSet<>(seeds)) {
            Readiness state = readiness.get(seed);
            if (state != null && state.totalIncoming == 0) {
                state.activeScopes.add(seedScope);
                ready.add(new ReadyNode(seed, seedScope));
            }
        }
        CompletionService<NodeCompletion> completions = new ExecutorCompletionService<>(virtualThreads);
        Map<Future<NodeCompletion>, AdmissionLease> regionRunning = new LinkedHashMap<>();
        int running = 0;
        try {
        while (!ready.isEmpty() || running > 0) {
            checkStop(request, cancellation);
            while (!ready.isEmpty() && nodeTaskAdmission.tryAcquire()) {
                ReadyNode next = ready.remove();
                Future<NodeCompletion> future = submitNode(completions, allRunningNodes, readiness,
                        next.nodeId(), next.scopePath(), executionId, plan, request, context,
                        cancellation, structuralState, subFlowDepth);
                regionRunning.put(future, allRunningNodes.get(future));
                running++;
            }
            if (running == 0 && !ready.isEmpty()) {
                acquireNodeAdmission(request, cancellation);
                ReadyNode next = ready.remove();
                Future<NodeCompletion> future = submitNode(completions, allRunningNodes, readiness,
                        next.nodeId(), next.scopePath(), executionId, plan, request, context,
                        cancellation, structuralState, subFlowDepth);
                regionRunning.put(future, allRunningNodes.get(future));
                running++;
            }
            if (running == 0) {
                break;
            }
            long remainingNanos = remainingNanos(request.deadline());
            if (remainingNanos <= 0) {
                throw new ExecutionDeadlineExceededException("execution deadline exceeded");
            }
            long completionWait = ready.isEmpty() ? remainingNanos : Math.min(
                    remainingNanos, TimeUnit.MILLISECONDS.toNanos(ADMISSION_POLL_MILLIS));
            Future<NodeCompletion> completedFuture = completions.poll(completionWait, TimeUnit.NANOSECONDS);
            if (completedFuture == null) {
                if (!ready.isEmpty()) {
                    continue;
                }
                throw new ExecutionDeadlineExceededException("execution deadline exceeded");
            }
            running--;
            allRunningNodes.remove(completedFuture);
            NodeCompletion completed;
            try {
                completed = completedFuture.get();
            } catch (CancellationException exception) {
                throw exception;
            } catch (ExecutionException exception) {
                throw propagateObserved(executionId, exception.getCause());
            }
            if (completed.failure() != null) {
                boolean rootRegion = isRootRegion(seedScope, boundaryNodeId, subFlowDepth);
                if (rootRegion) {
                    CancellationHandle.Reason reason = completed.failure()
                            instanceof ExecutionDeadlineExceededException
                                    ? CancellationHandle.Reason.DEADLINE
                                    : CancellationHandle.Reason.FAILURE;
                    lifecycleObserver.beforeControllerReasonSelection(reason);
                    if (reason == CancellationHandle.Reason.DEADLINE) {
                        cancellation.cancelForDeadline();
                    } else {
                        cancellation.cancelForFailure();
                    }
                    Thread.interrupted();
                } else {
                    cancellation.requestStopWithoutReason();
                }
                boolean sourceCleanup = requiresSourceCleanup(
                        seedScope, boundaryNodeId, subFlowDepth, completed.scopePath());
                if (!cancelAndAwait(regionRunning, CleanupBudget.standard(), sourceCleanup)) {
                    completed.failure().addSuppressed(new IllegalStateException(
                            "structured sibling cleanup did not finish before the deadline"));
                    logStructuredCleanupTimeout(executionId, seedScope);
                }
                throw propagateObserved(executionId, completed.failure());
            }
            regionRunning.remove(completedFuture);
            Readiness state = readiness.get(completed.nodeId());
            state.state = NodeState.COMPLETED;
            CompiledNode node = plan.nodesById().get(completed.nodeId());
            completed.result().outputs().forEach((key, value) ->
                    context.writeOnce(node.node().nodeCode(), completed.scopePath(), key, value));
            if (completed.result().terminalOutcome() != null) {
                boolean rootRegion = isRootRegion(seedScope, boundaryNodeId, subFlowDepth);
                if (rootRegion) {
                    cancellation.cancelForTerminal();
                    Thread.interrupted();
                } else {
                    cancellation.requestStopWithoutReason();
                }
                boolean sourceCleanup = requiresSourceCleanup(
                        seedScope, boundaryNodeId, subFlowDepth, completed.scopePath());
                if (!cancelAndAwait(regionRunning, CleanupBudget.standard(), sourceCleanup)) {
                    logStructuredCleanupTimeout(executionId, seedScope);
                    throw new NodeExecutionException("STRUCTURED_CLEANUP_TIMEOUT",
                            "structured sibling cleanup did not finish before the deadline");
                }
                if (!terminalAllowed) {
                    throw new NodeExecutionException("STRUCTURED_REGION_END_FORBIDDEN",
                            "structured region reached END before its boundary");
                }
                return new RegionOutcome(completed.result().terminalOutcome(), false);
            }
            if (boundaryNodeId != null && boundaryNodeId.equals(completed.nodeId())) {
                if (!cancelAndAwait(regionRunning, CleanupBudget.standard(), true)) {
                    logStructuredCleanupTimeout(executionId, seedScope);
                    throw new NodeExecutionException("STRUCTURED_CLEANUP_TIMEOUT",
                            "structured sibling cleanup did not finish before the deadline");
                }
                return new RegionOutcome(null, true);
            }
            resolveOutgoing(executionId, plan, included, readiness, completed, ready, context);
        }
        return new RegionOutcome(null, false);
        } catch (ExecutionDeadlineExceededException failure) {
            if (cancellation.hasUnresolvedSideEffect()) {
                if (isRootRegion(seedScope, boundaryNodeId, subFlowDepth)) {
                    lifecycleObserver.beforeControllerReasonSelection(CancellationHandle.Reason.FAILURE);
                }
                cancellation.cancelForFailure();
                Thread.interrupted();
                if (!cancelAndAwait(regionRunning, CleanupBudget.standard(), true)) {
                    logStructuredCleanupTimeout(executionId, seedScope);
                }
                if (cancellation.hasUnresolvedSideEffect()) {
                    throw uncertainHttpSideEffect();
                }
                throw failure;
            }
            if (isRootRegion(seedScope, boundaryNodeId, subFlowDepth)) {
                lifecycleObserver.beforeControllerReasonSelection(CancellationHandle.Reason.DEADLINE);
            }
            cancellation.cancelForDeadline();
            Thread.interrupted();
            if (!cancelAndAwait(regionRunning, CleanupBudget.standard(), true)) {
                logStructuredCleanupTimeout(executionId, seedScope);
            }
            throw failure;
        } catch (CancellationException failure) {
            boolean possiblySent = cancellation.hasUnresolvedSideEffect();
            cancellation.requestStopWithoutReason();
            Thread.interrupted();
            if (!cancelAndAwait(regionRunning, CleanupBudget.standard(),
                    possiblySent || cancellation.reason() == CancellationHandle.Reason.DEADLINE
                            || requiresRegionSourceCleanup(seedScope, boundaryNodeId, subFlowDepth))) {
                logStructuredCleanupTimeout(executionId, seedScope);
            }
            if (cancellation.hasUnresolvedSideEffect()) {
                Thread.interrupted();
                throw uncertainHttpSideEffect();
            }
            throw failure;
        } catch (InterruptedException failure) {
            boolean possiblySent = cancellation.hasUnresolvedSideEffect();
            Thread.interrupted();
            cancellation.requestStopWithoutReason();
            if (!cancelAndAwait(regionRunning, CleanupBudget.standard(),
                    possiblySent || cancellation.reason() == CancellationHandle.Reason.DEADLINE
                            || requiresRegionSourceCleanup(seedScope, boundaryNodeId, subFlowDepth))) {
                logStructuredCleanupTimeout(executionId, seedScope);
            }
            if (cancellation.hasUnresolvedSideEffect()) {
                Thread.interrupted();
                throw uncertainHttpSideEffect();
            }
            throw failure;
        }
    }

    private static NodeExecutionException uncertainHttpSideEffect() {
        return new NodeExecutionException("HTTP_SIDE_EFFECT_UNCERTAIN",
                "HTTP side effect may have been sent; manual confirmation is required",
                ExecutionStatus.WAITING_MANUAL);
    }

    private static boolean isRootRegion(ScopePath scopePath, String boundaryNodeId, int subFlowDepth) {
        return ScopePath.root().equals(scopePath) && boundaryNodeId == null && subFlowDepth == 0;
    }

    private static boolean requiresSourceCleanup(ScopePath seedScope, String boundaryNodeId,
            int subFlowDepth, ScopePath completedScope) {
        return boundaryNodeId != null || subFlowDepth > 0 || !completedScope.equals(seedScope);
    }

    private static boolean requiresRegionSourceCleanup(
            ScopePath seedScope, String boundaryNodeId, int subFlowDepth) {
        return boundaryNodeId != null || subFlowDepth > 0 || !ScopePath.root().equals(seedScope);
    }

    private Future<NodeCompletion> submitNode(CompletionService<NodeCompletion> completions,
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes,
            Map<String, Readiness> readiness, String nodeId, ScopePath scopePath,
            String executionId, ExecutionPlan plan, ExecutionRequest request, ExecutionContext context,
            CancellationHandle cancellation, StructuralState structuralState, int subFlowDepth) {
        Readiness state = readiness.get(nodeId);
        if (state.state != NodeState.PENDING) {
            nodeTaskAdmission.release();
            throw new IllegalStateException("ready queue contained a non-pending node: " + nodeId);
        }
        state.state = NodeState.RUNNING;
        com.playrewards.flowengine.core.observation.ExecutionRecorder.Attempt observation;
        try {
            observation = recorder == null ? null
                    : recorder.queued(executionId, plan.nodesById().get(nodeId), scopePath);
        } catch (RuntimeException failure) {
            // Observation capacity is reserved after node admission, so rejection must return that permit.
            state.state = NodeState.PENDING;
            nodeTaskAdmission.release();
            throw failure;
        }
        AdmissionLease admissionLease = new AdmissionLease(
                nodeTaskAdmission, !ScopePath.root().equals(scopePath));
        admissionLease.beforeStartCancellation = () -> {
            if (observation != null) observation.failed(new CancellationException("cancelled before start"));
        };
        try {
            Future<NodeCompletion> future = completions.submit(() -> {
                admissionLease.started();
                try {
                    return invokeNode(executionId, plan, nodeId, scopePath, request, context,
                            cancellation, admissionLease, runningNodes, structuralState, subFlowDepth, observation);
                } finally {
                    admissionLease.releaseAfterRun();
                }
            });
            runningNodes.put(future, admissionLease);
            return future;
        } catch (RuntimeException failure) {
            state.state = NodeState.PENDING;
            admissionLease.releaseBeforeStart();
            throw failure;
        }
    }

    private static void cancelRunningTask(Future<NodeCompletion> task, AdmissionLease admissionLease) {
        task.cancel(true);
        admissionLease.releaseBeforeStart();
    }

    private void acquireNodeAdmission(ExecutionRequest request, CancellationHandle cancellation)
            throws InterruptedException {
        while (true) {
            checkStop(request, cancellation);
            long remainingNanos = remainingNanos(request.deadline());
            if (remainingNanos <= 0) {
                throw new ExecutionDeadlineExceededException("execution deadline exceeded");
            }
            long waitNanos = Math.min(remainingNanos,
                    TimeUnit.MILLISECONDS.toNanos(ADMISSION_POLL_MILLIS));
            if (nodeTaskAdmission.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
                return;
            }
        }
    }

    private NodeCompletion invokeNode(String executionId, ExecutionPlan plan, String nodeId,
            ScopePath scopePath, ExecutionRequest request, ExecutionContext context,
            CancellationHandle cancellation, AdmissionLease admissionLease,
            Map<Future<NodeCompletion>, AdmissionLease> allRunningNodes,
            StructuralState structuralState, int subFlowDepth,
            com.playrewards.flowengine.core.observation.ExecutionRecorder.Attempt observation) {
        ConcurrencyGovernor.Permit permit = null;
        boolean lockWaitPermit = false;
        try {
            checkStop(request, cancellation);
            CompiledNode node = plan.nodesById().get(nodeId);
            ComponentInputs inputs = inputBindingResolver.resolve(plan, node, context, scopePath);
            if (observation != null) observation.inputs(Map.of("required", inputs.requiredValues(), "optional", inputs.optionalValues()));
            NodeExecutionContext nodeContext = new NodeExecutionContext(executionId, plan, node, context,
                    scopePath, inputs, cancellation, request, clock, observation == null ? executionLogger : observation);
            String resourceKey = resourceKey(node);
            ConcurrencyGovernor.PermitRequest permitRequest = new ConcurrencyGovernor.PermitRequest(
                    request.tenantId(), plan.flowIdentity().flowId(), node.node().componentCode(), resourceKey,
                    node.componentContract().capabilities().maxConcurrency());
            permit = governor.acquire(permitRequest, cancellation, request.deadline());
            nodeContext.throwIfStopped();
            if (observation != null) observation.started();
            NodeExecutor executor = executors.required(node.componentContract().componentType(),
                    node.node().nodeCode());
            boolean lockAcquisition = node.componentContract().componentType() == ComponentType.LOCK
                    && "ACQUIRE".equals(text(node.node().config().get("role")));
            if (lockAcquisition) {
                if (!lockWaitAdmission.tryAcquire()) {
                    throw new NodeExecutionException("LOCK_WAIT_CAPACITY_EXCEEDED",
                            "LOCK wait capacity exceeded");
                }
                lockWaitPermit = true;
            }
            if (node.componentContract().componentType() == ComponentType.LOOP
                    || node.componentContract().componentType() == ComponentType.SUB_FLOW
                    || lockAcquisition) {
                permit.close();
                permit = null;
                admissionLease.releaseForStructuredWait();
            }
            NodeResult structured = switch (node.componentContract().componentType()) {
                case LOOP -> executeLoop(executionId, plan, nodeId, nodeContext, request, context,
                        cancellation, allRunningNodes, structuralState, subFlowDepth, executor);
                case SUB_FLOW -> executeSubFlow(executionId, plan, nodeId, nodeContext, request,
                        cancellation, allRunningNodes, structuralState, subFlowDepth, executor);
                case LOCK -> executeLock(plan, nodeId, nodeContext, structuralState, executor);
                default -> null;
            };
            if (structured != null) {
                outputValidator.validateResult(node, structured);
                if (observation != null) observation.succeeded(structured);
                return NodeCompletion.success(nodeId, scopePath, structured);
            }
            NodeInvocation invocation = Objects.requireNonNull(executor.execute(nodeContext),
                    "node invocation must not be null");
            var content = new com.playrewards.flowengine.core.node.ContentDrain(chunk -> {
                if (observation != null) observation.content(chunk);
            });
            StageAwaiter awaiter = bridge(invocation.completion(), permit, admissionLease);
            permit = null; // The source lifecycle now owns both concurrency leases.
            try (content; CancellationHandle.CancellationRegistration contentCancellation =
                    cancellation.onCancellation(content::close);
                    CancellationHandle.CancellationRegistration ignoredRegistration =
                    cancellation.onCancellation(awaiter::cancel)) {
                invocation.contentStream().subscribe(content);
                NodeResult result = awaitNode(awaiter, request, cancellation);
                content.await(nodeContext);
                outputValidator.validateResult(node, result);
                if (observation != null) observation.succeeded(result);
                return NodeCompletion.success(nodeId, scopePath, result);
            } finally {
                if (!awaiter.completion().isDone()) awaiter.cancel();
            }
        } catch (Throwable failure) {
            if (observation != null) observation.failed(failure);
            return NodeCompletion.failure(nodeId, scopePath, failure);
        } finally {
            if (permit != null) {
                permit.close();
            }
            if (lockWaitPermit) {
                lockWaitAdmission.release();
            }
        }
    }

    private NodeResult executeLoop(String executionId, ExecutionPlan plan, String nodeId,
            NodeExecutionContext nodeContext, ExecutionRequest request, ExecutionContext context,
            CancellationHandle cancellation, Map<Future<NodeCompletion>, AdmissionLease> allRunningNodes,
            StructuralState structuralState, int subFlowDepth, NodeExecutor executor)
            throws Exception {
        if (!(executor instanceof LoopCoordinator loop)) {
            throw new NodeExecutionException("LOOP_EXECUTOR_INVALID", "LOOP executor is not a coordinator");
        }
        String role = text(nodeContext.node().node().config().get("role"));
        if ("END".equals(role)) {
            return NodeResult.continueWith(Map.of(), Set.of("NEXT"));
        }
        if (!"START".equals(role)) {
            throw new NodeExecutionException("LOOP_CONFIG_INVALID", "LOOP role must be START or END");
        }
        LoopCoordinator.CollectionEvaluation collectionEvaluation = loop.collection(nodeContext);
        List<Object> collection = awaitValue(collectionEvaluation.completion(),
                collectionEvaluation.sourceCompletion(), request, cancellation,
                "LOOP_COLLECTION_FAILED", nodeContext);
        String endId = plan.loopPairs().get(nodeId);
        List<String> bodyTargets = plan.outgoingByPort().getOrDefault(nodeId, Map.of())
                .getOrDefault("BODY", List.of());
        if (endId == null || bodyTargets.isEmpty()) {
            throw new NodeExecutionException("LOOP_PLAN_INVALID", "LOOP plan is missing its body or pair");
        }
        String itemAlias = loop.itemAlias(nodeContext);
        String indexAlias = loop.indexAlias(nodeContext);
        LoopCoordinator.Source resultBinding = loop.resultBinding(nodeContext);
        Set<String> bodyRegion = collectRegion(plan, new LinkedHashSet<>(bodyTargets), endId);
        if (resultBinding != null && (!bodyRegion.contains(resultBinding.nodeId())
                || endId.equals(resultBinding.nodeId()))) {
            throw new NodeExecutionException("LOOP_RESULT_SOURCE_INVALID",
                    "LOOP result source must be inside the paired body");
        }
        List<Object> results = new ArrayList<>(collection.size());
        for (int index = 0; index < collection.size(); index++) {
            nodeContext.throwIfStopped();
            ScopePath iterationScope = nodeContext.scopePath()
                    .child("loop:" + nodeContext.node().node().nodeCode())
                    .child("index:" + index);
            context.writeOnce(nodeContext.node().node().nodeCode(), iterationScope, itemAlias,
                    collection.get(index));
            context.writeOnce(nodeContext.node().node().nodeCode(), iterationScope, indexAlias, index);
            CancellationHandle iterationCancellation = cancellation.linkedChild();
            RegionOutcome iteration;
            try {
                iteration = executeRegion(executionId, plan, request, context, iterationCancellation,
                        allRunningNodes, structuralState, iterationScope,
                        new LinkedHashSet<>(bodyTargets), endId, subFlowDepth, false);
            } finally {
                iterationCancellation.detachFromParent();
            }
            if (!iteration.boundaryReached()) {
                throw new NodeExecutionException("LOOP_ITERATION_INCOMPLETE",
                        "LOOP iteration did not reach its paired LOOP_END");
            }
            if (resultBinding == null) {
                results.add(null);
            } else {
                CompiledNode source = plan.nodesById().get(resultBinding.nodeId());
                if (source == null) {
                    throw new NodeExecutionException("LOOP_RESULT_SOURCE_NOT_FOUND",
                            "LOOP result source is absent from the plan");
                }
                ExecutionContext.ContextValue value = context.lookupVisible(
                        source.node().nodeCode(), iterationScope, resultBinding.outputKey());
                if (!value.present()) {
                    throw new NodeExecutionException("LOOP_RESULT_MISSING",
                            "LOOP result source did not produce a value");
                }
                results.add(value.value());
            }
        }
        return loop.completed(java.util.Collections.unmodifiableList(results));
    }

    private NodeResult executeSubFlow(String executionId, ExecutionPlan plan, String nodeId,
            NodeExecutionContext nodeContext, ExecutionRequest request,
            CancellationHandle cancellation, Map<Future<NodeCompletion>, AdmissionLease> allRunningNodes,
            StructuralState structuralState, int subFlowDepth, NodeExecutor executor)
            throws Exception {
        if (!(executor instanceof SubFlowNodeExecutor subFlow)) {
            throw new NodeExecutionException("SUB_FLOW_EXECUTOR_INVALID",
                    "SUB_FLOW executor is not a coordinator");
        }
        if (subFlowDepth >= 32) {
            throw new NodeExecutionException("SUB_FLOW_DEPTH_EXCEEDED",
                    "SUB_FLOW nesting exceeds the maximum depth");
        }
        ExecutionPlan childPlan = plan.subFlowPlans().get(nodeId);
        if (childPlan == null) {
            throw new NodeExecutionException("SUB_FLOW_PLAN_MISSING",
                    "compiled SUB_FLOW plan is missing");
        }
        String childId = UUID.randomUUID().toString();
        ScopePath childScope = nodeContext.scopePath()
                .child("subflow:" + nodeContext.node().node().nodeCode())
                .child("execution:" + childId);
        ExecutionContext childContext = new ExecutionContext(nodeContext.externalScopes());
        nodeContext.executionContext().registerChildExecution(childId, childScope, childContext);
        Map<String, Object> childInputs = new LinkedHashMap<>(nodeContext.inputs().requiredValues());
        childInputs.putAll(nodeContext.inputs().optionalValues());
        ExecutionRequest childRequest = new ExecutionRequest(request.flowReference(), request.mode(),
                request.purpose(), childInputs, request.externalScopes(), request.contextReference(),
                request.idempotencyKey(), request.tenantId(), request.deadline(), request.metadata());
        Set<String> childStarts = childPlan.nodesById().entrySet().stream()
                .filter(entry -> entry.getValue().componentContract().componentType() == ComponentType.START)
                .map(Map.Entry::getKey).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        CancellationHandle childCancellation = cancellation.linkedChild();
        RegionOutcome child;
        ExecutionResult observedChild = null;
        Instant childStartedAt = clock.instant();
        boolean childObservationCreated = false;
        try {
            if (recorder != null) {
                recorder.created(childId, childPlan, childRequest, executionId);
                childObservationCreated = true;
                recorder.started(childId);
            }
            child = executeRegion(childId, childPlan, childRequest, childContext,
                    childCancellation, allRunningNodes, structuralState, childScope, childStarts,
                    null, subFlowDepth + 1, true);
            if (recorder != null) {
                var terminal = child.terminalOutcome();
                var childResult = result(childId, terminal == null ? ExecutionStatus.FAILED : terminal.status(),
                        terminal == null ? null : terminal.result(), terminal == null ? "SUB_FLOW_TERMINATED_WITHOUT_END" : terminal.errorCode(),
                        terminal != null && terminal.status() == ExecutionStatus.SUCCEEDED ? null : "sub-flow failed", childStartedAt, terminal);
                // finished 自身负责 finally 清理，调用后不得在 catch 中再次使用子活跃身份。
                childObservationCreated = false;
                observedChild = recorder.finished(childResult);
            }
        } catch (Exception failure) {
            if (childObservationCreated) recorder.finished(result(childId, ExecutionStatus.FAILED, null,
                    failure instanceof NodeExecutionException n ? n.code() : "INTERNAL", "sub-flow failed", childStartedAt, failure));
            throw failure;
        } finally {
            childCancellation.detachFromParent();
        }
        NodeResult.TerminalOutcome outcome = child.terminalOutcome();
        // 子终态已经提交并移除活跃身份；在子执行异常处理区之外传播审计降级结果。
        if (outcome != null && observedChild != null && observedChild.status() != outcome.status()) {
            var auditFailure = new NodeExecutionException(observedChild.errorCode(), "child audit failed");
            recorder.inheritCause(executionId, observedChild, auditFailure);
            throw auditFailure;
        }
        if (outcome == null) {
            throw new NodeExecutionException("SUB_FLOW_TERMINATED_WITHOUT_END",
                    "child flow completed without reaching END");
        }
        if (outcome.status() != ExecutionStatus.SUCCEEDED) {
            var failure = new NodeExecutionException(outcome.errorCode(),
                    outcome.errorMessage() == null ? "child flow failed" : outcome.errorMessage());
            if (recorder != null) recorder.inheritCause(executionId, outcome, failure);
            throw failure;
        }
        return subFlow.mapSuccessfulResult(nodeContext, outcome.result());
    }

    private NodeResult executeLock(ExecutionPlan plan, String nodeId,
            NodeExecutionContext nodeContext, StructuralState structuralState, NodeExecutor executor) {
        if (!(executor instanceof LockNodeExecutor lock)) {
            throw new NodeExecutionException("LOCK_EXECUTOR_INVALID", "LOCK executor is not a coordinator");
        }
        String role = text(nodeContext.node().node().config().get("role"));
        if ("ACQUIRE".equals(role)) {
            LockAddress address = new LockAddress(plan.flowIdentity().flowId(), nodeId, nodeContext.scopePath());
            LockLease lease = lock.acquire(nodeContext);
            structuralState.register(address, lock, lease);
            return lock.continued();
        }
        if ("RELEASE".equals(role)) {
            String acquireId = plan.lockPairs().get(nodeId);
            if (acquireId == null) {
                throw new NodeExecutionException("LOCK_PLAN_INVALID", "LOCK release has no paired acquire");
            }
            structuralState.release(new LockAddress(
                    plan.flowIdentity().flowId(), acquireId, nodeContext.scopePath()));
            return lock.continued();
        }
        throw new NodeExecutionException("LOCK_CONFIG_INVALID", "LOCK role must be ACQUIRE or RELEASE");
    }

    private <T> T awaitValue(CompletionStage<T> stage, CompletionStage<?> sourceStage,
            ExecutionRequest request, CancellationHandle cancellation, String failureCode,
            NodeExecutionContext nodeContext) throws Exception {
        Objects.requireNonNull(stage, "structured completion must not be null");
        Objects.requireNonNull(sourceStage, "structured source completion must not be null");
        CompletableFuture<T> future = new CompletableFuture<>();
        java.util.concurrent.CountDownLatch sourceDone = new java.util.concurrent.CountDownLatch(1);
        sourceStage.whenComplete((ignored, failure) -> sourceDone.countDown());
        stage.whenComplete((value, failure) -> {
            if (failure == null) {
                future.complete(value);
            } else {
                future.completeExceptionally(failure);
            }
        });
        Future<?> cancellableStage = stage instanceof Future<?> source ? source : null;
        Future<?> cancellableSource = sourceStage instanceof Future<?> source ? source : null;
        try (CancellationHandle.CancellationRegistration ignored = cancellation.onCancellation(
                () -> {
                    if (cancellableSource != null) {
                        cancellableSource.cancel(true);
                    }
                    if (cancellableStage != null && cancellableStage != cancellableSource) {
                        cancellableStage.cancel(true);
                    }
                    future.cancel(true);
                })) {
            try {
                return future.get(Math.max(1, TimeUnit.NANOSECONDS.toMillis(
                        remainingNanos(request.deadline()))), TimeUnit.MILLISECONDS);
            } catch (TimeoutException failure) {
                if (cancellableSource != null) {
                    cancellableSource.cancel(true);
                }
                if (cancellableStage != null && cancellableStage != cancellableSource) {
                    cancellableStage.cancel(true);
                }
                future.cancel(true);
                cancellation.cancelForDeadline();
                ExecutionDeadlineExceededException deadlineFailure =
                        new ExecutionDeadlineExceededException("execution deadline exceeded", failure);
                awaitStructuredSourceCleanup(sourceDone, nodeContext, deadlineFailure);
                throw deadlineFailure;
            } catch (InterruptedException failure) {
                if (cancellation.isCancellationRequested()) {
                    awaitStructuredSourceCleanup(sourceDone, nodeContext, failure);
                }
                throw failure;
            } catch (ExecutionException failure) {
                Throwable cause = unwrapCompletionFailure(failure.getCause());
                if (cause instanceof Exception exception) {
                    throw exception;
                }
                throw propagate(cause);
            } catch (CancellationException failure) {
                if (cancellation.isCancellationRequested()) {
                    awaitStructuredSourceCleanup(sourceDone, nodeContext, failure);
                    throw failure;
                }
                throw new NodeExecutionException(failureCode, "structured node completion was cancelled");
            }
        }
    }

    private void awaitStructuredSourceCleanup(java.util.concurrent.CountDownLatch sourceDone,
            NodeExecutionContext nodeContext, Throwable selectedFailure) {
        boolean interrupted = false;
        CleanupBudget budget = CleanupBudget.standard();
        boolean cleaned = false;
        try {
            while (budget.remainingNanos() > 0) {
                try {
                    cleaned = sourceDone.await(budget.remainingNanos(), TimeUnit.NANOSECONDS);
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                    // External cancellation can race with cleanup; the real source still owns resources.
                }
            }
        } finally {
            if (!cleaned) {
                selectedFailure.addSuppressed(new IllegalStateException(
                        "structured source cleanup did not finish before the deadline"));
                logStructuredCleanupTimeout(nodeContext.executionId(), nodeContext.scopePath());
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private NodeResult awaitNode(StageAwaiter awaiter, ExecutionRequest request,
            CancellationHandle cancellation) throws InterruptedException {
        long remainingNanos = remainingNanos(request.deadline());
        if (remainingNanos <= 0) {
            cancellation.cancelForDeadline();
            throw new ExecutionDeadlineExceededException("execution deadline exceeded");
        }
        try {
            return awaiter.completion().get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (CancellationException exception) {
            throw completionCancellation(exception, cancellation);
        } catch (InterruptedException exception) {
            awaiter.cancel();
            throw exception;
        } catch (TimeoutException exception) {
            awaiter.cancel();
            cancellation.cancelForDeadline();
            throw new ExecutionDeadlineExceededException("execution deadline exceeded", exception);
        } catch (ExecutionException exception) {
            Throwable failure = unwrapCompletionFailure(exception.getCause());
            if (failure instanceof CancellationException cancelled) {
                throw completionCancellation(cancelled, cancellation);
            }
            throw propagate(failure);
        }
    }

    private static RuntimeException completionCancellation(CancellationException sourceFailure,
            CancellationHandle cancellation) {
        if (cancellation.isCancellationRequested()) {
            return sourceFailure;
        }
        return new NodeExecutionException("NODE_COMPLETION_CANCELLED", "node completion was cancelled");
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static StageAwaiter bridge(CompletionStage<NodeResult> source,
            ConcurrencyGovernor.Permit permit, AdmissionLease admissionLease) {
        Objects.requireNonNull(source, "node completion must not be null");
        CompletableFuture<NodeResult> completion = new CompletableFuture<>();
        SourceLifecycle lifecycle = new SourceLifecycle(permit, admissionLease);
        lifecycle.arm();
        try {
            source.whenComplete((result, failure) -> {
                try {
                    if (failure == null) {
                        completion.complete(result);
                    } else {
                        completion.completeExceptionally(failure);
                    }
                } finally {
                    lifecycle.close();
                }
            });
        } catch (Throwable failure) {
            lifecycle.close();
            throw failure;
        }
        return new StageAwaiter(completion, source instanceof Future<?> future ? future : null);
    }

    private void resolveOutgoing(String executionId, ExecutionPlan plan, Set<String> included,
            Map<String, Readiness> readiness, NodeCompletion completed, Queue<ReadyNode> ready,
            ExecutionContext context) {
        ArrayDeque<Decision> decisions = new ArrayDeque<>();
        decisions.add(new Decision(completed.nodeId(), completed.result().activatedPorts(), completed.scopePath()));
        while (!decisions.isEmpty()) {
            Decision decision = decisions.removeFirst();
            Map<String, List<String>> outgoing = plan.outgoingByPort().getOrDefault(decision.nodeId(), Map.of());
            new TreeMap<>(outgoing).forEach((port, targets) -> {
                boolean active = decision.activatedPorts().contains(port);
                List<String> stableTargets = targets.stream().sorted().toList();
                for (int index = 0; index < stableTargets.size(); index++) {
                    String target = stableTargets.get(index);
                    if (!included.contains(target)) {
                        continue;
                    }
                    if (isLoopBack(plan, decision.nodeId(), target, port)) {
                        continue;
                    }
                    Readiness state = readiness.get(target);
                    state.resolvedIncoming++;
                    if (active) {
                        state.activeIncoming++;
                        state.activeScopes.add(targetScope(
                                plan, decision, port, target, index, context));
                    }
                    if (state.resolvedIncoming == state.totalIncoming && state.state == NodeState.PENDING) {
                        if (state.activeIncoming > 0) {
                            ready.add(new ReadyNode(target, context.convergeScopes(state.activeScopes)));
                        } else {
                            state.state = NodeState.SKIPPED;
                            if (recorder != null) recorder.queued(executionId, plan.nodesById().get(target), decision.scopePath()).skipped();
                            decisions.addLast(new Decision(target, Set.of(), decision.scopePath()));
                        }
                    }
                }
            });
        }
    }

    private static ScopePath targetScope(ExecutionPlan plan, Decision decision,
            String port, String target, int targetIndex, ExecutionContext context) {
        CompiledNode source = plan.nodesById().get(decision.nodeId());
        if (source.componentContract().componentType() == ComponentType.PARALLEL
                && "BRANCH".equals(port)) {
            return context.parallelBranchScope(
                    decision.scopePath(), source.node().nodeCode(), targetIndex);
        }
        return decision.scopePath();
    }

    private static Set<String> collectRegion(ExecutionPlan plan, Set<String> seeds, String boundaryNodeId) {
        Set<String> included = new LinkedHashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>(new java.util.TreeSet<>(seeds));
        while (!pending.isEmpty()) {
            String nodeId = pending.removeFirst();
            if (!included.add(nodeId) || nodeId.equals(boundaryNodeId)) {
                continue;
            }
            CompiledNode node = plan.nodesById().get(nodeId);
            if (node == null) {
                throw new NodeExecutionException("STRUCTURED_PLAN_INVALID", "region seed is absent from plan");
            }
            Map<String, List<String>> outgoing = plan.outgoingByPort().getOrDefault(nodeId, Map.of());
            new TreeMap<>(outgoing).forEach((port, targets) -> {
                if (node.componentContract().componentType() == ComponentType.LOOP
                        && "START".equals(text(node.node().config().get("role")))
                        && "BODY".equals(port)) {
                    return;
                }
                for (String target : targets) {
                    if (!isLoopBack(plan, nodeId, target, port)) {
                        pending.addLast(target);
                    }
                }
            });
        }
        return Set.copyOf(included);
    }

    private static Map<String, Readiness> initializeReadiness(ExecutionPlan plan, Set<String> included) {
        Map<String, Integer> incoming = new HashMap<>();
        included.forEach(nodeId -> incoming.put(nodeId, 0));
        for (String sourceId : included) {
            CompiledNode source = plan.nodesById().get(sourceId);
            for (Map.Entry<String, List<String>> port
                    : plan.outgoingByPort().getOrDefault(sourceId, Map.of()).entrySet()) {
                if (source.componentContract().componentType() == ComponentType.LOOP
                        && "START".equals(text(source.node().config().get("role")))
                        && "BODY".equals(port.getKey())) {
                    continue;
                }
                for (String target : port.getValue()) {
                    if (included.contains(target) && !isLoopBack(plan, sourceId, target, port.getKey())) {
                        incoming.compute(target, (ignored, value) -> value + 1);
                    }
                }
            }
        }
        Map<String, Readiness> result = new HashMap<>();
        incoming.forEach((nodeId, total) -> result.put(nodeId, new Readiness(total)));
        return result;
    }

    private static boolean isLoopBack(ExecutionPlan plan, String sourceId, String targetId, String port) {
        CompiledNode source = plan.nodesById().get(sourceId);
        return source != null && source.componentContract().componentType() == ComponentType.LOOP
                && "END".equals(text(source.node().config().get("role")))
                && "NEXT".equals(port) && targetId.equals(plan.loopPairs().get(sourceId));
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private boolean cancelAndAwait(Map<Future<NodeCompletion>, AdmissionLease> running,
            CleanupBudget budget, boolean awaitSourceCleanup) throws InterruptedException {
        running.forEach(DagScheduler::cancelRunningTask);
        boolean cleaned = true;
        boolean interrupted = false;
        try {
            for (AdmissionLease lease : List.copyOf(running.values())) {
                AwaitOutcome task = awaitCleanupSignal(lease, budget, false);
                cleaned &= task.completed();
                interrupted |= task.interrupted();
                if (awaitSourceCleanup || lease.structuredCleanupRequired()) {
                    AwaitOutcome source = awaitCleanupSignal(lease, budget, true);
                    cleaned &= source.completed();
                    interrupted |= source.interrupted();
                }
            }
        } finally {
            running.clear();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return cleaned;
    }

    private AwaitOutcome awaitCleanupSignal(
            AdmissionLease lease, CleanupBudget budget, boolean sourceSignal) {
        boolean interrupted = false;
        while (budget.remainingNanos() > 0) {
            try {
                boolean completed = sourceSignal
                        ? lease.awaitSourceCleanup(budget.remainingNanos())
                        : lease.awaitTaskCompletion(budget.remainingNanos());
                return new AwaitOutcome(completed, interrupted);
            } catch (InterruptedException ignored) {
                interrupted = true;
                // Cancellation interrupts cleanup workers too; keep draining until the bounded deadline.
            }
        }
        return new AwaitOutcome(false, interrupted);
    }

    private static void validateExecutionBoundary(ExecutionPlan plan, ExecutionRequest request) {
        if (request.mode() != ExecutionMode.EPHEMERAL) {
            throw new NodeExecutionException("EXECUTION_MODE_UNSUPPORTED",
                    "flow runtime only supports EPHEMERAL execution");
        }
        if (!plan.flowIdentity().tenantId().equals(request.tenantId())) {
            throw new NodeExecutionException("EXECUTION_TENANT_MISMATCH",
                    "execution tenant does not match compiled flow tenant");
        }
    }

    private void checkStop(ExecutionRequest request, CancellationHandle cancellation) {
        if (!clock.instant().isBefore(request.deadline())) {
            cancellation.cancelForDeadline();
            throw new ExecutionDeadlineExceededException("execution deadline exceeded");
        }
        cancellation.throwIfCancellationRequested();
    }

    private long remainingNanos(Instant deadline) {
        try {
            return Math.max(0, clock.instant().until(deadline, ChronoUnit.NANOS));
        } catch (ArithmeticException exception) {
            return Long.MAX_VALUE;
        }
    }

    private void logStructuredCleanupTimeout(String executionId, ScopePath scopePath) {
        try {
            executionLogger.error("structured source cleanup timed out", null, Map.of(
                    "executionId", executionId,
                    "scopePath", scopePath.value()));
        } catch (RuntimeException ignored) {
            // Cleanup observation must not replace the already selected execution outcome.
        }
    }

    private ExecutionResult result(String executionId, ExecutionStatus status, Object value,
            String errorCode, String errorMessage, Instant startedAt) {
        return new ExecutionResult(executionId, status, value, errorCode, errorMessage, null,
                startedAt, clock.instant());
    }

    private ExecutionResult observedResult(ExecutionResult result) {
        if (recorder == null) return result;
        try { return recorder.finished(result); }
        catch (RuntimeException failure) {
            return new ExecutionResult(result.executionId(), ExecutionStatus.FAILED, null,
                    "OBSERVATION_AUDIT_FAILED", "critical observation could not be flushed",
                    result.errorId(), result.startedAt(), clock.instant());
        }
    }

    private ExecutionResult result(String executionId, ExecutionStatus status, Object value,
            String errorCode, String errorMessage, Instant startedAt, Object cause) {
        var result = result(executionId, status, value, errorCode, errorMessage, startedAt);
        return recorder == null ? result : recorder.identify(result, cause);
    }

    private static String resourceKey(CompiledNode node) {
        Object configured = node.node().config().get(EngineReservedConfig.RESOURCE_KEY);
        if (configured instanceof String text && !text.isBlank()) {
            return text.strip();
        }
        return "component:" + node.node().componentCode();
    }

    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new RuntimeException(failure);
    }

    /** 仅将本调度器新建的 checked 包装显式关联到已观察异常，不遍历业务 cause 链。 */
    private RuntimeException propagateObserved(String executionId, Throwable failure) {
        RuntimeException propagated = propagate(failure);
        if (recorder != null && propagated != failure)
            recorder.inheritCause(executionId, failure, propagated);
        return propagated;
    }

    /**
     * Atomically rejects future schedules, cancels every active execution token and then stops this
     * scheduler's virtual-thread executor. Process-shared governors and admissions remain usable by
     * other schedulers.
     */
    @Override
    public void close() {
        List<ExecutionRegistration> registrations;
        synchronized (lifecycleMonitor) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            registrations = List.copyOf(activeExecutions.values());
        }
        registrations.forEach(ExecutionRegistration::cancelFromClose);
        virtualThreads.shutdownNow();
        CleanupBudget closeBudget = CleanupBudget.standard();
        registrations.forEach(registration -> registration.awaitCloseCompletion(closeBudget));
    }

    /**
     * Owns one controller admission and one terminal decision across submit, close and completion races.
     * Reason selection and winning-result selection share {@code terminalMonitor}; actual future completion
     * stays outside that monitor because dependent user callbacks may execute inline and re-enter the engine.
     */
    private final class ExecutionRegistration {

        private static final int SUBMITTED = 0;
        private static final int RUNNING = 1;
        private static final int FINISHED = 2;

        private final String executionId;
        private final Instant scheduledAt;
        private final CancellationHandle cancellation;
        private final CompletableFuture<ExecutionResult> completion;
        private final AtomicInteger state = new AtomicInteger(SUBMITTED);
        private final Object terminalMonitor = new Object();
        private final AtomicBoolean resultPublished = new AtomicBoolean();
        private ExecutionResult winningResult;
        private volatile Future<?> controllerFuture;

        private ExecutionRegistration(String executionId, Instant scheduledAt,
                CancellationHandle cancellation, CompletableFuture<ExecutionResult> completion) {
            this.executionId = executionId;
            this.scheduledAt = scheduledAt;
            this.cancellation = cancellation;
            this.completion = completion;
        }

        private boolean start() {
            return state.compareAndSet(SUBMITTED, RUNNING);
        }

        private void controllerFuture(Future<?> future) {
            this.controllerFuture = Objects.requireNonNull(future, "controller future must not be null");
        }

        private boolean cancelFromClose() {
            Future<?> future;
            CancellationHandle.Reason reason;
            synchronized (terminalMonitor) {
                if (winningResult != null) {
                    return false;
                }
                boolean closeWon = selectReasonLocked(CancellationHandle.Reason.USER);
                reason = cancellation.reason();
                if (!closeWon && reason != CancellationHandle.Reason.USER) {
                    return false;
                }
                future = controllerFuture;
            }
            lifecycleObserver.afterCloseReasonSelection(reason);
            if (future != null) {
                future.cancel(true);
            }
            if (finishBeforeStart()) {
                ExecutionResult selectedResult;
                synchronized (terminalMonitor) {
                    if (winningResult == null) {
                        winningResult = cancelledResult();
                    }
                    selectedResult = winningResult;
                }
                completeSelectedResult(selectedResult);
            }
            return true;
        }

        private void awaitCloseCompletion(CleanupBudget closeBudget) {
            long remainingNanos = closeBudget.remainingNanos();
            if (remainingNanos <= 0) {
                return;
            }
            try {
                completion.get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException ignored) {
                // close is bounded; the execution handle retains the eventual cleanup result.
            }
        }

        private boolean selectReason(CancellationHandle.Reason requestedReason) {
            synchronized (terminalMonitor) {
                return selectReasonLocked(requestedReason);
            }
        }

        private boolean selectReasonLocked(CancellationHandle.Reason requestedReason) {
            return switch (requestedReason) {
                case USER -> cancellation.cancel();
                case DEADLINE -> cancellation.cancelForDeadline();
                case FAILURE -> cancellation.cancelForFailure();
                case TERMINAL -> cancellation.cancelForTerminal();
            };
        }

        private void publishFromController(ExecutionResult candidate) {
            ExecutionResult selectedResult;
            synchronized (terminalMonitor) {
                if (winningResult == null) {
                    winningResult = normalizeForWinningReason(candidate);
                }
                selectedResult = winningResult;
            }
            completeSelectedResult(selectedResult);
        }

        private void publishUnexpectedControllerFailure() {
            ExecutionResult selectedResult;
            synchronized (terminalMonitor) {
                if (winningResult == null) {
                    if (cancellation.reason() == null) {
                        selectReasonLocked(CancellationHandle.Reason.FAILURE);
                    }
                    ExecutionResult safeFailure = result(executionId, ExecutionStatus.FAILED, null,
                            "NODE_EXECUTION_FAILED", "node execution failed", scheduledAt);
                    winningResult = normalizeForWinningReason(safeFailure);
                }
                selectedResult = winningResult;
            }
            completeSelectedResult(selectedResult);
        }

        private ExecutionResult normalizeForWinningReason(ExecutionResult candidate) {
            // An unprotected request that may have crossed the socket boundary is a safer public
            // outcome than USER/DEADLINE cancellation, but is selected only after source cleanup.
            if (candidate.status() == ExecutionStatus.WAITING_MANUAL) {
                return candidate;
            }
            return switch (cancellation.reason()) {
                case USER -> cancelledResult();
                case DEADLINE -> result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_DEADLINE_EXCEEDED", "execution deadline exceeded", scheduledAt);
                case FAILURE -> candidate.status() == ExecutionStatus.FAILED
                        || candidate.status() == ExecutionStatus.WAITING_MANUAL
                        ? candidate
                        : result(executionId, ExecutionStatus.FAILED, null,
                                "NODE_EXECUTION_FAILED", "node execution failed", scheduledAt);
                case TERMINAL -> candidate;
                case null -> candidate;
            };
        }

        private ExecutionResult cancelledResult() {
            return result(executionId, ExecutionStatus.CANCELLED, null,
                    "EXECUTION_CANCELLED", "execution was cancelled", scheduledAt);
        }

        private void completeSelectedResult(ExecutionResult selectedResult) {
            if (resultPublished.compareAndSet(false, true)) {
                completion.complete(observedResult(selectedResult));
            }
        }

        private boolean finishBeforeStart() {
            if (state.compareAndSet(SUBMITTED, FINISHED)) {
                release();
                return true;
            }
            return false;
        }

        private void finish() {
            if (state.compareAndSet(RUNNING, FINISHED)) {
                release();
            }
        }

        private void release() {
            activeExecutions.remove(cancellation, this);
            executionAdmission.release();
            lifecycleObserver.afterExecutionAdmissionReleased();
        }
    }

    private enum NodeState {
        PENDING,
        RUNNING,
        COMPLETED,
        SKIPPED
    }

    private static final class Readiness {

        private final int totalIncoming;
        private int resolvedIncoming;
        private int activeIncoming;
        private final List<ScopePath> activeScopes = new ArrayList<>();
        private NodeState state = NodeState.PENDING;

        private Readiness(int totalIncoming) {
            this.totalIncoming = totalIncoming;
        }
    }

    private record ReadyNode(String nodeId, ScopePath scopePath) {
    }

    private record Decision(String nodeId, Set<String> activatedPorts, ScopePath scopePath) {
    }

    private record RegionOutcome(NodeResult.TerminalOutcome terminalOutcome, boolean boundaryReached) {
    }

    private record NodeCompletion(String nodeId, ScopePath scopePath, NodeResult result, Throwable failure) {

        private static NodeCompletion success(String nodeId, ScopePath scopePath, NodeResult result) {
            return new NodeCompletion(nodeId, scopePath, result, null);
        }

        private static NodeCompletion failure(String nodeId, ScopePath scopePath, Throwable failure) {
            return new NodeCompletion(nodeId, scopePath, null, failure);
        }
    }

    private record LockAddress(String flowId, String acquireNodeId, ScopePath scopePath) {
    }

    /** Scheduler-private structural state; leases never enter business ctx or public results. */
    private static final class StructuralState {

        private final ConcurrentHashMap<LockAddress, HeldLease> heldLeases = new ConcurrentHashMap<>();

        private void register(LockAddress address, LockNodeExecutor executor, LockLease lease) {
            HeldLease held = new HeldLease(executor, lease);
            if (heldLeases.putIfAbsent(address, held) != null) {
                executor.cleanup(lease);
                throw new NodeExecutionException("LOCK_ALREADY_HELD", "paired LOCK is already held in this scope");
            }
        }

        private void release(LockAddress address) {
            HeldLease held = heldLeases.remove(address);
            if (held == null) {
                throw new NodeExecutionException("LOCK_LEASE_MISSING", "paired LOCK lease is not held");
            }
            held.executor().release(held.lease());
        }

        private List<LockAddress> cleanupAll() {
            List<LockAddress> failures = new ArrayList<>();
            List<Map.Entry<LockAddress, HeldLease>> remaining = new ArrayList<>(heldLeases.entrySet());
            remaining.sort(java.util.Comparator
                    .comparing((Map.Entry<LockAddress, HeldLease> entry) -> entry.getKey().flowId())
                    .thenComparing(entry -> entry.getKey().acquireNodeId())
                    .thenComparing(entry -> entry.getKey().scopePath().value()));
            for (Map.Entry<LockAddress, HeldLease> entry : remaining) {
                if (heldLeases.remove(entry.getKey(), entry.getValue())) {
                    if (!entry.getValue().executor().cleanup(entry.getValue().lease())) {
                        failures.add(entry.getKey());
                    }
                }
            }
            return List.copyOf(failures);
        }
    }

    private record HeldLease(LockNodeExecutor executor, LockLease lease) {
    }

    /** One engine-owned future bridges arbitrary CompletionStage implementations exactly once. */
    private record StageAwaiter(CompletableFuture<NodeResult> completion, Future<?> cancellableSource) {

        private void cancel() {
            try {
                if (cancellableSource != null) {
                    cancellableSource.cancel(true);
                }
            } finally {
                completion.cancel(true);
            }
        }
    }

    /** Holds component and node admission until the adapter's actual source stage has completed. */
    private static final class SourceLifecycle implements AutoCloseable {

        private final ConcurrencyGovernor.Permit permit;
        private final AdmissionLease admissionLease;
        private final java.util.concurrent.atomic.AtomicBoolean open =
                new java.util.concurrent.atomic.AtomicBoolean(true);

        private SourceLifecycle(ConcurrencyGovernor.Permit permit, AdmissionLease admissionLease) {
            this.permit = Objects.requireNonNull(permit, "permit must not be null");
            this.admissionLease = Objects.requireNonNull(admissionLease, "admissionLease must not be null");
        }

        private void arm() {
            admissionLease.deferToSource();
        }

        @Override
        public void close() {
            if (open.compareAndSet(true, false)) {
                try {
                    permit.close();
                } finally {
                    admissionLease.sourceDone();
                }
            }
        }
    }

    /** Releases node admission only after both the source and its submitted callable are finished. */
    private static final class AdmissionLease {

        private Runnable beforeStartCancellation = () -> { };

        private final Semaphore admission;
        private final boolean structuredCleanupRequired;
        private boolean started;
        private boolean deferred;
        private boolean sourceDone;
        private boolean taskDone;
        private boolean released;
        private final java.util.concurrent.CountDownLatch taskSignal =
                new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch sourceSignal =
                new java.util.concurrent.CountDownLatch(1);

        private AdmissionLease(Semaphore admission, boolean structuredCleanupRequired) {
            this.admission = admission;
            this.structuredCleanupRequired = structuredCleanupRequired;
        }

        private synchronized void started() {
            if (started || released) {
                throw new IllegalStateException("cancelled node task started after admission was released");
            }
            started = true;
        }

        private synchronized void releaseBeforeStart() {
            if (!started && !released) {
                released = true;
                try {
                    beforeStartCancellation.run();
                } finally {
                    admission.release();
                    taskSignal.countDown();
                    sourceSignal.countDown();
                }
            }
        }

        private synchronized void releaseAfterRun() {
            taskDone = true;
            taskSignal.countDown();
            releaseIfFinished();
        }

        private synchronized void releaseForStructuredWait() {
            if (!started || deferred || taskDone || released) {
                throw new IllegalStateException("structured node admission cannot be released in this state");
            }
            released = true;
            admission.release();
            sourceSignal.countDown();
        }

        private synchronized void deferToSource() {
            if (!started || taskDone || deferred || released) {
                throw new IllegalStateException("node admission cannot be transferred after release");
            }
            deferred = true;
        }

        private synchronized void sourceDone() {
            if (!deferred) {
                throw new IllegalStateException("source completion arrived before admission transfer");
            }
            sourceDone = true;
            releaseIfFinished();
        }

        private void releaseIfFinished() {
            if (!released && taskDone && (!deferred || sourceDone)) {
                released = true;
                admission.release();
                sourceSignal.countDown();
            }
        }

        private boolean awaitTaskCompletion(long remainingNanos) throws InterruptedException {
            return remainingNanos > 0 && taskSignal.await(remainingNanos, TimeUnit.NANOSECONDS);
        }

        private boolean awaitSourceCleanup(long remainingNanos) throws InterruptedException {
            return remainingNanos > 0 && sourceSignal.await(remainingNanos, TimeUnit.NANOSECONDS);
        }

        private boolean structuredCleanupRequired() {
            return structuredCleanupRequired;
        }
    }

    private record AwaitOutcome(boolean completed, boolean interrupted) {
    }
}
