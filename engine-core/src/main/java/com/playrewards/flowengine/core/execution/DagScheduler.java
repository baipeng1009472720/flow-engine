package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.CompiledNode.InputBinding;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.JsonValueValidator;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.ComponentInputs;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private static final long ADMISSION_POLL_MILLIS = 25;
    private static final Semaphore PROCESS_NODE_TASK_ADMISSION =
            new Semaphore(DEFAULT_MAX_IN_FLIGHT_NODE_TASKS, true);
    private static final Semaphore PROCESS_EXECUTION_ADMISSION =
            new Semaphore(DEFAULT_MAX_IN_FLIGHT_EXECUTIONS, true);

    private final NodeExecutorRegistry executors;
    private final ConcurrencyGovernor governor;
    private final Clock clock;
    private final Semaphore nodeTaskAdmission;
    private final Semaphore executionAdmission;
    private final ExecutionLifecycleObserver lifecycleObserver;
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
                ExecutionLifecycleObserver.NOOP);
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
                ExecutionLifecycleObserver.NOOP);
    }

    private DagScheduler(NodeExecutorRegistry executors, ConcurrencyGovernor governor, Clock clock,
            Semaphore nodeTaskAdmission, Semaphore executionAdmission,
            ExecutionLifecycleObserver lifecycleObserver) {
        this.executors = Objects.requireNonNull(executors, "executors must not be null");
        this.governor = Objects.requireNonNull(governor, "governor must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.nodeTaskAdmission = Objects.requireNonNull(nodeTaskAdmission, "nodeTaskAdmission must not be null");
        this.executionAdmission = Objects.requireNonNull(
                executionAdmission, "executionAdmission must not be null");
        this.lifecycleObserver = Objects.requireNonNull(lifecycleObserver, "lifecycleObserver must not be null");
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
                lifecycleObserver);
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
        synchronized (lifecycleMonitor) {
            if (closed.get()) {
                cancellation.cancelForFailure();
                completion.complete(result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_SCHEDULER_CLOSED", "execution scheduler is closed", scheduledAt));
                return handle;
            }
            if (!executionAdmission.tryAcquire()) {
                cancellation.cancelForFailure();
                completion.complete(result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_CAPACITY_EXCEEDED", "execution capacity exceeded", scheduledAt));
                return handle;
            }
            AtomicReference<Thread> controller = new AtomicReference<>();
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes = new ConcurrentHashMap<>();
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
                        ExecutionResult executionResult =
                                run(executionId, plan, request, context, cancellation, registration, runningNodes);
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
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes) {
        Instant startedAt = clock.instant();
        try {
            validateExecutionBoundary(plan, request);
            checkStop(request, cancellation);
            NodeResult.TerminalOutcome outcome = executeGraph(
                    executionId, plan, request, context, cancellation, registration, runningNodes);
            return result(executionId, outcome.status(), outcome.result(), outcome.errorCode(),
                    outcome.errorMessage(), startedAt);
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
            return result(executionId, ExecutionStatus.FAILED, null,
                    exception.code(), exception.getMessage(), startedAt);
        } catch (RuntimeException exception) {
            lifecycleObserver.beforeControllerReasonSelection(CancellationHandle.Reason.FAILURE);
            registration.selectReason(CancellationHandle.Reason.FAILURE);
            return result(executionId, ExecutionStatus.FAILED, null, "NODE_EXECUTION_FAILED",
                    "node execution failed", startedAt);
        }
    }

    private NodeResult.TerminalOutcome executeGraph(String executionId, ExecutionPlan plan,
            ExecutionRequest request, ExecutionContext context, CancellationHandle cancellation,
            ExecutionRegistration registration,
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes) throws InterruptedException {
        Map<String, Readiness> readiness = new HashMap<>();
        PriorityQueue<String> ready = new PriorityQueue<>();
        for (String nodeId : plan.nodesById().keySet()) {
            int incoming = plan.reverseAdjacency().getOrDefault(nodeId, List.of()).size();
            readiness.put(nodeId, new Readiness(incoming));
            if (incoming == 0) {
                ready.add(nodeId);
            }
        }
        CompletionService<NodeCompletion> completions = new ExecutorCompletionService<>(virtualThreads);
        int running = 0;
        while (!ready.isEmpty() || running > 0) {
            checkStop(request, cancellation);
            while (!ready.isEmpty() && nodeTaskAdmission.tryAcquire()) {
                String nodeId = ready.remove();
                submitNode(completions, runningNodes, readiness, nodeId,
                        executionId, plan, request, context, cancellation);
                running++;
            }
            if (running == 0 && !ready.isEmpty()) {
                acquireNodeAdmission(request, cancellation);
                String nodeId = ready.remove();
                submitNode(completions, runningNodes, readiness, nodeId,
                        executionId, plan, request, context, cancellation);
                running++;
            }
            if (running == 0) {
                break;
            }
            long remainingNanos = remainingNanos(request.deadline());
            if (remainingNanos <= 0) {
                throw new ExecutionDeadlineExceededException("execution deadline exceeded");
            }
            Future<NodeCompletion> completedFuture = completions.poll(remainingNanos, TimeUnit.NANOSECONDS);
            if (completedFuture == null) {
                throw new ExecutionDeadlineExceededException("execution deadline exceeded");
            }
            running--;
            runningNodes.remove(completedFuture);
            NodeCompletion completed;
            try {
                completed = completedFuture.get();
            } catch (CancellationException exception) {
                throw exception;
            } catch (ExecutionException exception) {
                throw propagate(exception.getCause());
            }
            if (completed.failure() != null) {
                throw propagate(completed.failure());
            }
            Readiness state = readiness.get(completed.nodeId());
            state.state = NodeState.COMPLETED;
            CompiledNode node = plan.nodesById().get(completed.nodeId());
            completed.result().outputs().forEach((key, value) ->
                    context.writeOnce(node.node().nodeCode(), ScopePath.root(), key, value));
            if (completed.result().terminalOutcome() != null) {
                if (registration.selectReason(CancellationHandle.Reason.TERMINAL)
                        || cancellation.reason() == CancellationHandle.Reason.TERMINAL) {
                    return completed.result().terminalOutcome();
                }
                throw new CancellationException("cancellation won the terminal outcome race");
            }
            resolveOutgoing(plan, readiness, completed.nodeId(), completed.result().activatedPorts(), ready);
        }
        throw new NodeExecutionException("FLOW_TERMINATED_WITHOUT_END",
                "active execution graph completed without reaching END");
    }

    private void submitNode(CompletionService<NodeCompletion> completions,
            Map<Future<NodeCompletion>, AdmissionLease> runningNodes,
            Map<String, Readiness> readiness, String nodeId,
            String executionId, ExecutionPlan plan, ExecutionRequest request, ExecutionContext context,
            CancellationHandle cancellation) {
        Readiness state = readiness.get(nodeId);
        if (state.state != NodeState.PENDING) {
            nodeTaskAdmission.release();
            throw new IllegalStateException("ready queue contained a non-pending node: " + nodeId);
        }
        state.state = NodeState.RUNNING;
        AdmissionLease admissionLease = new AdmissionLease(nodeTaskAdmission);
        try {
            Future<NodeCompletion> future = completions.submit(() -> {
                admissionLease.started();
                try {
                    return invokeNode(executionId, plan, nodeId, request, context, cancellation, admissionLease);
                } finally {
                    admissionLease.releaseAfterRun();
                }
            });
            runningNodes.put(future, admissionLease);
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
            ExecutionRequest request, ExecutionContext context, CancellationHandle cancellation,
            AdmissionLease admissionLease) {
        ConcurrencyGovernor.Permit permit = null;
        try {
            checkStop(request, cancellation);
            CompiledNode node = plan.nodesById().get(nodeId);
            ComponentInputs inputs = resolveInputs(plan, node, context);
            NodeExecutionContext nodeContext = new NodeExecutionContext(executionId, plan, node, context,
                    ScopePath.root(), inputs, cancellation, request, clock);
            String resourceKey = resourceKey(node);
            ConcurrencyGovernor.PermitRequest permitRequest = new ConcurrencyGovernor.PermitRequest(
                    request.tenantId(), plan.flowIdentity().flowId(), node.node().componentCode(), resourceKey,
                    node.componentContract().capabilities().maxConcurrency());
            permit = governor.acquire(permitRequest, cancellation, request.deadline());
            nodeContext.throwIfStopped();
            NodeExecutor executor = executors.required(node.componentContract().componentType(),
                    node.node().nodeCode());
            NodeInvocation invocation = Objects.requireNonNull(executor.execute(nodeContext),
                    "node invocation must not be null");
            StageAwaiter awaiter = bridge(invocation.completion(), permit, admissionLease);
            permit = null; // The source lifecycle now owns both concurrency leases.
            try (CancellationHandle.CancellationRegistration ignoredRegistration =
                    cancellation.onCancellation(awaiter::cancel)) {
                NodeResult result = awaitNode(awaiter, request, cancellation);
                validateOutputs(node, result);
                return NodeCompletion.success(nodeId, result);
            }
        } catch (Throwable failure) {
            return NodeCompletion.failure(nodeId, failure);
        } finally {
            if (permit != null) {
                permit.close();
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

    private ComponentInputs resolveInputs(ExecutionPlan plan, CompiledNode node, ExecutionContext context) {
        Map<String, Object> required = new LinkedHashMap<>();
        Map<String, Object> optional = new LinkedHashMap<>();
        Map<String, InputSlotContract> slots = new TreeMap<>(node.componentContract().inputSlots());
        for (Map.Entry<String, InputSlotContract> slot : slots.entrySet()) {
            InputBinding binding = node.inputBindings().get(slot.getKey());
            if (binding == null) {
                if (slot.getValue().required()) {
                    throw new NodeExecutionException("NODE_REQUIRED_INPUT_MISSING",
                            "required input has no binding: " + slot.getKey());
                }
                continue;
            }
            CompiledNode source = plan.nodesById().get(binding.sourceNodeId());
            ExecutionContext.ContextValue value = context.lookup(
                    source.node().nodeCode(), ScopePath.root(), binding.sourceOutputKey());
            if (!value.present()) {
                if (slot.getValue().required()) {
                    throw new NodeExecutionException("NODE_REQUIRED_INPUT_MISSING",
                            "required input source produced no value: " + slot.getKey());
                }
                continue;
            }
            JsonValueValidator.validate(value.value(), slot.getValue().protocol(), "NODE_INPUT_INVALID",
                    "inputs." + slot.getKey());
            (slot.getValue().required() ? required : optional).put(slot.getKey(), value.value());
        }
        return new ComponentInputs(required, optional);
    }

    private static void validateOutputs(CompiledNode node, NodeResult result) {
        Objects.requireNonNull(result, "node result must not be null");
        Map<String, OutputSlotContract> slots = node.componentContract().outputSlots();
        for (Map.Entry<String, Object> output : result.outputs().entrySet()) {
            OutputSlotContract slot = slots.get(output.getKey());
            if (slot == null) {
                throw new NodeExecutionException("NODE_OUTPUT_UNKNOWN",
                        "node produced undeclared output: " + output.getKey());
            }
            JsonValueValidator.validate(output.getValue(), slot.protocol(), "NODE_OUTPUT_INVALID",
                    "outputs." + output.getKey());
        }
    }

    private static void resolveOutgoing(ExecutionPlan plan, Map<String, Readiness> readiness,
            String completedNodeId, Set<String> activatedPorts, Queue<String> ready) {
        ArrayDeque<Decision> decisions = new ArrayDeque<>();
        decisions.add(new Decision(completedNodeId, activatedPorts));
        while (!decisions.isEmpty()) {
            Decision decision = decisions.removeFirst();
            Map<String, List<String>> outgoing = plan.outgoingByPort().getOrDefault(decision.nodeId(), Map.of());
            new TreeMap<>(outgoing).forEach((port, targets) -> {
                boolean active = decision.activatedPorts().contains(port);
                for (String target : targets) {
                    Readiness state = readiness.get(target);
                    state.resolvedIncoming++;
                    if (active) {
                        state.activeIncoming++;
                    }
                    if (state.resolvedIncoming == state.totalIncoming && state.state == NodeState.PENDING) {
                        if (state.activeIncoming > 0) {
                            ready.add(target);
                        } else {
                            state.state = NodeState.SKIPPED;
                            decisions.addLast(new Decision(target, Set.of()));
                        }
                    }
                }
            });
        }
    }

    private static void validateExecutionBoundary(ExecutionPlan plan, ExecutionRequest request) {
        if (request.mode() != ExecutionMode.EPHEMERAL) {
            throw new NodeExecutionException("EXECUTION_MODE_UNSUPPORTED",
                    "Task 5 only supports EPHEMERAL execution");
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

    private ExecutionResult result(String executionId, ExecutionStatus status, Object value,
            String errorCode, String errorMessage, Instant startedAt) {
        return new ExecutionResult(executionId, status, value, errorCode, errorMessage, null,
                startedAt, clock.instant());
    }

    private static String resourceKey(CompiledNode node) {
        Object configured = node.node().config().get("resourceKey");
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

        private void cancelFromClose() {
            ExecutionResult selectedResult;
            Future<?> future;
            CancellationHandle.Reason reason;
            synchronized (terminalMonitor) {
                if (winningResult != null) {
                    return;
                }
                boolean closeWon = selectReasonLocked(CancellationHandle.Reason.USER);
                reason = cancellation.reason();
                if (!closeWon && reason != CancellationHandle.Reason.USER) {
                    return;
                }
                winningResult = cancelledResult();
                selectedResult = winningResult;
                future = controllerFuture;
            }
            lifecycleObserver.afterCloseReasonSelection(reason);
            if (future != null) {
                future.cancel(true);
            }
            finishBeforeStart();
            completeSelectedResult(selectedResult);
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
            return switch (cancellation.reason()) {
                case USER -> cancelledResult();
                case DEADLINE -> result(executionId, ExecutionStatus.FAILED, null,
                        "EXECUTION_DEADLINE_EXCEEDED", "execution deadline exceeded", scheduledAt);
                case FAILURE -> candidate.status() == ExecutionStatus.FAILED
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
                completion.complete(selectedResult);
            }
        }

        private void finishBeforeStart() {
            if (state.compareAndSet(SUBMITTED, FINISHED)) {
                release();
            }
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
        private NodeState state = NodeState.PENDING;

        private Readiness(int totalIncoming) {
            this.totalIncoming = totalIncoming;
        }
    }

    private record Decision(String nodeId, Set<String> activatedPorts) {
    }

    private record NodeCompletion(String nodeId, NodeResult result, Throwable failure) {

        private static NodeCompletion success(String nodeId, NodeResult result) {
            return new NodeCompletion(nodeId, result, null);
        }

        private static NodeCompletion failure(String nodeId, Throwable failure) {
            return new NodeCompletion(nodeId, null, failure);
        }
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

        private final Semaphore admission;
        private boolean started;
        private boolean deferred;
        private boolean sourceDone;
        private boolean taskDone;
        private boolean released;

        private AdmissionLease(Semaphore admission) {
            this.admission = admission;
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
                admission.release();
            }
        }

        private synchronized void releaseAfterRun() {
            taskDone = true;
            releaseIfFinished();
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
            }
        }
    }
}
