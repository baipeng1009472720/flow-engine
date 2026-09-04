package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.await;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.compile;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.contract;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.edge;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.node;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.ComponentInvocation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Verifies cooperative stop propagation and hierarchical-permit cleanup. */
class CancellationAndDeadlineTest {

    @Test
    void cancellation_interrupts_a_running_virtual_thread_and_reaches_the_node_token() throws Exception {
        BlockingExecutor blocker = new BlockingExecutor();
        ExecutionPlan plan = blockingPlan();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(blocker));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(plan, request(Map.of()));
            assertTrue(blocker.started.await(2, TimeUnit.SECONDS));
            assertTrue(handle.cancel());

            ExecutionResult result = await(handle);
            assertEquals(ExecutionStatus.CANCELLED, result.status());
            assertEquals("EXECUTION_CANCELLED", result.errorCode());
            assertTrue(blocker.virtualThread.get());
            assertTrue(blocker.observedCancellation.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void deadline_cancels_running_children_and_finishes_with_a_deadline_error() throws Exception {
        BlockingExecutor blocker = new BlockingExecutor();
        ExecutionPlan plan = blockingPlan();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(blocker));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(plan,
                    request(Map.of(), Instant.now().plus(150, ChronoUnit.MILLIS)));
            assertTrue(blocker.started.await(2, TimeUnit.SECONDS));

            ExecutionResult result = await(handle);
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
            assertTrue(blocker.observedCancellation.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellation_cancels_an_outstanding_asynchronous_node_completion() throws Exception {
        AsyncExecutor asynchronous = new AsyncExecutor();
        ExecutionPlan plan = blockingPlan();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(asynchronous));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(plan, request(Map.of()));
            assertTrue(asynchronous.started.await(2, TimeUnit.SECONDS));
            assertTrue(handle.cancel());

            assertEquals(ExecutionStatus.CANCELLED, await(handle).status());
            assertTrue(asynchronous.completion.cancelled.await(2, TimeUnit.SECONDS));
            assertTrue(asynchronous.completion.isCancelled());
        }
    }

    @Test
    void close_cancels_active_non_future_source_and_restores_shared_capacity_for_another_scheduler()
            throws Exception {
        CloseAwareStageExecutor closeAware = new CloseAwareStageExecutor();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(closeAware));
        ExecutionPlan plan = closeAwarePlan();
        DagScheduler closing = new DagScheduler(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC());
        DagScheduler survivor = new DagScheduler(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC());
        try {
            ExecutionHandle active = closing.schedule(plan, request(Map.of()));
            assertTrue(closeAware.firstEntered.await(2, TimeUnit.SECONDS));

            closing.close();
            assertEquals(ExecutionStatus.CANCELLED, await(active).status());
            assertTrue(closeAware.firstObservedCancellation.await(2, TimeUnit.SECONDS));

            ExecutionHandle next = survivor.schedule(plan, request(Map.of()));
            assertFalse(closeAware.secondEntered.await(200, TimeUnit.MILLISECONDS));
            closeAware.allowFirstSourceCompletion.countDown();

            assertTrue(closeAware.firstSourceCompleted.await(2, TimeUnit.SECONDS));
            assertEquals(ExecutionStatus.SUCCEEDED, await(next).status());
            assertTrue(closeAware.secondEntered.await(2, TimeUnit.SECONDS));
        } finally {
            closeAware.allowFirstSourceCompletion.countDown();
            closing.close();
            survivor.close();
        }
    }

    @Test
    void schedule_after_close_returns_a_completed_rejection_without_consuming_admission() throws Exception {
        DagScheduler scheduler = new DagScheduler(
                NodeExecutorRegistry.builtIns(), ConcurrencyGovernor.defaults(), Clock.systemUTC());
        scheduler.close();

        ExecutionResult rejected = await(scheduler.schedule(closeAwarePlan(), request(Map.of())));

        assertEquals(ExecutionStatus.FAILED, rejected.status());
        assertEquals("EXECUTION_SCHEDULER_CLOSED", rejected.errorCode());
    }

    @Test
    void concurrent_schedule_and_close_always_returns_a_terminal_handle() throws Exception {
        ExecutionPlan plan = compile(
                List.of(node("race-start", "builtin.start", "raceStart", Map.of()),
                        node("race-end", "builtin.end", "raceEnd", Map.of("resultStatus", "SUCCESS"))),
                List.of(edge("race-edge", "race-start", "race-end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        for (int iteration = 0; iteration < 50; iteration++) {
            DagScheduler scheduler = new DagScheduler(
                    NodeExecutorRegistry.builtIns(), ConcurrencyGovernor.defaults(), Clock.systemUTC(), 4, 1);
            CountDownLatch startRace = new CountDownLatch(1);
            CompletableFuture<ExecutionHandle> scheduled = CompletableFuture.supplyAsync(() -> {
                awaitLatch(startRace);
                return scheduler.schedule(plan, request(Map.of()));
            });
            CompletableFuture<Void> closed = CompletableFuture.runAsync(() -> {
                awaitLatch(startRace);
                scheduler.close();
            });

            startRace.countDown();
            ExecutionResult result = await(scheduled.get(2, TimeUnit.SECONDS));
            closed.get(2, TimeUnit.SECONDS);

            assertTrue(result.status() == ExecutionStatus.SUCCEEDED
                    || result.status() == ExecutionStatus.CANCELLED
                    || result.status() == ExecutionStatus.FAILED);
            if (result.status() == ExecutionStatus.FAILED) {
                assertEquals("EXECUTION_SCHEDULER_CLOSED", result.errorCode());
            }
        }
    }

    @Test
    void cancellation_fans_out_past_a_failing_callback_and_closed_registrations_are_removed() {
        CancellationHandle cancellation = new CancellationHandle();
        AtomicBoolean removedCallback = new AtomicBoolean();
        AtomicBoolean laterCallback = new AtomicBoolean();
        CancellationHandle.CancellationRegistration removed =
                cancellation.onCancellation(() -> removedCallback.set(true));
        removed.close();
        cancellation.onCancellation(() -> {
            throw new IllegalStateException("adapter cancellation failed");
        });
        cancellation.onCancellation(() -> laterCallback.set(true));

        assertTrue(cancellation.cancel());
        assertEquals(false, removedCallback.get());
        assertTrue(laterCallback.get());
    }

    @Test
    void failed_resource_acquisition_releases_every_previously_acquired_hierarchy_level() {
        ConcurrencyGovernor governor = new ConcurrencyGovernor(2, 2, 2, 2, 1, Clock.systemUTC());
        ConcurrencyGovernor.PermitRequest shared = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow", "component", "shared-resource");
        ConcurrencyGovernor.PermitRequest other = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow", "component", "other-resource");
        CancellationHandle neverCancelled = new CancellationHandle();

        try (ConcurrencyGovernor.Permit ignored = governor.acquire(shared, neverCancelled,
                Instant.now().plus(2, ChronoUnit.SECONDS))) {
            assertThrows(ExecutionDeadlineExceededException.class,
                    () -> governor.acquire(shared, neverCancelled,
                            Instant.now().plus(75, ChronoUnit.MILLIS)));

            try (ConcurrencyGovernor.Permit acquiredAfterCleanup = governor.acquire(other, neverCancelled,
                    Instant.now().plus(1, ChronoUnit.SECONDS))) {
                assertTrue(acquiredAfterCleanup.acquired());
            }
        }
    }

    @Test
    void component_contract_max_concurrency_is_enforced_without_blocking_other_components() {
        ConcurrencyGovernor governor = new ConcurrencyGovernor(4, 4, 4, 4, 4, Clock.systemUTC());
        CancellationHandle neverCancelled = new CancellationHandle();
        ConcurrencyGovernor.PermitRequest limited = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow", "limited", "resource-1", 1);
        ConcurrencyGovernor.PermitRequest sameComponent = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow", "limited", "resource-2", 1);
        ConcurrencyGovernor.PermitRequest otherComponent = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow", "other", "resource-3", 1);

        try (ConcurrencyGovernor.Permit ignored = governor.acquire(limited, neverCancelled,
                Instant.now().plus(2, ChronoUnit.SECONDS))) {
            assertThrows(ExecutionDeadlineExceededException.class,
                    () -> governor.acquire(sameComponent, neverCancelled,
                            Instant.now().plus(75, ChronoUnit.MILLIS)));
            try (ConcurrencyGovernor.Permit acquired = governor.acquire(otherComponent, neverCancelled,
                    Instant.now().plus(1, ChronoUnit.SECONDS))) {
                assertTrue(acquired.acquired());
            }
        }
    }

    @Test
    void overlapping_bundle_versions_use_the_safer_component_limit_without_configuration_failure() {
        ConcurrencyGovernor governor = new ConcurrencyGovernor(4, 4, 4, 4, 4, Clock.systemUTC());
        CancellationHandle neverCancelled = new CancellationHandle();
        ConcurrencyGovernor.PermitRequest older = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow-v1", "shared", "resource-1", 2);
        ConcurrencyGovernor.PermitRequest newer = new ConcurrencyGovernor.PermitRequest(
                "tenant", "flow-v2", "shared", "resource-2", 1);

        try (ConcurrencyGovernor.Permit ignored = governor.acquire(older, neverCancelled,
                Instant.now().plus(1, ChronoUnit.SECONDS))) {
            assertThrows(ExecutionDeadlineExceededException.class,
                    () -> governor.acquire(newer, neverCancelled,
                            Instant.now().plus(75, ChronoUnit.MILLIS)));
        }
        try (ConcurrencyGovernor.Permit acquired = governor.acquire(newer, neverCancelled,
                Instant.now().plus(1, ChronoUnit.SECONDS))) {
            assertTrue(acquired.acquired());
        }
    }

    @Test
    void execution_admission_rejects_excess_controllers_without_creating_waiting_tasks() throws Exception {
        BlockingExecutor blocker = new BlockingExecutor();
        ExecutionPlan plan = blockingPlan();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(blocker));

        try (DagScheduler scheduler = new DagScheduler(
                registry, ConcurrencyGovernor.defaults(), Clock.systemUTC(), 16, 1)) {
            ExecutionHandle first = scheduler.schedule(plan, request(Map.of()));
            assertTrue(blocker.started.await(2, TimeUnit.SECONDS));

            ExecutionResult excess = await(scheduler.schedule(plan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, excess.status());
            assertEquals("EXECUTION_CAPACITY_EXCEEDED", excess.errorCode());

            assertTrue(first.cancel());
            assertEquals(ExecutionStatus.CANCELLED, await(first).status());
        }
    }

    @Test
    void scheduler_admits_only_the_configured_number_of_node_tasks() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        NodeExecutor measuring = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                int running = active.incrementAndGet();
                maximum.accumulateAndGet(running, Math::max);
                try {
                    Thread.sleep(40);
                    return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(exception);
                } finally {
                    active.decrementAndGet();
                }
            }
        };
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode first = node("first", "test.first", "first", Map.of());
        FlowNode second = node("second", "test.second", "second", Map.of());
        FlowNode third = node("third", "test.third", "third", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, first, second, third, end), List.of(
                edge("e1", "start", "first", "NEXT", "IN"),
                edge("e2", "start", "second", "NEXT", "IN"),
                edge("e3", "start", "third", "NEXT", "IN"),
                edge("e4", "first", "end", "NEXT", "IN"),
                edge("e5", "second", "end", "NEXT", "IN"),
                edge("e6", "third", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "test.first", contract("test.first", ComponentType.SCRIPT, Map.of(), Map.of()),
                        "test.second", contract("test.second", ComponentType.SCRIPT, Map.of(), Map.of()),
                        "test.third", contract("test.third", ComponentType.SCRIPT, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(measuring));

        try (DagScheduler scheduler = new DagScheduler(
                registry, ConcurrencyGovernor.defaults(), Clock.systemUTC(), 1)) {
            assertEquals(ExecutionStatus.SUCCEEDED, await(scheduler.schedule(plan, request(Map.of()))).status());
            assertEquals(1, maximum.get());
        }
    }

    @Test
    void node_admission_waits_for_both_source_completion_and_callable_exit() throws Exception {
        PostCompletionBlockingExecutor blocking = new PostCompletionBlockingExecutor();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(blocking));
        ExecutionPlan firstPlan = twoInvocationPlan("a", "test.post-completion-a");
        ExecutionPlan secondPlan = twoInvocationPlan("b", "test.post-completion-b");

        try (DagScheduler scheduler = new DagScheduler(
                registry, new ConcurrencyGovernor(8, 8, 8, 8, 8, Clock.systemUTC()),
                Clock.systemUTC(), 1, 2)) {
            ExecutionHandle first = scheduler.schedule(firstPlan, request(Map.of()));
            assertTrue(blocking.sourceCallbackCompleted.await(2, TimeUnit.SECONDS));

            ExecutionHandle second = scheduler.schedule(secondPlan, request(Map.of()));
            boolean enteredWhileFirstCallableStillRunning =
                    blocking.secondEntered.await(200, TimeUnit.MILLISECONDS);
            blocking.allowFirstCallableExit.countDown();

            assertFalse(enteredWhileFirstCallableStillRunning);
            assertEquals(ExecutionStatus.SUCCEEDED, await(first).status());
            assertEquals(ExecutionStatus.SUCCEEDED, await(second).status());
            assertTrue(blocking.secondEntered.await(2, TimeUnit.SECONDS));
        } finally {
            blocking.allowFirstCallableExit.countDown();
        }
    }

    @Test
    void non_future_stage_retains_component_capacity_after_end_until_the_source_really_completes()
            throws Exception {
        DeferredStageExecutor deferred = new DeferredStageExecutor();
        AtomicInteger lateSideEffects = new AtomicInteger();
        NodeExecutor gate = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.BUSINESS;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                try {
                    if (!deferred.firstEntered.await(2, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("deferred sibling did not start");
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(exception);
                }
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
            }
        };
        NodeExecutor lateSideEffect = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.HTTP;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                lateSideEffects.incrementAndGet();
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
            }
        };
        Map<String, CompiledComponentContract> contracts = Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                "test.deferred", contract("test.deferred", ComponentType.SCRIPT, Map.of(), Map.of(), 1),
                "test.gate", new CompiledComponentContract(
                        "test.gate", ComponentType.BUSINESS, "gateExecutor", Map.of(), Map.of(), Map.of(),
                        new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 16)),
                "test.late-side-effect", contract(
                        "test.late-side-effect", ComponentType.HTTP, Map.of(), Map.of()),
                "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of()));
        FlowNode firstStart = node("start", "builtin.start", "start", Map.of());
        FlowNode firstDeferred = node("deferred", "test.deferred", "deferred", Map.of());
        FlowNode firstGate = node("gate", "test.gate", "gate", Map.of());
        FlowNode firstLate = node("late", "test.late-side-effect", "late", Map.of());
        FlowNode firstEnd = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan terminalSiblingPlan = compile(
                List.of(firstStart, firstDeferred, firstGate, firstLate, firstEnd), List.of(
                        edge("e1", "start", "deferred", "NEXT", "IN"),
                        edge("e2", "start", "gate", "NEXT", "IN"),
                        edge("e3", "gate", "end", "NEXT", "IN"),
                        edge("e-late", "deferred", "late", "NEXT", "IN")), contracts);
        FlowNode secondStart = node("start-2", "builtin.start", "start2", Map.of());
        FlowNode secondDeferred = node("deferred-2", "test.deferred", "deferred2", Map.of());
        FlowNode secondEnd = node("end-2", "builtin.end", "end2", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan linearPlan = compile(List.of(secondStart, secondDeferred, secondEnd), List.of(
                edge("e4", "start-2", "deferred-2", "NEXT", "IN"),
                edge("e5", "deferred-2", "end-2", "NEXT", "IN")), contracts);
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(deferred, gate, lateSideEffect));
        ConcurrencyGovernor governor = new ConcurrencyGovernor(16, 16, 16, 16, 16, Clock.systemUTC());

        try (DagScheduler scheduler = new DagScheduler(registry, governor, Clock.systemUTC(), 16, 16)) {
            ExecutionResult firstResult = await(scheduler.schedule(terminalSiblingPlan, request(Map.of())));
            assertEquals(ExecutionStatus.SUCCEEDED, firstResult.status());
            assertTrue(deferred.firstToken.isCancellationRequested());

            ExecutionHandle second = scheduler.schedule(linearPlan, request(Map.of()));
            boolean enteredBeforeSourceCompletion = deferred.secondEntered.await(200, TimeUnit.MILLISECONDS);
            deferred.firstCompletion.complete(
                    NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));

            assertFalse(enteredBeforeSourceCompletion);
            assertEquals(ExecutionStatus.SUCCEEDED, await(second).status());
            assertTrue(deferred.secondEntered.await(2, TimeUnit.SECONDS));
            assertEquals(0, lateSideEffects.get());
        }
    }

    @Test
    void non_future_stage_retains_node_admission_after_cancellation_until_source_completion()
            throws Exception {
        DeferredStageExecutor deferred = new DeferredStageExecutor();
        Map<String, CompiledComponentContract> contracts = Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                "test.first-deferred", contract(
                        "test.first-deferred", ComponentType.SCRIPT, Map.of(), Map.of(), 8),
                "test.second-deferred", contract(
                        "test.second-deferred", ComponentType.SCRIPT, Map.of(), Map.of(), 8),
                "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of()));
        ExecutionPlan firstPlan = compile(List.of(
                        node("start-a", "builtin.start", "startA", Map.of()),
                        node("deferred-a", "test.first-deferred", "deferredA", Map.of()),
                        node("end-a", "builtin.end", "endA", Map.of("resultStatus", "SUCCESS"))),
                List.of(edge("e1", "start-a", "deferred-a", "NEXT", "IN"),
                        edge("e2", "deferred-a", "end-a", "NEXT", "IN")), contracts);
        ExecutionPlan secondPlan = compile(List.of(
                        node("start-b", "builtin.start", "startB", Map.of()),
                        node("deferred-b", "test.second-deferred", "deferredB", Map.of()),
                        node("end-b", "builtin.end", "endB", Map.of("resultStatus", "SUCCESS"))),
                List.of(edge("e3", "start-b", "deferred-b", "NEXT", "IN"),
                        edge("e4", "deferred-b", "end-b", "NEXT", "IN")), contracts);
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(deferred));

        try (DagScheduler scheduler = new DagScheduler(
                registry, ConcurrencyGovernor.defaults(), Clock.systemUTC(), 1, 16)) {
            ExecutionHandle first = scheduler.schedule(firstPlan, request(Map.of()));
            assertTrue(deferred.firstEntered.await(2, TimeUnit.SECONDS));
            assertTrue(first.cancel());
            assertEquals(ExecutionStatus.CANCELLED, await(first).status());

            ExecutionHandle second = scheduler.schedule(secondPlan, request(Map.of()));
            boolean enteredBeforeSourceCompletion = deferred.secondEntered.await(200, TimeUnit.MILLISECONDS);
            deferred.firstCompletion.complete(
                    NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));

            assertFalse(enteredBeforeSourceCompletion);
            assertEquals(ExecutionStatus.SUCCEEDED, await(second).status());
            assertTrue(deferred.secondEntered.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void non_future_stage_retains_component_capacity_after_deadline_until_source_completion()
            throws Exception {
        DeferredStageExecutor deferred = new DeferredStageExecutor();
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode delayed = node("delayed", "test.deadline-deferred", "delayed", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, delayed, end), List.of(
                edge("e1", "start", "delayed", "NEXT", "IN"),
                edge("e2", "delayed", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "test.deadline-deferred", contract(
                                "test.deadline-deferred", ComponentType.SCRIPT, Map.of(), Map.of(), 1),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(deferred));

        try (DagScheduler scheduler = new DagScheduler(
                registry, ConcurrencyGovernor.defaults(), Clock.systemUTC(), 16, 16)) {
            ExecutionResult timedOut = await(scheduler.schedule(
                    plan, request(Map.of(), Instant.now().plus(150, ChronoUnit.MILLIS))));
            assertEquals(ExecutionStatus.FAILED, timedOut.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", timedOut.errorCode());
            assertTrue(deferred.firstToken.isCancellationRequested());

            ExecutionHandle second = scheduler.schedule(plan, request(Map.of()));
            boolean enteredBeforeSourceCompletion = deferred.secondEntered.await(200, TimeUnit.MILLISECONDS);
            deferred.firstCompletion.complete(
                    NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));

            assertFalse(enteredBeforeSourceCompletion);
            assertEquals(ExecutionStatus.SUCCEEDED, await(second).status());
            assertTrue(deferred.secondEntered.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void spontaneous_source_stage_cancellation_is_a_node_failure_and_cancels_siblings() throws Exception {
        CountDownLatch siblingStarted = new CountDownLatch(1);
        CountDownLatch siblingObservedCancellation = new CountDownLatch(1);
        NodeExecutor executor = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                if (context.node().node().nodeCode().equals("cancelledSource")) {
                    try {
                        if (!siblingStarted.await(2, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("sibling did not start");
                        }
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(exception);
                    }
                    CompletableFuture<NodeResult> cancelled = new CompletableFuture<>();
                    cancelled.cancel(false);
                    return new NodeInvocation(cancelled, ComponentInvocation.emptyContent());
                }
                siblingStarted.countDown();
                try {
                    while (true) {
                        context.throwIfStopped();
                        Thread.sleep(25);
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    context.throwIfStopped();
                    throw new IllegalStateException(exception);
                } finally {
                    if (context.cancellationToken().isCancellationRequested()) {
                        siblingObservedCancellation.countDown();
                    }
                }
            }
        };
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode cancelled = node("cancelled", "test.cancelled", "cancelledSource", Map.of());
        FlowNode sibling = node("sibling", "test.cancelled", "sibling", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, cancelled, sibling, end), List.of(
                edge("e1", "start", "cancelled", "NEXT", "IN"),
                edge("e2", "start", "sibling", "NEXT", "IN"),
                edge("e3", "cancelled", "end", "NEXT", "IN"),
                edge("e4", "sibling", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "test.cancelled", contract("test.cancelled", ComponentType.SCRIPT, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(executor));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("NODE_COMPLETION_CANCELLED", result.errorCode());
            assertEquals("node completion was cancelled", result.errorMessage());
            assertTrue(siblingObservedCancellation.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void default_runtime_resources_enforce_component_limits_across_engine_instances() throws Exception {
        SharedLimitExecutor shared = new SharedLimitExecutor();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(shared));
        ExecutionPlan plan = sharedLimitPlan();

        try (FlowEngine firstEngine = new FlowEngine(registry);
                FlowEngine secondEngine = new FlowEngine(registry)) {
            ExecutionHandle first = firstEngine.execute(plan, request(Map.of()));
            assertTrue(shared.firstEntered.await(2, TimeUnit.SECONDS));
            ExecutionHandle second = secondEngine.execute(plan, request(Map.of()));
            boolean concurrentEntry = shared.secondEntered.await(200, TimeUnit.MILLISECONDS);
            shared.releaseFirst.countDown();

            assertFalse(concurrentEntry);
            assertEquals(ExecutionStatus.SUCCEEDED, await(first).status());
            assertEquals(ExecutionStatus.SUCCEEDED, await(second).status());
            assertTrue(shared.secondEntered.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void default_scheduler_governor_is_shared_across_instances() throws Exception {
        SharedLimitExecutor shared = new SharedLimitExecutor();
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(shared));
        ExecutionPlan plan = sharedLimitPlan();

        try (DagScheduler firstScheduler = new DagScheduler(
                        registry, ConcurrencyGovernor.defaults(), Clock.systemUTC());
                DagScheduler secondScheduler = new DagScheduler(
                        registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle first = firstScheduler.schedule(plan, request(Map.of()));
            assertTrue(shared.firstEntered.await(2, TimeUnit.SECONDS));
            ExecutionHandle second = secondScheduler.schedule(plan, request(Map.of()));
            boolean concurrentEntry = shared.secondEntered.await(200, TimeUnit.MILLISECONDS);
            shared.releaseFirst.countDown();

            assertFalse(concurrentEntry);
            assertEquals(ExecutionStatus.SUCCEEDED, await(first).status());
            assertEquals(ExecutionStatus.SUCCEEDED, await(second).status());
            assertTrue(shared.secondEntered.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void default_engines_share_the_process_execution_admission_limit() throws Exception {
        HoldingExecutor holding = new HoldingExecutor(0);
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(holding));
        ExecutionPlan plan = blockingPlan();
        List<ExecutionHandle> admitted = new ArrayList<>(1024);

        try (FlowEngine firstEngine = new FlowEngine(registry);
                FlowEngine secondEngine = new FlowEngine(registry)) {
            for (int index = 0; index < 1024; index++) {
                FlowEngine engine = index % 2 == 0 ? firstEngine : secondEngine;
                admitted.add(engine.execute(plan, request(Map.of())));
            }

            ExecutionResult excess = await(firstEngine.execute(plan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, excess.status());
            assertEquals("EXECUTION_CAPACITY_EXCEEDED", excess.errorCode());

            admitted.forEach(ExecutionHandle::cancel);
            for (ExecutionHandle handle : admitted) {
                assertEquals(ExecutionStatus.CANCELLED, await(handle).status());
            }
        }
    }

    @Test
    void default_engine_node_admission_is_bounded_across_instances() throws Exception {
        HoldingExecutor holding = new HoldingExecutor(1024);
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(holding));
        ExecutionPlan plan = wideHoldingPlan(600);
        ConcurrencyGovernor firstGovernor = new ConcurrencyGovernor(
                2048, 2048, 2048, 2048, 2048, Clock.systemUTC());
        ConcurrencyGovernor secondGovernor = new ConcurrencyGovernor(
                2048, 2048, 2048, 2048, 2048, Clock.systemUTC());

        try (FlowEngine firstEngine = new FlowEngine(registry, firstGovernor, Clock.systemUTC());
                FlowEngine secondEngine = new FlowEngine(registry, secondGovernor, Clock.systemUTC())) {
            ExecutionHandle first = firstEngine.execute(plan, request(Map.of()));
            ExecutionHandle second = secondEngine.execute(plan, request(Map.of()));
            assertTrue(holding.expectedEntries.await(5, TimeUnit.SECONDS));
            Thread.sleep(200);

            assertEquals(1024, holding.entered.get());
            assertTrue(first.cancel());
            assertTrue(second.cancel());
            assertEquals(ExecutionStatus.CANCELLED, await(first).status());
            assertEquals(ExecutionStatus.CANCELLED, await(second).status());
        }
    }

    private static ExecutionPlan sharedLimitPlan() {
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode limited = node("limited", "test.shared-limit", "limited", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        return compile(List.of(start, limited, end), List.of(
                edge("e1", "start", "limited", "NEXT", "IN"),
                edge("e2", "limited", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "test.shared-limit", contract(
                                "test.shared-limit", ComponentType.SCRIPT, Map.of(), Map.of(), 1),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
    }

    private static ExecutionPlan closeAwarePlan() {
        return twoInvocationPlan("close", "test.close-aware");
    }

    private static ExecutionPlan twoInvocationPlan(String suffix, String componentCode) {
        FlowNode start = node("start-" + suffix, "builtin.start", "start" + suffix, Map.of());
        FlowNode work = node("work-" + suffix, componentCode, "work" + suffix, Map.of());
        FlowNode end = node("end-" + suffix, "builtin.end", "end" + suffix,
                Map.of("resultStatus", "SUCCESS"));
        return compile(List.of(start, work, end), List.of(
                edge("first-" + suffix, start.id(), work.id(), "NEXT", "IN"),
                edge("last-" + suffix, work.id(), end.id(), "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        componentCode, contract(componentCode, ComponentType.SCRIPT, Map.of(), Map.of(), 1),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
    }

    private static ExecutionPlan wideHoldingPlan(int width) {
        List<FlowNode> nodes = new ArrayList<>(width + 2);
        List<FlowEdge> edges = new ArrayList<>(width * 2);
        FlowNode start = node("wide-start", "builtin.start", "wideStart", Map.of());
        FlowNode end = node("wide-end", "builtin.end", "wideEnd", Map.of("resultStatus", "SUCCESS"));
        nodes.add(start);
        for (int index = 0; index < width; index++) {
            String nodeId = "hold-" + index;
            nodes.add(node(nodeId, "test.node-admission", nodeId, Map.of()));
            edges.add(edge("start-" + index, start.id(), nodeId, "NEXT", "IN"));
            edges.add(edge("end-" + index, nodeId, end.id(), "NEXT", "IN"));
        }
        nodes.add(end);
        return compile(nodes, edges, Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                "test.node-admission", contract(
                        "test.node-admission", ComponentType.SCRIPT, Map.of(), Map.of(), 2048),
                "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
    }

    private static ExecutionPlan blockingPlan() {
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode blocking = node("blocking", "test.blocking", "blocking", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        List<FlowEdge> edges = List.of(
                edge("e1", "start", "blocking", "NEXT", "IN"),
                edge("e2", "blocking", "end", "NEXT", "IN"));
        Map<String, CompiledComponentContract> contracts = Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                "test.blocking", contract("test.blocking", ComponentType.SCRIPT, Map.of(), Map.of()),
                "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of()));
        return compile(List.of(start, blocking, end), edges, contracts);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    private static final class BlockingExecutor implements NodeExecutor {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch observedCancellation = new CountDownLatch(1);
        private final AtomicBoolean virtualThread = new AtomicBoolean();

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            virtualThread.set(Thread.currentThread().isVirtual());
            started.countDown();
            try {
                while (true) {
                    context.throwIfStopped();
                    Thread.sleep(25);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                if (context.cancellationToken().isCancellationRequested()) {
                    observedCancellation.countDown();
                }
                context.throwIfStopped();
                throw new IllegalStateException("interrupted without cancellation", exception);
            } finally {
                if (context.cancellationToken().isCancellationRequested()) {
                    observedCancellation.countDown();
                }
            }
        }
    }

    private static final class AsyncExecutor implements NodeExecutor {

        private final CountDownLatch started = new CountDownLatch(1);
        private final NonConvertibleFuture<NodeResult> completion = new NonConvertibleFuture<>();

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            started.countDown();
            return new NodeInvocation(completion,
                    com.playrewards.flowengine.spi.ComponentInvocation.emptyContent());
        }
    }

    private static final class CloseAwareStageExecutor implements NodeExecutor {

        private final AtomicInteger invocations = new AtomicInteger();
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch firstObservedCancellation = new CountDownLatch(1);
        private final CountDownLatch allowFirstSourceCompletion = new CountDownLatch(1);
        private final CountDownLatch firstSourceCompleted = new CountDownLatch(1);
        private final CountDownLatch secondEntered = new CountDownLatch(1);

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            if (invocations.incrementAndGet() != 1) {
                secondEntered.countDown();
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
            }
            firstEntered.countDown();
            CompletableFuture<NodeResult> source = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                long stopAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                try {
                    while (!context.cancellationToken().isCancellationRequested()
                            && System.nanoTime() < stopAt) {
                        Thread.sleep(10);
                    }
                    if (context.cancellationToken().isCancellationRequested()) {
                        firstObservedCancellation.countDown();
                    }
                    allowFirstSourceCompletion.await(3, TimeUnit.SECONDS);
                    source.complete(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    source.completeExceptionally(exception);
                } finally {
                    firstSourceCompleted.countDown();
                }
            });
            return new NodeInvocation(nonFutureStage(source), ComponentInvocation.emptyContent());
        }
    }

    private static final class PostCompletionBlockingExecutor implements NodeExecutor {

        private final AtomicInteger invocations = new AtomicInteger();
        private final CountDownLatch sourceCallbackCompleted = new CountDownLatch(1);
        private final CountDownLatch allowFirstCallableExit = new CountDownLatch(1);
        private final CountDownLatch secondEntered = new CountDownLatch(1);

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            if (invocations.incrementAndGet() != 1) {
                secondEntered.countDown();
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
            }
            NodeResult result = NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT"));
            return new NodeInvocation(callbackThenBlockStage(
                    result, sourceCallbackCompleted, allowFirstCallableExit), ComponentInvocation.emptyContent());
        }
    }

    private static final class DeferredStageExecutor implements NodeExecutor {

        private final AtomicInteger invocations = new AtomicInteger();
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch secondEntered = new CountDownLatch(1);
        private final CompletableFuture<NodeResult> firstCompletion = new CompletableFuture<>();
        private volatile CancellationToken firstToken;

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            if (invocations.incrementAndGet() == 1) {
                firstToken = context.cancellationToken();
                firstEntered.countDown();
                CompletionStage<NodeResult> stage = nonFutureStage(firstCompletion);
                assertFalse(stage instanceof Future<?>);
                return new NodeInvocation(stage, ComponentInvocation.emptyContent());
            }
            secondEntered.countDown();
            return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
        }
    }

    private static final class SharedLimitExecutor implements NodeExecutor {

        private final AtomicInteger invocations = new AtomicInteger();
        private final CountDownLatch firstEntered = new CountDownLatch(1);
        private final CountDownLatch secondEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            if (invocations.incrementAndGet() == 1) {
                firstEntered.countDown();
                try {
                    if (!releaseFirst.await(2, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("first execution was not released");
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(exception);
                }
            } else {
                secondEntered.countDown();
            }
            return NodeInvocation.completed(NodeResult.continueWith(Map.of(), java.util.Set.of("NEXT")));
        }
    }

    private static final class HoldingExecutor implements NodeExecutor {

        private final AtomicInteger entered = new AtomicInteger();
        private final CountDownLatch expectedEntries;

        private HoldingExecutor(int expectedEntries) {
            this.expectedEntries = new CountDownLatch(expectedEntries);
        }

        @Override
        public ComponentType supports() {
            return ComponentType.SCRIPT;
        }

        @Override
        public NodeInvocation execute(NodeExecutionContext context) {
            entered.incrementAndGet();
            expectedEntries.countDown();
            return new NodeInvocation(new CompletableFuture<>(), ComponentInvocation.emptyContent());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<T> nonFutureStage(CompletableFuture<T> delegate) {
        return (CompletionStage<T>) Proxy.newProxyInstance(
                CancellationAndDeadlineTest.class.getClassLoader(),
                new Class<?>[] {CompletionStage.class},
                (proxy, method, arguments) -> {
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<T> callbackThenBlockStage(
            T value, CountDownLatch callbackCompleted, CountDownLatch allowReturn) {
        CompletableFuture<T> delegate = CompletableFuture.completedFuture(value);
        return (CompletionStage<T>) Proxy.newProxyInstance(
                CancellationAndDeadlineTest.class.getClassLoader(),
                new Class<?>[] {CompletionStage.class},
                (proxy, method, arguments) -> {
                    try {
                        Object returned = method.invoke(delegate, arguments);
                        if ("whenComplete".equals(method.getName())) {
                            callbackCompleted.countDown();
                            if (!allowReturn.await(2, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("node callable was not released");
                            }
                        }
                        return returned;
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }

    /** Legal CompletionStage whose optional conversion method is deliberately unavailable. */
    private static final class NonConvertibleFuture<T> extends CompletableFuture<T> {

        private final CountDownLatch cancelled = new CountDownLatch(1);

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            try {
                return super.cancel(mayInterruptIfRunning);
            } finally {
                cancelled.countDown();
            }
        }

        @Override
        public CompletableFuture<T> toCompletableFuture() {
            throw new UnsupportedOperationException("conversion is not supported");
        }
    }
}
