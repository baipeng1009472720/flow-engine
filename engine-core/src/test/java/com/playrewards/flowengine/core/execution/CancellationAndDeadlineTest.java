package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.await;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.compile;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.contract;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.edge;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.node;
import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
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
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
