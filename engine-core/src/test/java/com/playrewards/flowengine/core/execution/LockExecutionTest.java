package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.await;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.edge;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.node;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.plan;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.registry;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.request;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.scripts;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.LockLease;
import com.playrewards.flowengine.spi.LockRequest;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Verifies LOCK acquisition, release, timeout and scheduler-owned exceptional cleanup. */
class LockExecutionTest {

    @Test
    void cancellation_waits_for_a_non_future_lock_key_source_to_clean_up() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        RecordingLocks locks = new RecordingLocks(true);
        try (FlowEngine engine = new FlowEngine(
                registry(successfulWork(), cancellationAwareLockKey(started, cancellationObserved, cleaned), locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(lockPlan(0, 1_000), request(Map.of()));
            assertTrue(started.await(2, TimeUnit.SECONDS));

            assertTrue(handle.cancel());
            assertTrue(cancellationObserved.await(2, TimeUnit.SECONDS));
            assertFalse(handle.completion().toCompletableFuture().isDone(),
                    "execution completion must remain pending while lock-key evaluation cleans up");
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.CANCELLED, result.status());
            assertEquals(0, cleaned.getCount());
            assertEquals(0, locks.acquires.get());
        }
    }

    @Test
    void deadline_waits_for_a_non_future_lock_key_source_to_clean_up() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        RecordingLocks locks = new RecordingLocks(true);
        try (FlowEngine engine = new FlowEngine(
                registry(successfulWork(), cancellationAwareLockKey(started, cancellationObserved, cleaned), locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(lockPlan(0, 1_000),
                    request(Map.of(), Map.of(), Instant.now().plusMillis(500)));
            assertTrue(started.await(2, TimeUnit.SECONDS));

            assertTrue(cancellationObserved.await(2, TimeUnit.SECONDS));
            assertFalse(handle.completion().toCompletableFuture().isDone(),
                    "execution completion must remain pending while deadline cleanup is running");
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
            assertEquals(0, cleaned.getCount());
            assertEquals(0, locks.acquires.get());
        }
    }

    @Test
    void lock_wait_does_not_hold_the_only_node_admission_needed_by_an_unlocking_branch() throws Exception {
        ToggleLocks locks = new ToggleLocks();
        NodeExecutor unlock = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                locks.available.set(true);
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
            }
        };
        try (DagScheduler scheduler = new DagScheduler(
                registry(unlock, scripts(invocation -> "customer-secret"), locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC(), 1, 2)) {
            ExecutionResult result = await(scheduler.schedule(lockWaitParallelPlan(), request(Map.of())));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status(), result::toString);
            assertTrue(locks.acquires.get() > 1);
        }
    }

    @Test
    void delimiter_bearing_tenant_and_flow_identities_cannot_collide_lock_names() throws Exception {
        RecordingLocks locks = new RecordingLocks(true);
        try (FlowEngine engine = engine(locks, successfulWork())) {
            ExecutionPlan firstPlan = withIdentity(lockPlan(0, 1_000), "a:flow:b", "c");
            ExecutionPlan secondPlan = withIdentity(lockPlan(0, 1_000), "a", "b:flow:c");

            assertEquals(ExecutionStatus.SUCCEEDED,
                    await(engine.execute(firstPlan, requestForTenant("a:flow:b"))).status());
            String firstName = locks.lastName;
            assertEquals(ExecutionStatus.SUCCEEDED,
                    await(engine.execute(secondPlan, requestForTenant("a"))).status());

            assertNotEquals(firstName, locks.lastName);
        }
    }

    @Test
    void close_publishes_cancellation_only_after_the_held_lease_cleanup_finishes() throws Exception {
        BlockingReleaseLocks locks = new BlockingReleaseLocks();
        CountDownLatch workStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        FlowEngine engine = new FlowEngine(
                registry(blockingWork(workStarted, never), scripts(invocation -> "customer-secret"), locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC());
        ExecutionHandle handle = engine.execute(lockPlan(0, 1_000), request(Map.of()));
        assertTrue(workStarted.await(2, TimeUnit.SECONDS));
        Thread closer = Thread.ofVirtual().start(engine::close);
        assertTrue(locks.releaseStarted.await(2, TimeUnit.SECONDS));
        try {
            assertFalse(handle.completion().toCompletableFuture().isDone());
            assertTrue(closer.isAlive());
        } finally {
            locks.allowRelease.countDown();
            closer.join(2_000);
            engine.close();
        }
        assertEquals(ExecutionStatus.CANCELLED, await(handle).status());
    }

    @Test
    void close_waits_for_cleanup_after_terminal_outcome_has_already_won() throws Exception {
        BlockingReleaseLocks locks = new BlockingReleaseLocks();
        FlowEngine engine = new FlowEngine(
                registry(successfulWork(), scripts(invocation -> "customer-secret"), locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC());
        ExecutionHandle handle = engine.execute(earlyEndPlan(), request(Map.of()));
        assertTrue(locks.releaseStarted.await(2, TimeUnit.SECONDS));
        Thread closer = Thread.ofVirtual().start(engine::close);
        try {
            closer.join(200);
            assertTrue(closer.isAlive(),
                    "close must await cleanup even when TERMINAL already won cancellation arbitration");
            assertFalse(handle.completion().toCompletableFuture().isDone());
        } finally {
            locks.allowRelease.countDown();
            closer.join(2_000);
            engine.close();
        }

        assertEquals(ExecutionStatus.SUCCEEDED, await(handle).status());
    }

    @Test
    void cleanup_failure_converts_an_early_success_to_a_safe_lock_failure() throws Exception {
        DistributedLockProvider locks = new DistributedLockProvider() {
            @Override
            public Optional<LockLease> tryAcquire(LockRequest request) {
                return Optional.of(new LockLease(request.lockName(), request.ownerId(), "lease",
                        Instant.now().plusSeconds(1), 1));
            }

            @Override
            public boolean release(LockLease lease) {
                return false;
            }
        };
        try (FlowEngine engine = new FlowEngine(
                registry(successfulWork(), scripts(invocation -> "customer-secret"), locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(earlyEndPlan(), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("LOCK_LEASE_CLEANUP_FAILED", result.errorCode());
        }
    }

    @Test
    void lock_key_evaluation_timeout_is_the_execution_deadline() throws Exception {
        RecordingLocks locks = new RecordingLocks(true);
        var never = new java.util.concurrent.CompletableFuture<Object>();
        try (FlowEngine engine = new FlowEngine(
                registry(successfulWork(), invocation -> never, locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(lockPlan(0, 1_000),
                    request(Map.of(), Map.of(), Instant.now().plusMillis(100))));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
        }
    }

    @Test
    void waiting_execution_acquires_only_after_the_first_owner_releases() throws Exception {
        RecordingLocks locks = new RecordingLocks(true);
        CountDownLatch firstWorkStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstWork = new CountDownLatch(1);
        FlowEngine first = engine(locks, blockingWork(firstWorkStarted, releaseFirstWork));
        FlowEngine second = engine(locks, successfulWork());
        try {
            ExecutionHandle firstHandle = first.execute(lockPlan(2_000, 1_000), request(Map.of()));
            assertTrue(firstWorkStarted.await(2, TimeUnit.SECONDS));
            ExecutionHandle secondHandle = second.execute(lockPlan(2_000, 1_000), request(Map.of()));
            awaitAttempts(locks, 2);
            assertTrue(!secondHandle.completion().toCompletableFuture().isDone());

            releaseFirstWork.countDown();
            assertEquals(ExecutionStatus.SUCCEEDED, await(firstHandle).status());
            assertEquals(ExecutionStatus.SUCCEEDED, await(secondHandle).status());
            assertEquals(2, locks.releases.get());
        } finally {
            releaseFirstWork.countDown();
            first.close();
            second.close();
        }
    }

    @Test
    void cancellation_and_deadline_stop_a_bounded_lock_wait() throws Exception {
        RecordingLocks locks = new RecordingLocks(false);
        try (FlowEngine engine = engine(locks, successfulWork())) {
            ExecutionHandle cancelled = engine.execute(lockPlan(2_000, 1_000), request(Map.of()));
            awaitAttempts(locks, 1);
            assertTrue(cancelled.cancel());
            assertEquals(ExecutionStatus.CANCELLED, await(cancelled).status());

            ExecutionResult deadline = await(engine.execute(lockPlan(2_000, 1_000),
                    request(Map.of(), Map.of(), Instant.now().plusMillis(100))));
            assertEquals(ExecutionStatus.FAILED, deadline.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", deadline.errorCode());
        }
    }

    @Test
    void early_end_and_engine_close_release_held_leases() throws Exception {
        RecordingLocks earlyLocks = new RecordingLocks(true);
        try (FlowEngine engine = engine(earlyLocks, successfulWork())) {
            ExecutionResult result = await(engine.execute(earlyEndPlan(), request(Map.of())));
            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(1, earlyLocks.releases.get());
        }

        RecordingLocks closeLocks = new RecordingLocks(true);
        CountDownLatch workStarted = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        FlowEngine closing = engine(closeLocks, blockingWork(workStarted, never));
        ExecutionHandle handle = closing.execute(lockPlan(0, 1_000), request(Map.of()));
        assertTrue(workStarted.await(2, TimeUnit.SECONDS));
        closing.close();
        assertEquals(ExecutionStatus.CANCELLED, await(handle).status());
        awaitReleases(closeLocks, 1);
    }

    @Test
    void expired_lease_fails_closed_at_the_paired_release() throws Exception {
        RecordingLocks locks = new RecordingLocks(true);
        NodeExecutor slowWork = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                }
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
            }
        };
        try (FlowEngine engine = engine(locks, slowWork)) {
            ExecutionResult result = await(engine.execute(lockPlan(0, 5), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("LOCK_LEASE_LOST", result.errorCode());
            assertEquals(1, locks.releases.get());
        }
    }

    @Test
    void acquires_and_releases_the_paired_lease_on_the_normal_path() throws Exception {
        RecordingLocks locks = new RecordingLocks(true);
        try (FlowEngine engine = engine(locks, successfulWork())) {
            ExecutionResult result = await(engine.execute(lockPlan(25, 1_000), request(Map.of())));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(1, locks.acquires.get());
            assertEquals(1, locks.releases.get());
            assertTrue(locks.lastName.startsWith("tenant:"));
            assertTrue(!locks.lastName.contains("customer-secret"));
        }
    }

    @Test
    void releases_an_acquired_lease_before_publishing_a_guarded_node_failure() throws Exception {
        RecordingLocks locks = new RecordingLocks(true);
        try (FlowEngine engine = engine(locks, failingWork())) {
            ExecutionResult result = await(engine.execute(lockPlan(25, 1_000), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("GUARDED_FAILURE", result.errorCode());
            assertEquals(1, locks.releases.get());
        }
    }

    @Test
    void finite_wait_returns_a_safe_timeout_without_running_the_guarded_body() throws Exception {
        RecordingLocks locks = new RecordingLocks(false);
        try (FlowEngine engine = engine(locks, successfulWork())) {
            ExecutionResult result = await(engine.execute(lockPlan(25, 1_000), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("LOCK_ACQUIRE_TIMEOUT", result.errorCode());
            assertEquals(0, locks.releases.get());
        }
    }

    private static FlowEngine engine(RecordingLocks locks, NodeExecutor work) {
        var runtime = scripts(invocation -> "customer-secret");
        return new FlowEngine(registry(work, runtime, locks),
                ConcurrencyGovernor.defaults(), Clock.systemUTC());
    }

    private static NodeExecutor successfulWork() {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
            }
        };
    }

    private static NodeExecutor failingWork() {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                throw new NodeExecutionException("GUARDED_FAILURE", "guarded failure");
            }
        };
    }

    private static NodeExecutor blockingWork(CountDownLatch started, CountDownLatch release) {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException("guarded work interrupted");
                }
                return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
            }
        };
    }

    private static ScriptRuntime cancellationAwareLockKey(CountDownLatch started,
            CountDownLatch cancellationObserved, CountDownLatch cleaned) {
        return invocation -> {
            CompletableFuture<Object> source = new CompletableFuture<>();
            started.countDown();
            Thread.ofVirtual().start(() -> {
                try {
                    while (!invocation.cancellationToken().isCancellationRequested()) {
                        Thread.sleep(5);
                    }
                    cancellationObserved.countDown();
                    Thread.sleep(100);
                    cleaned.countDown();
                    source.completeExceptionally(
                            new java.util.concurrent.CancellationException("lock key cancelled"));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    source.completeExceptionally(failure);
                }
            });
            return nonFutureStage(source);
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<T> nonFutureStage(CompletableFuture<T> delegate) {
        return (CompletionStage<T>) Proxy.newProxyInstance(
                LockExecutionTest.class.getClassLoader(),
                new Class<?>[]{CompletionStage.class},
                (proxy, method, arguments) -> {
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }

    private static ExecutionPlan lockPlan(long waitMillis, long leaseMillis) {
        CompiledNode start = node("start", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode acquire = node("acquire", ComponentType.LOCK, Map.of(
                "role", "ACQUIRE", "pairNodeId", "release", "lockKey", "customer-key",
                "waitMillis", waitMillis, "leaseMillis", leaseMillis), Map.of());
        CompiledNode work = node("work", ComponentType.SCRIPT, Map.of(), Map.of());
        CompiledNode release = node("release", ComponentType.LOCK,
                Map.of("role", "RELEASE", "pairNodeId", "acquire"), Map.of());
        CompiledNode end = node("end", ComponentType.END, Map.of("resultStatus", "SUCCESS"), Map.of());
        return plan("lock-flow", List.of(start, acquire, work, release, end), List.of(
                edge("start", "acquire", "NEXT", "IN"), edge("acquire", "work", "NEXT", "IN"),
                edge("work", "release", "NEXT", "IN"), edge("release", "end", "NEXT", "IN")),
                Map.of(), Map.of("acquire", "release", "release", "acquire"), Map.of());
    }

    private static ExecutionPlan lockWaitParallelPlan() {
        CompiledNode start = node("start", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode parallel = node("parallel", ComponentType.PARALLEL, Map.of(), Map.of());
        CompiledNode acquire = node("acquire", ComponentType.LOCK, Map.of(
                "role", "ACQUIRE", "pairNodeId", "release", "lockKey", "customer-key",
                "waitMillis", 500, "leaseMillis", 1_000), Map.of());
        CompiledNode unlock = node("unlock", ComponentType.SCRIPT, Map.of(), Map.of());
        CompiledNode release = node("release", ComponentType.LOCK,
                Map.of("role", "RELEASE", "pairNodeId", "acquire"), Map.of());
        CompiledNode join = node("join", ComponentType.SCRIPT, Map.of(), Map.of());
        CompiledNode end = node("end", ComponentType.END, Map.of("resultStatus", "SUCCESS"), Map.of());
        return plan("lock-wait-parallel", List.of(start, parallel, acquire, unlock, release, join, end), List.of(
                edge("start", "parallel", "NEXT", "IN"),
                edge("parallel", "acquire", "BRANCH", "IN"),
                edge("parallel", "unlock", "BRANCH", "IN"),
                edge("acquire", "release", "NEXT", "IN"),
                edge("release", "join", "NEXT", "IN"),
                edge("unlock", "join", "NEXT", "IN"),
                edge("join", "end", "NEXT", "IN")), Map.of(),
                Map.of("acquire", "release", "release", "acquire"), Map.of());
    }

    private static ExecutionPlan withIdentity(ExecutionPlan plan, String tenantId, String flowId) {
        return new ExecutionPlan(new FlowIdentity(tenantId, flowId, plan.flowIdentity().version()),
                plan.bundleHash(), plan.nodesById(), plan.forwardAdjacency(), plan.reverseAdjacency(),
                plan.outgoingByPort(), plan.incomingByPort(), plan.activationPredecessors(),
                plan.loopPairs(), plan.lockPairs(), plan.subFlowPlans());
    }

    private static com.playrewards.flowengine.contract.execution.ExecutionRequest requestForTenant(
            String tenantId) {
        var base = request(Map.of());
        return new com.playrewards.flowengine.contract.execution.ExecutionRequest(
                base.flowReference(), base.mode(), base.purpose(), base.startInputs(), base.externalScopes(),
                base.contextReference(), base.idempotencyKey(), tenantId, base.deadline(), base.metadata());
    }

    private static ExecutionPlan earlyEndPlan() {
        CompiledNode start = node("start", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode acquire = node("acquire", ComponentType.LOCK, Map.of(
                "role", "ACQUIRE", "pairNodeId", "release", "lockKey", "customer-key",
                "waitMillis", 0, "leaseMillis", 1_000), Map.of());
        CompiledNode end = node("end", ComponentType.END, Map.of("resultStatus", "SUCCESS"), Map.of());
        return plan("early-end-lock", List.of(start, acquire, end), List.of(
                edge("start", "acquire", "NEXT", "IN"), edge("acquire", "end", "NEXT", "IN")),
                Map.of(), Map.of("acquire", "release"), Map.of());
    }

    private static void awaitAttempts(RecordingLocks locks, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (locks.acquires.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(locks.acquires.get() >= expected);
    }

    private static void awaitReleases(RecordingLocks locks, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (locks.releases.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, locks.releases.get());
    }

    private static final class RecordingLocks implements DistributedLockProvider {
        private final boolean available;
        private final AtomicInteger acquires = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();
        private final AtomicReference<LockLease> active = new AtomicReference<>();
        private volatile String lastName;

        private RecordingLocks(boolean available) {
            this.available = available;
        }

        @Override
        public Optional<LockLease> tryAcquire(LockRequest request) {
            acquires.incrementAndGet();
            lastName = request.lockName();
            if (!available) {
                return Optional.empty();
            }
            LockLease lease = lease(request);
            return active.compareAndSet(null, lease) ? Optional.of(lease) : Optional.empty();
        }

        @Override
        public boolean release(LockLease lease) {
            releases.incrementAndGet();
            if (!lease.expiresAt().isAfter(Instant.now())) {
                active.compareAndSet(lease, null);
                return false;
            }
            return active.compareAndSet(lease, null);
        }

        private static LockLease lease(LockRequest request) {
            try {
                Constructor<LockLease> fenced = LockLease.class.getConstructor(
                        String.class, String.class, String.class, Instant.class, long.class);
                return fenced.newInstance(request.lockName(), request.ownerId(), UUID.randomUUID().toString(),
                        Instant.now().plus(request.leaseDuration()), 1L);
            } catch (ReflectiveOperationException failure) {
                throw new AssertionError(failure);
            }
        }
    }

    private static final class ToggleLocks implements DistributedLockProvider {
        private final AtomicBoolean available = new AtomicBoolean();
        private final AtomicInteger acquires = new AtomicInteger();
        private final AtomicReference<LockLease> active = new AtomicReference<>();

        @Override
        public Optional<LockLease> tryAcquire(LockRequest request) {
            acquires.incrementAndGet();
            if (!available.get()) {
                return Optional.empty();
            }
            LockLease lease = new LockLease(request.lockName(), request.ownerId(), UUID.randomUUID().toString(),
                    Instant.now().plus(request.leaseDuration()), acquires.get());
            return active.compareAndSet(null, lease) ? Optional.of(lease) : Optional.empty();
        }

        @Override
        public boolean release(LockLease lease) {
            return active.compareAndSet(lease, null);
        }
    }

    private static final class BlockingReleaseLocks implements DistributedLockProvider {
        private final CountDownLatch releaseStarted = new CountDownLatch(1);
        private final CountDownLatch allowRelease = new CountDownLatch(1);

        @Override
        public Optional<LockLease> tryAcquire(LockRequest request) {
            return Optional.of(new LockLease(request.lockName(), request.ownerId(), "lease",
                    Instant.now().plusSeconds(5), 1));
        }

        @Override
        public boolean release(LockLease lease) {
            releaseStarted.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    allowRelease.await();
                    break;
                } catch (InterruptedException failure) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return true;
        }
    }
}
