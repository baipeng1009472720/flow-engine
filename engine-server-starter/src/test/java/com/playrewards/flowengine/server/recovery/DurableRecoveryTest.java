package com.playrewards.flowengine.server.recovery;

import com.playrewards.flowengine.server.TestMySql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.playrewards.flowengine.adapter.mysql.*;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.durable.DurableExecutionCoordinator;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.core.node.*;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * A new Spring context and scheduler resume from committed MySQL state, never observation events.
 */
class DurableRecoveryTest {
    static final TestMySql MYSQL = TestMySql.start();
    MySqlRuntimeDatabase db;

    @BeforeEach
    void setup() {
        var ds = new MysqlDataSource();
        ds.setURL(MYSQL.getJdbcUrl());
        ds.setUser(MYSQL.getUsername());
        ds.setPassword(MYSQL.getPassword());
        db = new MySqlRuntimeDatabase(ds);
        db.migrate();
    }

    @Test
    void resumesAfterThreeDistinctCommittedNodeCheckpointsWithoutRepeatingSideEffects()
            throws Exception {
        for (int stopAfter = 1; stopAfter <= 3; stopAfter++) {
            AtomicInteger calls = new AtomicInteger();
            var saved = new CountDownLatch(1);
            var parked = new CountDownLatch(1);
            int stop = stopAfter;
            AtomicInteger checkpoints = new AtomicInteger();
            String tenant = "tenant-" + UUID.randomUUID();
            var stores = stores(tenant);
            String id;
            try (var first =
                    context(
                            stores,
                            calls,
                            state -> {
                                if (state.state().containsKey("lastCompleted")
                                        && checkpoints.incrementAndGet() == stop) {
                                    saved.countDown();
                                    try {
                                        parked.await();
                                    } catch (InterruptedException ignored) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            })) {
                var handle = first.getBean(FlowEngine.class).execute(plan(tenant), request(tenant));
                id = handle.executionId();
                assertTrue(saved.await(15, TimeUnit.SECONDS));
            } finally {
                parked.countDown();
            }
            try (var second = context(stores, calls, state -> {})) {
                var handle = second.getBean(FlowEngine.class).resume(id, tenant);
                var result = handle.completion().toCompletableFuture().get(15, TimeUnit.SECONDS);
                assertEquals(ExecutionStatus.SUCCEEDED, result.status(), result.errorCode());
                assertEquals(2, calls.get(), "committed business invocations must not repeat");
                assertEquals(
                        ExecutionStatus.SUCCEEDED, stores.states().find(id).orElseThrow().status());
                var events = new MySqlObservationStore(db, tenant).readAfter(id, -1);
                assertEquals(
                        events.size(),
                        events.stream().map(ExecutionEvent::sequence).distinct().count());
                assertEquals(
                        1, events.stream().filter(e -> e.eventType().equals("CREATED")).count());
                assertEquals(
                        1,
                        events.stream()
                                .filter(
                                        e ->
                                                e.eventType().equals("COMPLETED")
                                                        && e.nodeId() == null)
                                .count());
            }
        }
    }

    private DurableStores stores(String tenant) {
        return new DurableStores(
                new MySqlExecutionStateStore(db, tenant),
                new MySqlLeaseRepository(db, tenant),
                new MySqlPayloadStore(db, tenant, 1048576, Duration.ofHours(1)));
    }

    @Test
    void readsLatestCheckpointAfterLeaseAcquisition() throws Exception {
        String tenant = "race-" + UUID.randomUUID();
        var original = stores(tenant);
        var first =
                new DurableExecutionCoordinator(
                        t -> original, new MySqlIdempotencyStore(db), Duration.ofSeconds(5));
        var session = first.begin(plan(tenant), request(tenant));
        String id = session.executionId();
        first.close();
        ExecutionLeaseRepository updatingLease =
                new ExecutionLeaseRepository() {
                    public Optional<ExecutionLease> acquire(
                            String executionId, String owner, Duration duration) {
                        var lease = original.leases().acquire(executionId, owner, duration);
                        lease.ifPresent(
                                value -> {
                                    var latest = original.states().find(executionId).orElseThrow();
                                    original.states().save(latest, latest.revision(), value);
                                });
                        return lease;
                    }

                    public boolean heartbeat(ExecutionLease lease, Duration duration) {
                        return original.leases().heartbeat(lease, duration);
                    }

                    public boolean release(ExecutionLease lease) {
                        return original.leases().release(lease);
                    }
                };
        var raced = new DurableStores(original.states(), updatingLease, original.payloads());
        try (var second = context(raced, new AtomicInteger(), state -> {})) {
            var result =
                    second.getBean(FlowEngine.class)
                            .resume(id, tenant)
                            .completion()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.SUCCEEDED, result.status(), result.errorCode());
        }
    }

    @Test
    void durableAutoConfigurationKeepsTheFullWorkloadPoolWarm() {
        new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withConfiguration(
                        org.springframework.boot.autoconfigure.AutoConfigurations.of(
                                com.playrewards.flowengine.server.config.DurableAutoConfiguration
                                        .class,
                                com.playrewards.flowengine.server.config.EngineAutoConfiguration
                                        .class))
                .withPropertyValues(
                        "flow-engine.durable.enabled=true",
                        "flow-engine.durable.jdbc-url=" + MYSQL.getJdbcUrl(),
                        "flow-engine.durable.username=" + MYSQL.getUsername(),
                        "flow-engine.durable.password=" + MYSQL.getPassword())
                .run(
                        context -> {
                            assertNull(context.getStartupFailure());
                            var source =
                                    context.getBean(
                                            org.apache.ibatis.datasource.pooled.PooledDataSource
                                                    .class);
                            assertEquals(16, source.getPoolMaximumActiveConnections());
                            assertEquals(16, source.getPoolMaximumIdleConnections());
                        });
    }

    @Test
    void starterRoutesAuthenticatedTenantsToDurableStoresAndPersistsAudit() {
        new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withConfiguration(
                        org.springframework.boot.autoconfigure.AutoConfigurations.of(
                                com.playrewards.flowengine.server.config.DurableAutoConfiguration
                                        .class,
                                com.playrewards.flowengine.server.config.EngineAutoConfiguration
                                        .class))
                .withBean(MySqlRuntimeDatabase.class, () -> db)
                .withPropertyValues("flow-engine.durable.enabled=true")
                .run(
                        context -> {
                            assertNull(context.getStartupFailure());
                            assertInstanceOf(
                                    MySqlTenantStoreRouter.class,
                                    context.getBean(ExecutionRecordStore.class));
                            FlowEngine engine = context.getBean(FlowEngine.class);
                            for (String tenant : List.of("starter-a", "starter-b")) {
                                var flow =
                                        graph(
                                                tenant,
                                                "simple",
                                                List.of(
                                                        simple(
                                                                "start",
                                                                ComponentType.START,
                                                                Map.of()),
                                                        simple(
                                                                "end",
                                                                ComponentType.END,
                                                                Map.of("resultStatus", "SUCCESS"))),
                                                List.<String[]>of(
                                                        new String[] {"start", "end", "NEXT"}),
                                                Map.of(),
                                                Map.of());
                                var handle = engine.execute(flow, request(tenant));
                                var result =
                                        handle.completion()
                                                .toCompletableFuture()
                                                .get(10, TimeUnit.SECONDS);
                                assertEquals(
                                        ExecutionStatus.SUCCEEDED,
                                        result.status(),
                                        result.errorCode());
                                var record =
                                        context.getBean(ExecutionRecordStore.class)
                                                .execution(handle.executionId())
                                                .orElseThrow();
                                assertEquals(tenant, record.flowIdentity().tenantId());
                                assertEquals(ExecutionStatus.SUCCEEDED, record.status());
                                assertTrue(
                                        new MySqlObservationStore(
                                                        db,
                                                        tenant.equals("starter-a")
                                                                ? "starter-b"
                                                                : "starter-a")
                                                .execution(handle.executionId())
                                                .isEmpty());
                            }
                        });
    }

    @Test
    void automaticRecoveryContinuesUnfinishedExecutionWithoutResumeCall() throws Exception {
        String tenant = "auto-" + UUID.randomUUID();
        var stores = stores(tenant);
        var paused = new CountDownLatch(1);
        var parked = new CountDownLatch(1);
        var flow =
                graph(
                        tenant,
                        "auto",
                        List.of(
                                simple("start", ComponentType.START, Map.of()),
                                simple(
                                        "end",
                                        ComponentType.END,
                                        Map.of("resultStatus", "SUCCESS"))),
                        List.<String[]>of(new String[] {"start", "end", "NEXT"}),
                        Map.of(),
                        Map.of());
        String id;
        try (var first =
                context(
                        stores,
                        new AtomicInteger(),
                        state -> {
                            if (state.state().containsKey("lastCompleted")) {
                                paused.countDown();
                                try {
                                    parked.await();
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        })) {
            var handle = first.getBean(FlowEngine.class).execute(flow, request(tenant));
            id = handle.executionId();
            assertTrue(paused.await(10, TimeUnit.SECONDS));
        } finally {
            parked.countDown();
        }
        new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withConfiguration(
                        org.springframework.boot.autoconfigure.AutoConfigurations.of(
                                com.playrewards.flowengine.server.config.DurableAutoConfiguration
                                        .class,
                                com.playrewards.flowengine.server.config.EngineAutoConfiguration
                                        .class))
                .withBean(MySqlRuntimeDatabase.class, () -> db)
                .withPropertyValues("flow-engine.durable.enabled=true")
                .run(
                        context -> {
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (stores.states().find(id).orElseThrow().status()
                                            != ExecutionStatus.SUCCEEDED
                                    && System.nanoTime() < deadline) Thread.sleep(30);
                            assertEquals(
                                    ExecutionStatus.SUCCEEDED,
                                    stores.states().find(id).orElseThrow().status());
                            assertEquals(
                                    1,
                                    new MySqlObservationStore(db, tenant)
                                            .readAfter(id, -1).stream()
                                                    .filter(e -> e.eventType().equals("CREATED"))
                                                    .count());
                        });
    }

    @Test
    void sameCompletedRequestReusesIdentityAndCommittedResult() throws Exception {
        String tenant = "dedupe-" + UUID.randomUUID();
        var stores = stores(tenant);
        var calls = new AtomicInteger();
        var request = request(tenant);
        try (var context = context(stores, calls, state -> {})) {
            var engine = context.getBean(FlowEngine.class);
            var first = engine.execute(plan(tenant), request);
            first.completion().toCompletableFuture().get(10, TimeUnit.SECONDS);
            var duplicate = engine.execute(plan(tenant), request);
            assertEquals(
                    ExecutionStatus.SUCCEEDED,
                    duplicate
                            .completion()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .status());
            assertEquals(first.executionId(), duplicate.executionId());
            assertEquals(2, calls.get());
            assertEquals(
                    1,
                    new MySqlObservationStore(db, tenant)
                            .readAfter(first.executionId(), -1).stream()
                                    .filter(
                                            e ->
                                                    e.eventType().equals("COMPLETED")
                                                            && e.nodeId() == null)
                                    .count());
        }
    }

    @Test
    void committedAuditRemainsTerminalWhenFinalCheckpointWriteFails() throws Exception {
        String tenant = "terminal-audit-" + UUID.randomUUID();
        var original = stores(tenant);
        var failed = new java.util.concurrent.atomic.AtomicBoolean();
        FencedExecutionStateStore unreliable =
                new FencedExecutionStateStore() {
                    public Optional<ExecutionState> find(String id) {
                        return original.states().find(id);
                    }

                    public ExecutionState save(
                            ExecutionState s, long revision, ExecutionLease lease) {
                        if (s.status() == ExecutionStatus.SUCCEEDED
                                && failed.compareAndSet(false, true))
                            throw new IllegalStateException("TEST_FINAL_CHECKPOINT_UNAVAILABLE");
                        return original.states().save(s, revision, lease);
                    }

                    public boolean transition(
                            String id,
                            long version,
                            ExecutionStatus from,
                            ExecutionStatus to,
                            Map<String, Object> state,
                            ExecutionLease lease) {
                        return original.states().transition(id, version, from, to, state, lease);
                    }

                    public List<ExecutionState> recoverable(int limit) {
                        return original.states().recoverable(limit);
                    }
                };
        String id;
        var calls = new AtomicInteger();
        try (var first =
                context(
                        new DurableStores(unreliable, original.leases(), original.payloads()),
                        calls,
                        state -> {})) {
            var handle = first.getBean(FlowEngine.class).execute(plan(tenant), request(tenant));
            id = handle.executionId();
            var result = handle.completion().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertTrue(failed.get());
            assertEquals(
                    ExecutionStatus.SUCCEEDED,
                    result.status(),
                    "public completion must match already committed audit");
            assertEquals(
                    result.status(),
                    new MySqlObservationStore(db, tenant).execution(id).orElseThrow().status());
        }
        try (var second = context(original, calls, state -> {})) {
            assertEquals(
                    ExecutionStatus.SUCCEEDED,
                    second.getBean(FlowEngine.class)
                            .resume(id, tenant)
                            .completion()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS)
                            .status());
            assertEquals(2, calls.get());
            assertEquals(
                    ExecutionStatus.SUCCEEDED, original.states().find(id).orElseThrow().status());
        }
    }

    @Test
    void resumedCriticalSectionReacquiresBusinessLockWithFreshFence() throws Exception {
        String tenant = "lock-" + UUID.randomUUID();
        var stores = stores(tenant);
        var calls = new AtomicInteger();
        var tokens = new java.util.concurrent.CopyOnWriteArrayList<Long>();
        var provider = new MySqlDistributedLockProvider(db);
        DistributedLockProvider traced =
                new DistributedLockProvider() {
                    public Optional<LockLease> tryAcquire(LockRequest request) {
                        var lease = provider.tryAcquire(request);
                        lease.ifPresent(l -> tokens.add(l.fencingToken()));
                        return lease;
                    }

                    public boolean release(LockLease lease) {
                        return provider.release(lease);
                    }
                };
        var base =
                graph(
                        tenant,
                        "locks",
                        List.of(
                                simple("start", ComponentType.START, Map.of()),
                                simple(
                                        "acquire",
                                        ComponentType.LOCK,
                                        Map.of(
                                                "role",
                                                "ACQUIRE",
                                                "pairNodeId",
                                                "release",
                                                "lockKey",
                                                "shared",
                                                "waitMillis",
                                                1000,
                                                "leaseMillis",
                                                5000)),
                                simple("a", ComponentType.BUSINESS, Map.of()),
                                simple(
                                        "release",
                                        ComponentType.LOCK,
                                        Map.of("role", "RELEASE", "pairNodeId", "acquire")),
                                simple(
                                        "end",
                                        ComponentType.END,
                                        Map.of("resultStatus", "SUCCESS"))),
                        List.of(
                                new String[] {"start", "acquire", "NEXT"},
                                new String[] {"acquire", "a", "NEXT"},
                                new String[] {"a", "release", "NEXT"},
                                new String[] {"release", "end", "NEXT"}),
                        Map.of(),
                        Map.of());
        var flow =
                new ExecutionPlan(
                        base.flowIdentity(),
                        base.bundleHash(),
                        base.nodesById(),
                        base.forwardAdjacency(),
                        base.reverseAdjacency(),
                        base.outgoingByPort(),
                        base.incomingByPort(),
                        base.activationPredecessors(),
                        Map.of(),
                        Map.of("acquire", "release", "release", "acquire"),
                        Map.of());
        NodeExecutor business =
                new NodeExecutor() {
                    public ComponentType supports() {
                        return ComponentType.BUSINESS;
                    }

                    public NodeInvocation execute(NodeExecutionContext c) {
                        calls.incrementAndGet();
                        return NodeInvocation.completed(
                                NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                    }
                };
        var registry =
                NodeExecutorRegistry.withBuiltIns(
                        List.of(
                                business,
                                new com.playrewards.flowengine.core.node.structure.LockNodeExecutor(
                                        i -> CompletableFuture.completedFuture("shared"),
                                        traced,
                                        Clock.systemUTC())));
        var paused = new CountDownLatch(1);
        var parked = new CountDownLatch(1);
        String id;
        var firstCoordinator =
                new DurableExecutionCoordinator(
                        t -> stores,
                        new MySqlIdempotencyStore(db),
                        Duration.ofSeconds(5),
                        state -> {
                            if (Objects.toString(state.state().get("lastCompleted"), "")
                                    .contains("/a/")) {
                                paused.countDown();
                                try {
                                    parked.await();
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        });
        try (var first =
                new FlowEngine(
                        registry,
                        ConcurrencyGovernor.defaults(),
                        Clock.systemUTC(),
                        recorder(),
                        firstCoordinator)) {
            var handle = first.execute(flow, request(tenant));
            id = handle.executionId();
            assertTrue(paused.await(10, TimeUnit.SECONDS));
        } finally {
            parked.countDown();
        }
        try (var second =
                new FlowEngine(
                        registry,
                        ConcurrencyGovernor.defaults(),
                        Clock.systemUTC(),
                        recorder(),
                        new DurableExecutionCoordinator(
                                t -> stores,
                                new MySqlIdempotencyStore(db),
                                Duration.ofSeconds(5)))) {
            var result =
                    second.resume(id, tenant)
                            .completion()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.SUCCEEDED, result.status(), result.errorCode());
            assertEquals(1, calls.get());
            assertEquals(2, tokens.size());
            assertTrue(tokens.getLast() > tokens.getFirst());
        }
    }

    @Test
    void restoresLoopFrameParallelDecisionsAndStableChildIdentity() throws Exception {
        String tenant = "structure-" + UUID.randomUUID();
        var stores = stores(tenant);
        var calls = new AtomicInteger();
        var collections = new AtomicInteger();
        var paused = new CountDownLatch(1);
        var parked = new CountDownLatch(1);
        var child =
                graph(
                        tenant,
                        "child",
                        List.of(
                                simple("start", ComponentType.START, Map.of()),
                                simple("parallel", ComponentType.PARALLEL, Map.of()),
                                valueNode("a", ComponentType.BUSINESS, Map.of()),
                                valueNode("b", ComponentType.BUSINESS, Map.of()),
                                simple(
                                        "end",
                                        ComponentType.END,
                                        Map.of(
                                                "resultStatus",
                                                "SUCCESS",
                                                "returnBinding",
                                                Map.of(
                                                        "sourceNodeId",
                                                        "a",
                                                        "sourceOutputKey",
                                                        "value")))),
                        List.of(
                                new String[] {"start", "parallel", "NEXT"},
                                new String[] {"parallel", "a", "BRANCH"},
                                new String[] {"parallel", "b", "BRANCH"},
                                new String[] {"a", "end", "NEXT"},
                                new String[] {"b", "end", "NEXT"}),
                        Map.of(),
                        Map.of());
        var loopConfig =
                Map.<String, Object>of(
                        "role",
                        "START",
                        "pairNodeId",
                        "loopEnd",
                        "collectionMode",
                        "GROOVY",
                        "collectionExpression",
                        "items",
                        "itemAlias",
                        "item",
                        "indexAlias",
                        "index",
                        "maxIterations",
                        10,
                        "resultBinding",
                        Map.of("sourceNodeId", "sub", "sourceOutputKey", "value"));
        var loop = simple("loop", ComponentType.LOOP, loopConfig);
        var any =
                new com.playrewards.flowengine.contract.protocol.ProtocolSchema(
                        "any", "1", Map.of());
        loop =
                new CompiledNode(
                        loop.node(),
                        new CompiledComponentContract(
                                loop.node().componentCode(),
                                ComponentType.LOOP,
                                null,
                                Map.of(),
                                Map.of(),
                                Map.of(
                                        "count",
                                        new OutputSlotContract("count", any),
                                        "results",
                                        new OutputSlotContract("results", any)),
                                loop.componentContract().capabilities()),
                        Map.of());
        var parent =
                graph(
                        tenant,
                        "parent",
                        List.of(
                                simple("start", ComponentType.START, Map.of()),
                                loop,
                                valueNode("sub", ComponentType.SUB_FLOW, Map.of()),
                                simple(
                                        "loopEnd",
                                        ComponentType.LOOP,
                                        Map.of("role", "END", "pairNodeId", "loop")),
                                simple(
                                        "end",
                                        ComponentType.END,
                                        Map.of(
                                                "resultStatus",
                                                "SUCCESS",
                                                "returnBinding",
                                                Map.of(
                                                        "sourceNodeId",
                                                        "loop",
                                                        "sourceOutputKey",
                                                        "results")))),
                        List.of(
                                new String[] {"start", "loop", "NEXT"},
                                new String[] {"loop", "sub", "BODY"},
                                new String[] {"sub", "loopEnd", "NEXT"},
                                new String[] {"loopEnd", "loop", "NEXT"},
                                new String[] {"loop", "end", "DONE"}),
                        Map.of("loop", "loopEnd", "loopEnd", "loop"),
                        Map.of("sub", child));
        String id;
        try (var first =
                structuredContext(
                        stores,
                        calls,
                        collections,
                        state -> {
                            String last = Objects.toString(state.state().get("lastCompleted"), "");
                            if (last.contains("/loopEnd/") && last.contains("index:0")) {
                                paused.countDown();
                                try {
                                    parked.await();
                                } catch (InterruptedException ignored) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        })) {
            var handle = first.getBean(FlowEngine.class).execute(parent, request(tenant));
            id = handle.executionId();
            assertTrue(paused.await(15, TimeUnit.SECONDS));
        } finally {
            parked.countDown();
        }
        try (var second = structuredContext(stores, calls, collections, state -> {})) {
            var handle = second.getBean(FlowEngine.class).resume(id, tenant);
            var result = handle.completion().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.SUCCEEDED, result.status(), result.errorCode());
            assertEquals(4, calls.get());
            assertEquals(1, collections.get());
            assertEquals(List.of(17, 17), result.result());
            assertEquals(2, handle.context().childExecutions().size());
            assertEquals(2, handle.context().required("loop", ScopePath.root(), "count"));
        }
    }

    private AnnotationConfigApplicationContext structuredContext(
            DurableStores stores,
            AtomicInteger calls,
            AtomicInteger collections,
            java.util.function.Consumer<ExecutionState> after) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(
                DurableExecutionCoordinator.class,
                () ->
                        new DurableExecutionCoordinator(
                                t -> stores,
                                new MySqlIdempotencyStore(db),
                                Duration.ofSeconds(5),
                                after));
        context.registerBean(
                FlowEngine.class,
                () -> {
                    NodeExecutor business =
                            new NodeExecutor() {
                                public ComponentType supports() {
                                    return ComponentType.BUSINESS;
                                }

                                public NodeInvocation execute(NodeExecutionContext c) {
                                    calls.incrementAndGet();
                                    return NodeInvocation.completed(
                                            NodeResult.continueWith(
                                                    Map.of("value", 17), Set.of("NEXT")));
                                }
                            };
                    ScriptRuntime scripts =
                            invocation -> {
                                collections.incrementAndGet();
                                return CompletableFuture.completedFuture(List.of(11, 22));
                            };
                    var registry =
                            NodeExecutorRegistry.withBuiltIns(
                                    List.of(
                                            business,
                                            new com.playrewards.flowengine.core.node.structure
                                                    .ParallelCoordinator(),
                                            new com.playrewards.flowengine.core.node.structure
                                                    .LoopCoordinator(scripts),
                                            new com.playrewards.flowengine.core.node.structure
                                                    .SubFlowNodeExecutor()));
                    return new FlowEngine(
                            registry,
                            ConcurrencyGovernor.defaults(),
                            Clock.systemUTC(),
                            recorder(),
                            context.getBean(DurableExecutionCoordinator.class));
                });
        context.refresh();
        return context;
    }

    private CompiledNode simple(String id, ComponentType type, Map<String, Object> config) {
        return new CompiledNode(
                new FlowNode(id, "builtin." + id, id, id, config),
                new CompiledComponentContract(
                        "builtin." + id,
                        type,
                        null,
                        Map.of(),
                        Map.of(),
                        Map.of(),
                        new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 10)),
                Map.of());
    }

    private CompiledNode valueNode(String id, ComponentType type, Map<String, Object> config) {
        var node = simple(id, type, config);
        var c = node.componentContract();
        return new CompiledNode(
                node.node(),
                new CompiledComponentContract(
                        c.componentCode(),
                        type,
                        null,
                        c.configSchema(),
                        Map.of(),
                        Map.of(
                                "value",
                                new OutputSlotContract(
                                        "value",
                                        new com.playrewards.flowengine.contract.protocol
                                                .ProtocolSchema("any", "1", Map.of()))),
                        c.capabilities()),
                Map.of());
    }

    private ExecutionPlan graph(
            String tenant,
            String flow,
            List<CompiledNode> nodes,
            List<String[]> edges,
            Map<String, String> loops,
            Map<String, ExecutionPlan> children) {
        Map<String, CompiledNode> index = new TreeMap<>();
        Map<String, List<String>> forward = new HashMap<>(), reverse = new HashMap<>();
        Map<String, Map<String, List<String>>> outgoing = new HashMap<>(),
                incoming = new HashMap<>();
        for (var node : nodes) {
            String id = node.node().id();
            index.put(id, node);
            forward.put(id, new ArrayList<>());
            reverse.put(id, new ArrayList<>());
            outgoing.put(id, new HashMap<>());
            incoming.put(id, new HashMap<>());
        }
        for (var edge : edges) {
            forward.get(edge[0]).add(edge[1]);
            reverse.get(edge[1]).add(edge[0]);
            outgoing.get(edge[0]).computeIfAbsent(edge[2], k -> new ArrayList<>()).add(edge[1]);
            incoming.get(edge[1]).computeIfAbsent("IN", k -> new ArrayList<>()).add(edge[0]);
        }
        return new ExecutionPlan(
                new FlowIdentity(tenant, flow, "1"),
                "hash-" + flow,
                index,
                forward,
                reverse,
                outgoing,
                incoming,
                reverse,
                loops,
                Map.of(),
                children);
    }

    private AnnotationConfigApplicationContext context(
            DurableStores stores,
            AtomicInteger calls,
            java.util.function.Consumer<ExecutionState> after) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(
                DurableExecutionCoordinator.class,
                () ->
                        new DurableExecutionCoordinator(
                                tenant -> stores,
                                new MySqlIdempotencyStore(db),
                                Duration.ofSeconds(5),
                                after));
        context.registerBean(
                FlowEngine.class,
                () -> {
                    NodeExecutor business =
                            new NodeExecutor() {
                                public ComponentType supports() {
                                    return ComponentType.BUSINESS;
                                }

                                public NodeInvocation execute(NodeExecutionContext c) {
                                    calls.incrementAndGet();
                                    return NodeInvocation.completed(
                                            NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                                }
                            };
                    return new FlowEngine(
                            NodeExecutorRegistry.withBuiltIns(List.of(business)),
                            ConcurrencyGovernor.defaults(),
                            Clock.systemUTC(),
                            recorder(),
                            context.getBean(DurableExecutionCoordinator.class));
                });
        context.refresh();
        return context;
    }

    private ExecutionRequest request(String tenant) {
        return new ExecutionRequest(
                "inline:recovery",
                ExecutionMode.DURABLE,
                ExecutionPurpose.PRODUCTION,
                Map.of(),
                Map.of(),
                null,
                UUID.randomUUID().toString(),
                tenant,
                Instant.now().plusSeconds(60),
                Map.of());
    }

    private com.playrewards.flowengine.core.observation.ExecutionRecorder recorder() {
        var router = new MySqlTenantStoreRouter(db);
        return new com.playrewards.flowengine.core.observation.ExecutionRecorder(
                router,
                router,
                new com.playrewards.flowengine.core.event.BoundedExecutionEventBus(
                        router, 100, 256),
                Clock.systemUTC(),
                true);
    }

    @Test
    void persistsRetryWaitAndAttemptBeforeAnotherInvocation() {
        String tenant = "retry-" + UUID.randomUUID();
        var stores = stores(tenant);
        try (var coordinator =
                new DurableExecutionCoordinator(
                        t -> stores, new MySqlIdempotencyStore(db), Duration.ofSeconds(5))) {
            var flow = plan(tenant);
            var session = coordinator.begin(flow, request(tenant));
            session.before(session.executionId(), flow.nodesById().get("a"), ScopePath.root());
            session.retry(
                    session.executionId(),
                    flow.nodesById().get("a"),
                    ScopePath.root(),
                    "HTTP_STATUS_503",
                    new ExecutionContext(Map.of()));
            var state = stores.states().find(session.executionId()).orElseThrow();
            assertEquals(ExecutionStatus.WAITING_RETRY, state.status());
            var document =
                    com.alibaba.fastjson2.JSON.parseObject(
                            com.alibaba.fastjson2.JSON.toJSONString(
                                    stores.payloads()
                                            .find(
                                                    new PayloadReference(
                                                            (String) state.state().get("document")))
                                            .orElseThrow()));
            assertEquals(2, document.getJSONObject("attempts").values().iterator().next());
            assertTrue(document.getJSONObject("retry").containsKey("code"));
        }
    }

    @Test
    void uncertainInFlightSideEffectWaitsForManualAndNeverCallsAgain() throws Exception {
        String tenant = "unsafe-" + UUID.randomUUID();
        var stores = stores(tenant);
        var calls = new AtomicInteger();
        var called = new CountDownLatch(1);
        var base = plan(tenant);
        var nodes = new LinkedHashMap<>(base.nodesById());
        var old = nodes.get("a");
        var c = old.componentContract();
        nodes.put(
                "a",
                new CompiledNode(
                        old.node(),
                        new CompiledComponentContract(
                                c.componentCode(),
                                c.componentType(),
                                c.executorRef(),
                                c.configSchema(),
                                c.inputSlots(),
                                c.outputSlots(),
                                new ComponentCapabilities(
                                        ComponentEffect.SIDE_EFFECT, true, false, false, 10)),
                        Map.of()));
        var flow =
                new ExecutionPlan(
                        base.flowIdentity(),
                        base.bundleHash(),
                        nodes,
                        base.forwardAdjacency(),
                        base.reverseAdjacency(),
                        base.outgoingByPort(),
                        base.incomingByPort(),
                        base.activationPredecessors(),
                        base.loopPairs(),
                        base.lockPairs(),
                        base.subFlowPlans());
        String id;
        var coordinator =
                new DurableExecutionCoordinator(
                        t -> stores, new MySqlIdempotencyStore(db), Duration.ofSeconds(5));
        NodeExecutor unsafe =
                new NodeExecutor() {
                    public ComponentType supports() {
                        return ComponentType.BUSINESS;
                    }

                    public NodeInvocation execute(NodeExecutionContext ctx) {
                        calls.incrementAndGet();
                        called.countDown();
                        return new NodeInvocation(
                                new CompletableFuture<>(), ComponentInvocation.emptyContent());
                    }
                };
        try (var first =
                new FlowEngine(
                        NodeExecutorRegistry.withBuiltIns(List.of(unsafe)),
                        ConcurrencyGovernor.defaults(),
                        Clock.systemUTC(),
                        recorder(),
                        coordinator)) {
            var handle = first.execute(flow, request(tenant));
            id = handle.executionId();
            assertTrue(called.await(10, TimeUnit.SECONDS));
        }
        try (var second = context(stores, calls, state -> {})) {
            var result =
                    second.getBean(FlowEngine.class)
                            .resume(id, tenant)
                            .completion()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.WAITING_MANUAL, result.status(), result.errorCode());
            assertEquals(1, calls.get());
            assertEquals(
                    ExecutionStatus.WAITING_MANUAL,
                    stores.states().find(id).orElseThrow().status());
        }
    }

    private ExecutionPlan plan(String tenant) {
        Map<String, CompiledNode> nodes = new LinkedHashMap<>();
        for (String id : List.of("start", "a", "b", "end")) {
            ComponentType type =
                    id.equals("start")
                            ? ComponentType.START
                            : id.equals("end") ? ComponentType.END : ComponentType.BUSINESS;
            var node =
                    new FlowNode(
                            id,
                            "builtin." + id,
                            id,
                            id,
                            id.equals("end") ? Map.of("resultStatus", "SUCCESS") : Map.of());
            var contract =
                    new CompiledComponentContract(
                            "builtin." + id,
                            type,
                            type == ComponentType.BUSINESS ? "business" : null,
                            Map.of(),
                            Map.of(),
                            Map.of(),
                            new ComponentCapabilities(
                                    ComponentEffect.IDEMPOTENT, true, false, false, 10));
            nodes.put(id, new CompiledNode(node, contract, Map.of()));
        }
        return new ExecutionPlan(
                new FlowIdentity(tenant, "recovery", "1"),
                "hash",
                nodes,
                Map.of("start", List.of("a"), "a", List.of("b"), "b", List.of("end")),
                Map.of(),
                Map.of(
                        "start",
                        Map.of("NEXT", List.of("a")),
                        "a",
                        Map.of("NEXT", List.of("b")),
                        "b",
                        Map.of("NEXT", List.of("end"))),
                Map.of(),
                Map.of("a", List.of("start"), "b", List.of("a"), "end", List.of("b")),
                Map.of(),
                Map.of(),
                Map.of());
    }
}
