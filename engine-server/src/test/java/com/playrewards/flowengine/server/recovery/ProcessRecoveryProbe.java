package com.playrewards.flowengine.server.recovery;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.adapter.mysql.*;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.durable.DurableExecutionCoordinator;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.core.node.*;
import com.playrewards.flowengine.server.config.DurableRecoveryLifecycle;
import com.playrewards.flowengine.spi.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

/**
 * 独立 JVM 恢复验收入口，仅使用显式指定的隔离数据库；不进入发行版业务组件。 外部驱动在 CHECKPOINT 标记后强制终止进程，计数留在 MySQL，不能靠共享 Java 内存通过。
 */
public final class ProcessRecoveryProbe {
    private ProcessRecoveryProbe() {}

    /** start 在提交后停车供外部终止；resume 在新 JVM 验证持久化结果与无重复调用。 */
    public static void main(String[] args) throws Exception {
        if (args.length != 3
                || !Set.of("start", "resume", "batch-start", "batch-resume", "batch-auto-resume")
                        .contains(args[0])) {
            throw new IllegalArgumentException(
                    "usage: start|resume|batch-start|batch-resume|batch-auto-resume"
                            + " tenant checkpointNumber|executionId|count");
        }
        String mode = args[0];
        boolean batch = mode.startsWith("batch-");
        String tenant = args[1];
        if (!tenant.startsWith("process-probe-"))
            throw new IllegalArgumentException("isolated tenant required");
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        required("FLOW_PROBE_JDBC_URL"),
                        required("FLOW_PROBE_DB_USER"),
                        required("FLOW_PROBE_DB_PASSWORD"));
        source.setPoolMaximumActiveConnections(16);
        source.setPoolMaximumIdleConnections(16);
        source.setPoolTimeToWait(5000);
        var ledgerSource =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        required("FLOW_PROBE_JDBC_URL"),
                        required("FLOW_PROBE_DB_USER"),
                        required("FLOW_PROBE_DB_PASSWORD"));
        ledgerSource.setPoolMaximumActiveConnections(16);
        ledgerSource.setPoolMaximumIdleConnections(16);
        ledgerSource.setPoolTimeToWait(5000);
        var database = new MySqlRuntimeDatabase(source);
        database.migrate();
        try (var connection = ledgerSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE IF NOT EXISTS probe_invocation (tenant_id VARCHAR(128) NOT NULL,"
                            + " node_code VARCHAR(64) NOT NULL, invocation_id VARCHAR(64) NOT NULL,"
                            + " PRIMARY KEY(tenant_id,invocation_id))");
        }
        var stores =
                new DurableStores(
                        new MySqlExecutionStateStore(database, tenant),
                        new MySqlLeaseRepository(database, tenant),
                        new MySqlPayloadStore(database, tenant, 1048576, Duration.ofHours(1)));
        AtomicInteger checkpoints = new AtomicInteger();
        int stopAt = mode.equals("start") ? Integer.parseInt(args[2]) : -1;
        int batchSize = batch ? Integer.parseInt(args[2]) : 0;
        if (batch && (batchSize < 1 || batchSize > 1000)) {
            throw new IllegalArgumentException("batch count must be between 1 and 1000");
        }
        var batchReady = new CountDownLatch(batchSize);
        var batchMarker = new AtomicBoolean();
        var batchExecutionIds = new ConcurrentSkipListSet<String>();
        try (var coordinator =
                new DurableExecutionCoordinator(
                        t -> {
                            if (!tenant.equals(t)) throw new SecurityException("wrong tenant");
                            return stores;
                        },
                        new MySqlIdempotencyStore(database),
                        Duration.ofSeconds(batch ? 15 : 2),
                        state -> {
                            if (!mode.equals("batch-start")
                                    && state.state().containsKey("lastCompleted")
                                    && checkpoints.incrementAndGet() == stopAt) {
                                System.out.println("CHECKPOINT " + state.executionId());
                                System.out.flush();
                                // 不使用正常关闭模拟崩溃；只有测试驱动 SIGKILL 会解除该独立进程。
                                new CompletableFuture<Void>().join();
                            }
                        })) {
            NodeExecutor business =
                    new NodeExecutor() {
                        @Override
                        public ComponentType supports() {
                            return ComponentType.BUSINESS;
                        }

                        @Override
                        public NodeInvocation execute(NodeExecutionContext context) {
                            if (context.node().node().nodeCode().equals("barrier")) {
                                if (mode.equals("batch-start")) {
                                    batchReady.countDown();
                                    if (batchReady.getCount() == 0
                                            && batchMarker.compareAndSet(false, true)) {
                                        while (batchExecutionIds.size() < batchSize) {
                                            Thread.onSpinWait();
                                        }
                                        System.out.println(
                                                "BATCH_CHECKPOINT "
                                                        + JSON.toJSONString(
                                                                Map.of(
                                                                        "executionIds",
                                                                        List.copyOf(
                                                                                batchExecutionIds))));
                                        System.out.flush();
                                    }
                                    // This wait is outside the synchronized checkpoint callback,
                                    // so Java 21 virtual threads can unmount instead of pinning all
                                    // scheduler carriers before the batch reaches the barrier.
                                    new CompletableFuture<Void>().join();
                                }
                                return NodeInvocation.completed(
                                        NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                            }
                            // 测试专用外部计数，不借用引擎事务，真实暴露重复业务调用。
                            try (var connection = ledgerSource.getConnection();
                                    var statement =
                                            connection.prepareStatement(
                                                    "INSERT INTO probe_invocation VALUES"
                                                            + " (?,?,?)")) {
                                statement.setString(1, tenant);
                                statement.setString(2, context.node().node().nodeCode());
                                statement.setString(
                                        3,
                                        context.executionId()
                                                + "/"
                                                + context.node().node().nodeCode());
                                statement.executeUpdate();
                            } catch (java.sql.SQLException failure) {
                                throw new IllegalStateException("probe counter failed");
                            }
                            return NodeInvocation.completed(
                                    NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                        }
                    };
            var governor =
                    new ConcurrencyGovernor(256, 256, 256, 256, 256, Clock.systemUTC());
            try (var engine =
                    new FlowEngine(
                            NodeExecutorRegistry.withBuiltIns(List.of(business)),
                            governor,
                            Clock.systemUTC(),
                            null,
                            coordinator)) {
                if (mode.equals("batch-start")) {
                    List<CompletableFuture<ExecutionResult>> completions = new ArrayList<>();
                    for (int index = 0; index < batchSize; index++) {
                        var handle = engine.execute(plan(tenant, true), request(tenant));
                        batchExecutionIds.add(handle.executionId());
                        completions.add(handle.completion().toCompletableFuture());
                    }
                    CompletableFuture.allOf(completions.toArray(CompletableFuture[]::new)).join();
                    throw new AssertionError("batch start completed without external SIGKILL");
                }

                if (mode.equals("batch-resume") || mode.equals("batch-auto-resume")) {
                    List<String> executionIds = executionIds(source, tenant);
                    if (executionIds.size() != batchSize) {
                        throw new AssertionError(
                                "unexpected recoverable count " + executionIds.size());
                    }
                    long started = System.nanoTime();
                    List<Long> latencies =
                            java.util.Collections.synchronizedList(new ArrayList<>());
                    long succeeded;
                    try (var resources = new RecoveryResourceSampler(governor, database)) {
                        if (mode.equals("batch-auto-resume")) {
                            var lifecycle = new DurableRecoveryLifecycle(
                                    new MySqlTenantStoreRouter(database), engine);
                            lifecycle.start();
                            try {
                                var completed = new HashSet<String>();
                                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(300);
                                while (completed.size() < executionIds.size()
                                        && System.nanoTime() < deadline) {
                                    for (String executionId : executionIds) {
                                        var state = stores.states().find(executionId);
                                        if (state.isPresent()
                                                && terminal(state.get().status())
                                                && completed.add(executionId)) {
                                            latencies.add(TimeUnit.NANOSECONDS.toMillis(
                                                    System.nanoTime() - started));
                                        }
                                    }
                                    Thread.sleep(25);
                                }
                                if (completed.size() != executionIds.size()) {
                                    throw new AssertionError(
                                            "automatic recovery timed out at " + completed.size());
                                }
                            } finally {
                                lifecycle.stop();
                            }
                            succeeded = executionIds.stream()
                                    .map(stores.states()::find)
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .filter(state -> state.status() == ExecutionStatus.SUCCEEDED)
                                    .count();
                        } else {
                            List<CompletableFuture<ExecutionResult>> completions = new ArrayList<>();
                            for (String executionId : executionIds) {
                                var completion =
                                        engine.resume(executionId, tenant)
                                                .completion()
                                                .toCompletableFuture()
                                                .whenComplete(
                                                        (ignored, failure) ->
                                                                latencies.add(
                                                                        TimeUnit.NANOSECONDS.toMillis(
                                                                                System.nanoTime()
                                                                                        - started)));
                                completions.add(completion);
                            }
                            CompletableFuture.allOf(completions.toArray(CompletableFuture[]::new))
                                    .get(300, TimeUnit.SECONDS);
                            succeeded =
                                    completions.stream()
                                            .map(CompletableFuture::join)
                                            .filter(result ->
                                                    result.status() == ExecutionStatus.SUCCEEDED)
                                            .count();
                        }
                        List<Map<String, Object>> counts = counts(ledgerSource, tenant);
                        assertCounts(counts, batchSize);
                        var result = new LinkedHashMap<String, Object>();
                        result.put("requested", batchSize);
                        result.put("succeeded", succeeded);
                        result.put("failed", batchSize - succeeded);
                        result.put(
                                "wallMillis",
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
                        result.put("p95Millis", percentile(latencies, 0.95));
                        result.put("p99Millis", percentile(latencies, 0.99));
                        result.put("resources", resources.metrics());
                        result.put("calls", counts);
                        System.out.println("BATCH_RECOVERED " + JSON.toJSONString(result));
                    }
                    return;
                }

                var handle =
                        mode.equals("start")
                                ? engine.execute(plan(tenant, false), request(tenant))
                                : engine.resume(args[2], tenant);
                var result = handle.completion().toCompletableFuture().get(45, TimeUnit.SECONDS);
                if (result.status() != ExecutionStatus.SUCCEEDED) {
                    throw new AssertionError(
                            "unexpected terminal " + result.status() + "/" + result.errorCode());
                }
                List<Map<String, Object>> counts = new ArrayList<>();
                counts.addAll(counts(ledgerSource, tenant));
                assertCounts(counts, 1);
                System.out.println(
                        "RECOVERED "
                                + JSON.toJSONString(
                                        Map.of(
                                                "executionId",
                                                result.executionId(),
                                                "status",
                                                result.status(),
                                                "calls",
                                                counts)));
            }
        }
    }

    private static final class RecoveryResourceSampler implements AutoCloseable {
        private final ConcurrencyGovernor governor;
        private final MySqlRuntimeDatabase database;
        private final long heapBaseline = usedHeap();
        private final int platformThreadBaseline = platformThreads();
        private final AtomicLong heapHighWater = new AtomicLong(heapBaseline);
        private final AtomicInteger platformThreadHighWater =
                new AtomicInteger(platformThreadBaseline);
        private final AtomicInteger nodePermitInFlightHighWater = new AtomicInteger();
        private final AtomicInteger storageCapacityInFlightHighWater = new AtomicInteger();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread sampler;

        private RecoveryResourceSampler(
                ConcurrencyGovernor governor, MySqlRuntimeDatabase database) {
            this.governor = governor;
            this.database = database;
            sampler =
                    Thread.ofPlatform()
                            .daemon()
                            .name("flow-recovery-capacity-sampler")
                            .start(
                                    () -> {
                                        while (running.get()) {
                                            sample();
                                            try {
                                                Thread.sleep(10);
                                            } catch (InterruptedException ignored) {
                                                Thread.currentThread().interrupt();
                                                break;
                                            }
                                        }
                                    });
        }

        private void sample() {
            heapHighWater.accumulateAndGet(usedHeap(), Math::max);
            platformThreadHighWater.accumulateAndGet(platformThreads(), Math::max);
            nodePermitInFlightHighWater.accumulateAndGet(
                    governor.snapshot().inFlightAcquisitions(), Math::max);
            storageCapacityInFlightHighWater.accumulateAndGet(
                    database.capacitySnapshot().inFlight(), Math::max);
        }

        private RecoveryResourceMetrics metrics() {
            sample();
            return new RecoveryResourceMetrics(
                    heapBaseline,
                    heapHighWater.get(),
                    Math.max(0, heapHighWater.get() - heapBaseline),
                    platformThreadBaseline,
                    platformThreadHighWater.get(),
                    Math.max(0, platformThreadHighWater.get() - platformThreadBaseline),
                    nodePermitInFlightHighWater.get(),
                    storageCapacityInFlightHighWater.get(),
                    governor.snapshot().inFlightAcquisitions(),
                    database.capacitySnapshot().inFlight());
        }

        @Override
        public void close() throws InterruptedException {
            running.set(false);
            sampler.interrupt();
            sampler.join(1000);
        }

        private static long usedHeap() {
            return java.lang.management.ManagementFactory.getMemoryMXBean()
                    .getHeapMemoryUsage()
                    .getUsed();
        }

        private static int platformThreads() {
            return java.lang.management.ManagementFactory.getThreadMXBean().getThreadCount();
        }
    }

    private record RecoveryResourceMetrics(
            long heapBaselineBytes,
            long heapHighWaterBytes,
            long heapGrowthBytes,
            int platformThreadsBaseline,
            int platformThreadsHighWater,
            int platformThreadGrowth,
            int nodePermitInFlightHighWater,
            int storageCapacityInFlightHighWater,
            int nodePermitInFlightFinal,
            int storageCapacityInFlightFinal) {}

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing " + name);
        return value;
    }

    private static boolean terminal(ExecutionStatus status) {
        return status == ExecutionStatus.SUCCEEDED
                || status == ExecutionStatus.FAILED
                || status == ExecutionStatus.CANCELLED
                || status == ExecutionStatus.WAITING_MANUAL;
    }

    private static List<String> executionIds(DataSource source, String tenant)
            throws java.sql.SQLException {
        List<String> ids = new ArrayList<>();
        try (var connection = source.getConnection();
                var statement =
                        connection.prepareStatement(
                                "SELECT execution_id FROM engine_execution_state"
                                        + " WHERE tenant_id=? ORDER BY execution_id")) {
            statement.setString(1, tenant);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) ids.add(rows.getString(1));
            }
        }
        return ids;
    }

    private static List<Map<String, Object>> counts(DataSource source, String tenant)
            throws java.sql.SQLException {
        List<Map<String, Object>> counts = new ArrayList<>();
        try (var connection = source.getConnection();
                var statement =
                        connection.prepareStatement(
                                "SELECT node_code,COUNT(*) AS calls FROM probe_invocation"
                                        + " WHERE tenant_id=? GROUP BY node_code ORDER BY node_code")) {
            statement.setString(1, tenant);
            try (var rows = statement.executeQuery()) {
                while (rows.next())
                    counts.add(Map.of("node", rows.getString(1), "calls", rows.getLong(2)));
            }
        }
        return counts;
    }

    private static void assertCounts(List<Map<String, Object>> counts, int expected) {
        if (counts.size() != 3
                || counts.stream()
                        .anyMatch(row -> ((Number) row.get("calls")).longValue() != expected)) {
            throw new AssertionError("business invocation repeated or missing: " + counts);
        }
    }

    private static long percentile(List<Long> values, double percentile) {
        List<Long> sorted = values.stream().sorted().toList();
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(percentile * sorted.size()) - 1);
        return sorted.get(Math.max(0, index));
    }

    private static ExecutionRequest request(String tenant) {
        return new ExecutionRequest(
                "inline:process-probe",
                ExecutionMode.DURABLE,
                ExecutionPurpose.PRODUCTION,
                Map.of(),
                Map.of(),
                null,
                UUID.randomUUID().toString(),
                tenant,
                Instant.now().plusSeconds(300),
                Map.of());
    }

    private static ExecutionPlan plan(String tenant, boolean includeBarrier) {
        Map<String, CompiledNode> nodes = new LinkedHashMap<>();
        var order =
                includeBarrier
                        ? List.of("start", "first", "barrier", "second", "third", "end")
                        : List.of("start", "first", "second", "third", "end");
        for (String id : order) {
            ComponentType type =
                    id.equals("start")
                            ? ComponentType.START
                            : id.equals("end") ? ComponentType.END : ComponentType.BUSINESS;
            var node =
                    new FlowNode(
                            id,
                            "probe." + id,
                            id,
                            id,
                            id.equals("end") ? Map.of("resultStatus", "SUCCESS") : Map.of());
            var contract =
                    new CompiledComponentContract(
                            "probe." + id,
                            type,
                            type == ComponentType.BUSINESS ? "probe" : null,
                            Map.of(),
                            Map.of(),
                            Map.of(),
                            new ComponentCapabilities(
                                    id.equals("barrier")
                                            ? ComponentEffect.PURE
                                            : ComponentEffect.IDEMPOTENT,
                                    true,
                                    false,
                                    false,
                                    256));
            nodes.put(id, new CompiledNode(node, contract, Map.of()));
        }
        Map<String, List<String>> outgoing = new LinkedHashMap<>(),
                incoming = new LinkedHashMap<>();
        Map<String, Map<String, List<String>>> ports = new LinkedHashMap<>();
        for (int i = 1; i < order.size(); i++) {
            String previous = order.get(i - 1), current = order.get(i);
            outgoing.put(previous, List.of(current));
            incoming.put(current, List.of(previous));
            ports.put(previous, Map.of("NEXT", List.of(current)));
        }
        return new ExecutionPlan(
                new FlowIdentity(tenant, "process-probe", "1"),
                "probe-v1",
                nodes,
                outgoing,
                Map.of(),
                ports,
                Map.of(),
                incoming,
                Map.of(),
                Map.of(),
                Map.of());
    }
}
