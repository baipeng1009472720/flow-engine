package com.playrewards.flowengine.server.recovery;

import com.alibaba.fastjson2.JSON;
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

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 独立 JVM 恢复验收入口，仅使用显式指定的隔离数据库；不进入发行版业务组件。 外部驱动在 CHECKPOINT 标记后强制终止进程，计数留在 MySQL，不能靠共享 Java 内存通过。
 */
public final class ProcessRecoveryProbe {
    private ProcessRecoveryProbe() {}

    /** start 在提交后停车供外部终止；resume 在新 JVM 验证持久化结果与无重复调用。 */
    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !Set.of("start", "resume").contains(args[0])) {
            throw new IllegalArgumentException(
                    "usage: start|resume tenant checkpointNumber|executionId");
        }
        String tenant = args[1];
        if (!tenant.startsWith("process-probe-"))
            throw new IllegalArgumentException("isolated tenant required");
        var source = new MysqlDataSource();
        source.setURL(required("FLOW_PROBE_JDBC_URL"));
        source.setUser(required("FLOW_PROBE_DB_USER"));
        source.setPassword(required("FLOW_PROBE_DB_PASSWORD"));
        var database = new MySqlRuntimeDatabase(source);
        database.migrate();
        try (var connection = source.getConnection();
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
        int stopAt = args[0].equals("start") ? Integer.parseInt(args[2]) : -1;
        try (var coordinator =
                new DurableExecutionCoordinator(
                        t -> {
                            if (!tenant.equals(t)) throw new SecurityException("wrong tenant");
                            return stores;
                        },
                        new MySqlIdempotencyStore(database),
                        Duration.ofSeconds(2),
                        state -> {
                            if (state.state().containsKey("lastCompleted")
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
                            // 测试专用外部计数，不借用引擎事务，真实暴露重复业务调用。
                            try (var connection = source.getConnection();
                                    var statement =
                                            connection.prepareStatement(
                                                    "INSERT INTO probe_invocation VALUES"
                                                            + " (?,?,?)")) {
                                statement.setString(1, tenant);
                                statement.setString(2, context.node().node().nodeCode());
                                statement.setString(3, UUID.randomUUID().toString());
                                statement.executeUpdate();
                            } catch (java.sql.SQLException failure) {
                                throw new IllegalStateException("probe counter failed");
                            }
                            return NodeInvocation.completed(
                                    NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                        }
                    };
            try (var engine =
                    new FlowEngine(
                            NodeExecutorRegistry.withBuiltIns(List.of(business)),
                            ConcurrencyGovernor.defaults(),
                            Clock.systemUTC(),
                            null,
                            coordinator)) {
                var handle =
                        args[0].equals("start")
                                ? engine.execute(plan(tenant), request(tenant))
                                : engine.resume(args[2], tenant);
                var result = handle.completion().toCompletableFuture().get(45, TimeUnit.SECONDS);
                if (result.status() != ExecutionStatus.SUCCEEDED) {
                    throw new AssertionError(
                            "unexpected terminal " + result.status() + "/" + result.errorCode());
                }
                List<Map<String, Object>> counts = new ArrayList<>();
                try (var connection = source.getConnection();
                        var statement =
                                connection.prepareStatement(
                                        "SELECT node_code,COUNT(*) AS calls FROM probe_invocation"
                                                + " WHERE tenant_id=? GROUP BY node_code ORDER BY"
                                                + " node_code")) {
                    statement.setString(1, tenant);
                    try (var rows = statement.executeQuery()) {
                        while (rows.next())
                            counts.add(Map.of("node", rows.getString(1), "calls", rows.getLong(2)));
                    }
                }
                if (counts.size() != 3
                        || counts.stream()
                                .anyMatch(row -> ((Number) row.get("calls")).longValue() != 1)) {
                    throw new AssertionError("business invocation repeated or missing: " + counts);
                }
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

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing " + name);
        return value;
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
                Instant.now().plusSeconds(120),
                Map.of());
    }

    private static ExecutionPlan plan(String tenant) {
        Map<String, CompiledNode> nodes = new LinkedHashMap<>();
        var order = List.of("start", "first", "second", "third", "end");
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
                                    ComponentEffect.IDEMPOTENT, true, false, false, 10));
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
