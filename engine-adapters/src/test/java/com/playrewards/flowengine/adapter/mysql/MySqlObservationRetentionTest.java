package com.playrewards.flowengine.adapter.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

/**
 * 观察记录保留契约：终态记录按模式过期并级联清理，清理分批执行；容量满时先清理过期记录再判断是否拒绝，
 * 活动执行永不因保留期被删除。
 */
class MySqlObservationRetentionTest {
    static final TestMySql MYSQL = TestMySql.start();
    MySqlRuntimeDatabase db;

    @BeforeEach
    void setup() {
        var source = new MysqlDataSource();
        source.setURL(MYSQL.getJdbcUrl());
        source.setUser(MYSQL.getUsername());
        source.setPassword(MYSQL.getPassword());
        db = new MySqlRuntimeDatabase(source);
        db.migrate();
    }

    @Test
    void terminalRecordsExpireByModeWhileActiveDurableNeverExpires() {
        String tenant = "expiry-" + UUID.randomUUID();
        var policy =
                new MySqlRetentionPolicy(
                        Duration.ofDays(7),
                        Duration.ofDays(1),
                        Duration.ofHours(6),
                        Duration.ofHours(1),
                        Duration.ofHours(1),
                        Duration.ofHours(1),
                        100);
        var store = new MySqlObservationStore(db, tenant, Map.of(), policy);
        String durable = UUID.randomUUID().toString();
        String ephemeral = UUID.randomUUID().toString();
        store.saveExecution(record(tenant, durable, ExecutionMode.DURABLE, ExecutionStatus.RUNNING));
        store.saveExecution(
                record(tenant, ephemeral, ExecutionMode.EPHEMERAL, ExecutionStatus.RUNNING));
        assertNull(expiresInHours(tenant, durable));
        assertEquals(6L, expiresInHours(tenant, ephemeral));

        store.commitTerminal(
                record(tenant, ephemeral, ExecutionMode.EPHEMERAL, ExecutionStatus.SUCCEEDED),
                terminal(ephemeral, 0));
        assertEquals(24L, expiresInHours(tenant, ephemeral));
    }

    @Test
    void purgeCascadesExpiredExecutionsInBatchesAndKeepsActiveOnes() {
        String tenant = "purge-" + UUID.randomUUID();
        var policy = MySqlRetentionPolicy.defaults().withBatchSize(2);
        var store = new MySqlObservationStore(db, tenant, Map.of(), policy);
        List<String> expired = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            String id = UUID.randomUUID().toString();
            finishedWithNode(store, tenant, id);
            expired.add(id);
        }
        String fresh = UUID.randomUUID().toString();
        finishedWithNode(store, tenant, fresh);
        String active = UUID.randomUUID().toString();
        store.saveExecution(record(tenant, active, ExecutionMode.DURABLE, ExecutionStatus.RUNNING));
        // 只把前五条推到过期；新鲜终态与活动执行必须保留。
        db.transaction(
                tx -> {
                    for (String id : expired)
                        tx.update(
                                "UPDATE engine_execution SET expires_at='2000-01-01' WHERE"
                                        + " tenant_id=#{args[0]} AND execution_id=#{args[1]}",
                                tenant,
                                id);
                    return null;
                });

        var retention = new MySqlObservationRetention(db, policy);
        var result = retention.purgeExpired();
        assertEquals(5, result.executions());
        for (String id : expired) {
            assertTrue(store.execution(id).isEmpty());
            assertTrue(store.nodes(id).isEmpty());
            assertTrue(store.readAfter(id, -1).isEmpty());
        }
        assertTrue(store.execution(fresh).isPresent());
        assertEquals(1, store.nodes(fresh).size());
        assertTrue(store.execution(active).isPresent());
        for (String table :
                List.of("engine_execution", "engine_node_attempt", "engine_event", "engine_log"))
            assertEquals(rows(table, tenant), usage(tenant, table), table);
    }

    @Test
    void fullCapacityPurgesExpiredRecordsBeforeRejecting() {
        String tenant = "full-" + UUID.randomUUID();
        var store =
                new MySqlObservationStore(
                        db,
                        tenant,
                        Map.of("engine_execution", 2, "engine_node_attempt", 2),
                        MySqlRetentionPolicy.defaults());
        String old = UUID.randomUUID().toString();
        finishedWithNode(store, tenant, old);
        String running = UUID.randomUUID().toString();
        store.saveExecution(record(tenant, running, ExecutionMode.DURABLE, ExecutionStatus.RUNNING));
        db.transaction(
                tx ->
                        tx.update(
                                "UPDATE engine_execution SET expires_at='2000-01-01' WHERE"
                                        + " tenant_id=#{args[0]} AND execution_id=#{args[1]}",
                                tenant,
                                old));

        String next = UUID.randomUUID().toString();
        store.saveExecution(record(tenant, next, ExecutionMode.EPHEMERAL, ExecutionStatus.RUNNING));
        assertTrue(store.execution(old).isEmpty());
        assertTrue(store.execution(running).isPresent());
        assertEquals(2L, usage(tenant, "engine_execution"));
        assertEquals(0L, usage(tenant, "engine_node_attempt"));
    }

    @Test
    void fullCapacityEvictsOldestTerminalExecutionsButNeverActiveOnes() {
        String tenant = "evict-" + UUID.randomUUID();
        var store =
                new MySqlObservationStore(
                        db, tenant, Map.of("engine_event", 3), MySqlRetentionPolicy.defaults());
        String finished = UUID.randomUUID().toString();
        store.saveExecution(record(tenant, finished, ExecutionMode.EPHEMERAL, ExecutionStatus.RUNNING));
        store.append(MySqlCapacityCounterTest.event(finished, 0));
        store.commitTerminal(
                record(tenant, finished, ExecutionMode.EPHEMERAL, ExecutionStatus.SUCCEEDED),
                terminal(finished, 1));
        String active = UUID.randomUUID().toString();
        store.saveExecution(record(tenant, active, ExecutionMode.DURABLE, ExecutionStatus.RUNNING));
        store.append(MySqlCapacityCounterTest.event(active, 0));
        // 表已满且没有任何过期记录：淘汰最早可过期的终态执行，而不是让新执行写入失败。
        store.append(MySqlCapacityCounterTest.event(active, 1));
        assertTrue(store.execution(finished).isEmpty());
        assertEquals(2, store.readAfter(active, -1).size());
        store.append(MySqlCapacityCounterTest.event(active, 2));
        // 只剩活动执行占满容量时保持硬上限。
        assertThrows(
                IllegalStateException.class,
                () -> store.append(MySqlCapacityCounterTest.event(active, 3)));
        assertEquals(3L, usage(tenant, "engine_event"));
    }

    private void finishedWithNode(MySqlObservationStore store, String tenant, String id) {
        store.saveExecution(record(tenant, id, ExecutionMode.EPHEMERAL, ExecutionStatus.RUNNING));
        String node = UUID.randomUUID().toString();
        Instant now = Instant.now();
        store.saveNode(
                new NodeExecutionRecord(
                        node,
                        id,
                        "node",
                        "node",
                        ComponentType.SCRIPT,
                        "root",
                        1,
                        ExecutionStatus.SUCCEEDED,
                        now,
                        now,
                        now,
                        null,
                        null,
                        null,
                        null));
        store.appendLog(
                new ExecutionLogRecord(
                        UUID.randomUUID().toString(),
                        id,
                        node,
                        now,
                        ExecutionLogLevel.INFO,
                        "engine",
                        0,
                        "node started",
                        Map.of(),
                        null));
        store.commitTerminal(
                record(tenant, id, ExecutionMode.EPHEMERAL, ExecutionStatus.SUCCEEDED),
                terminal(id, 0));
    }

    private Long expiresInHours(String tenant, String id) {
        return db.transaction(
                tx -> {
                    var row =
                            tx.row(
                                            "SELECT execution_id,TIMESTAMPDIFF(MINUTE,CURRENT_TIMESTAMP(6),"
                                                    + "expires_at) AS m FROM engine_execution"
                                                    + " WHERE tenant_id=#{args[0]} AND"
                                                    + " execution_id=#{args[1]}",
                                            tenant,
                                            id)
                                    .orElseThrow();
                    Object minutes = row.get("m");
                    return minutes == null
                            ? null
                            : Math.round(((Number) minutes).doubleValue() / 60.0);
                });
    }

    private long rows(String table, String tenant) {
        return db.transaction(
                tx ->
                        MySqlRuntimeDatabase.number(
                                tx.row(
                                                "SELECT COUNT(*) AS n FROM "
                                                        + table
                                                        + " WHERE tenant_id=#{args[0]}",
                                                tenant)
                                        .orElseThrow(),
                                "n"));
    }

    private long usage(String tenant, String table) {
        return db.transaction(
                tx ->
                        tx.row(
                                        "SELECT used FROM engine_capacity_usage WHERE"
                                                + " tenant_id=#{args[0]} AND table_name=#{args[1]}",
                                        tenant,
                                        table)
                                .map(r -> MySqlRuntimeDatabase.number(r, "used"))
                                .orElse(0L));
    }

    static ExecutionRecord record(
            String tenant, String id, ExecutionMode mode, ExecutionStatus status) {
        boolean finished = status != ExecutionStatus.RUNNING;
        return new ExecutionRecord(
                id,
                id,
                null,
                new FlowIdentity(tenant, "flow", "1"),
                "hash",
                mode,
                ExecutionPurpose.TRIAL,
                status,
                Instant.now(),
                finished ? Instant.now() : null,
                null,
                null,
                null,
                -1);
    }

    static ExecutionEvent terminal(String id, long sequence) {
        return new ExecutionEvent(
                id + ":" + sequence,
                id,
                "COMPLETED",
                ExecutionStatus.SUCCEEDED,
                null,
                null,
                null,
                0,
                sequence,
                Instant.now(),
                Map.of());
    }
}
