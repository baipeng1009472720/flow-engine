package com.playrewards.flowengine.adapter.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 增量容量计数契约：上限判断不再扫描整表，但在并发写入、过期删除和升级种子后仍与真实行数一致。
 */
class MySqlCapacityCounterTest {
    static final TestMySql MYSQL = TestMySql.start();
    MySqlRuntimeDatabase db;

    @BeforeEach
    void setup() {
        db = new MySqlRuntimeDatabase(source());
        db.migrate();
    }

    @Test
    void concurrentAppendsNeverOvershootAndCounterMatchesRealRows() throws Exception {
        String tenant = "counter-" + UUID.randomUUID();
        var store = new MySqlObservationStore(db, tenant, 25, Duration.ofHours(1));
        List<String> executions = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            String id = UUID.randomUUID().toString();
            store.saveExecution(running(tenant, id));
            executions.add(id);
        }
        var accepted = new AtomicInteger();
        var rejected = new AtomicInteger();
        var start = new CountDownLatch(1);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> calls = new ArrayList<>();
            for (int index = 0; index < 8; index++) {
                String id = executions.get(index);
                calls.add(
                        callers.submit(
                                () -> {
                                    start.await();
                                    for (int sequence = 0; sequence < 5; sequence++) {
                                        try {
                                            store.append(event(id, sequence));
                                            accepted.incrementAndGet();
                                        } catch (IllegalStateException full) {
                                            assertEquals(
                                                    "RUNTIME_STORAGE_CAPACITY_EXCEEDED",
                                                    full.getMessage());
                                            rejected.incrementAndGet();
                                        }
                                    }
                                    return null;
                                }));
            }
            start.countDown();
            for (var call : calls) call.get(30, TimeUnit.SECONDS);
        }
        assertEquals(25, accepted.get());
        assertEquals(15, rejected.get());
        assertEquals(25L, rows("engine_event", tenant));
        assertEquals(25L, usage(tenant, "engine_event"));
        assertEquals(8L, usage(tenant, "engine_execution"));
    }

    @Test
    void retentionDeletesReleaseCountedCapacity() {
        String tenant = "counter-release-" + UUID.randomUUID();
        var store = new MySqlObservationStore(db, tenant, 2, Duration.ofHours(1));
        String id = UUID.randomUUID().toString();
        store.saveExecution(running(tenant, id));
        store.append(event(id, 0));
        store.append(event(id, 1));
        assertThrows(IllegalStateException.class, () -> store.append(event(id, 2)));
        // 模拟两条事件已超过保留期；后续写入必须先释放计数，而不是被历史计数永久卡住。
        db.transaction(
                tx ->
                        tx.update(
                                "UPDATE engine_event SET created_at='2000-01-01' WHERE"
                                        + " tenant_id=#{args[0]}",
                                tenant));
        store.append(event(id, 2));
        assertEquals(1L, rows("engine_event", tenant));
        assertEquals(1L, usage(tenant, "engine_event"));
    }

    @Test
    void upgradeSeedsCountersFromExistingRows() {
        var fresh = TestMySql.start();
        var source = source(fresh);
        Flyway.configure()
                .dataSource(source)
                .locations("classpath:db/migration")
                .target("3")
                .load()
                .migrate();
        var legacy = new MySqlRuntimeDatabase(source);
        legacy.transaction(
                tx -> {
                    for (int index = 0; index < 3; index++)
                        tx.update(
                                "INSERT INTO engine_execution(tenant_id,execution_id,status,body)"
                                        + " VALUES('legacy',#{args[0]},'SUCCEEDED','{}')",
                                "e-" + index);
                    return null;
                });
        legacy.migrate();
        assertEquals(
                3L,
                (long) legacy.transaction(
                        tx ->
                                MySqlRuntimeDatabase.number(
                                        tx.row(
                                                        "SELECT used FROM engine_capacity_usage"
                                                                + " WHERE tenant_id='legacy' AND"
                                                                + " table_name='engine_execution'")
                                                .orElseThrow(),
                                        "used")));
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

    static ExecutionRecord running(String tenant, String id) {
        return new ExecutionRecord(
                id,
                id,
                null,
                new FlowIdentity(tenant, "flow", "1"),
                "hash",
                ExecutionMode.EPHEMERAL,
                ExecutionPurpose.TRIAL,
                ExecutionStatus.RUNNING,
                Instant.now(),
                null,
                null,
                null,
                null,
                -1);
    }

    static ExecutionEvent event(String id, long sequence) {
        return new ExecutionEvent(
                id + ":" + sequence,
                id,
                "LOG",
                ExecutionStatus.RUNNING,
                null,
                null,
                null,
                0,
                sequence,
                Instant.now(),
                Map.of());
    }

    private static MysqlDataSource source() {
        return source(MYSQL);
    }

    private static MysqlDataSource source(TestMySql mysql) {
        var source = new MysqlDataSource();
        source.setURL(mysql.getJdbcUrl());
        source.setUser(mysql.getUsername());
        source.setPassword(mysql.getPassword());
        return source;
    }
}
