package com.playrewards.flowengine.adapter.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.*;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

/**
 * 观察写后缓冲契约：诊断写入批量提交（显著少于逐条事务），读取与终态前排空保证读己之写与事件顺序，
 * 背压与关闭不丢写入，异步失败在 flush 时报告。
 */
class MySqlWriteBehindTest {
    static final TestMySql MYSQL = TestMySql.start();
    final AtomicInteger commits = new AtomicInteger();
    MySqlRuntimeDatabase db;

    @BeforeEach
    void setup() {
        db = new MySqlRuntimeDatabase(countingCommits(plain()));
        db.migrate();
    }

    @Test
    void diagnosticWritesAreBatchedAndReadableImmediately() {
        try (var router = router(new MySqlTenantStoreRouter.WriteBehind(2, 10000, 256))) {
            String tenant = "batch-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
            router.saveExecution(MySqlCapacityCounterTest.running(tenant, id));
            commits.set(0);
            String node = UUID.randomUUID().toString();
            router.saveNode(node(id, node, ExecutionStatus.RUNNING));
            for (int sequence = 0; sequence < 200; sequence++) {
                router.append(MySqlCapacityCounterTest.event(id, sequence));
                router.appendLog(log(id, node, sequence));
            }
            var ref = router.store(id, Map.of("value", 7));
            router.saveNode(node(id, node, ExecutionStatus.SUCCEEDED));
            // 读取必须先排空通道：此刻即可看到全部写入且顺序不变。
            assertEquals(200, router.readAfter(id, -1).size());
            assertEquals(
                    List.of(0L, 1L, 2L),
                    router.readAfter(id, -1).stream().limit(3).map(ExecutionEvent::sequence).toList());
            assertEquals(200, router.logs(node).size());
            assertEquals(ExecutionStatus.SUCCEEDED, router.node(node).orElseThrow().status());
            assertEquals(Map.of("value", 7), router.find(ref).orElseThrow());
            assertTrue(commits.get() < 60, "commits=" + commits.get());
        }
    }

    @Test
    void terminalCommitFlushesPendingEventsFirst() {
        try (var router = router(new MySqlTenantStoreRouter.WriteBehind(1, 10000, 256))) {
            String tenant = "terminal-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
            router.saveExecution(MySqlCapacityCounterTest.running(tenant, id));
            for (int sequence = 0; sequence < 50; sequence++)
                router.append(MySqlCapacityCounterTest.event(id, sequence));
            router.flush(id);
            router.commitTerminal(
                    MySqlObservationRetentionTest.record(
                            tenant, id, ExecutionMode.EPHEMERAL, ExecutionStatus.SUCCEEDED),
                    MySqlObservationRetentionTest.terminal(id, 50));
            var events = new MySqlObservationStore(db, tenant).readAfter(id, -1);
            assertEquals(51, events.size());
            assertEquals("COMPLETED", events.getLast().eventType());
        }
    }

    @Test
    void backpressureAndCloseNeverDropWrites() {
        String tenant = "close-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
        var router = router(new MySqlTenantStoreRouter.WriteBehind(1, 1, 8));
        router.saveExecution(MySqlCapacityCounterTest.running(tenant, id));
        for (int sequence = 0; sequence < 100; sequence++)
            router.append(MySqlCapacityCounterTest.event(id, sequence));
        router.close();
        router.append(MySqlCapacityCounterTest.event(id, 100));
        assertEquals(101, new MySqlObservationStore(db, tenant).readAfter(id, -1).size());
    }

    @Test
    void asynchronousFailureIsReportedOnFlushWithoutLosingNeighbours() {
        try (var router = router(new MySqlTenantStoreRouter.WriteBehind(1, 10000, 256))) {
            String tenant = "failure-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
            router.saveExecution(MySqlCapacityCounterTest.running(tenant, id));
            router.append(MySqlCapacityCounterTest.event(id, 0));
            router.append(MySqlCapacityCounterTest.event(id, 0));
            router.append(MySqlCapacityCounterTest.event(id, 1));
            var failure = assertThrows(IllegalStateException.class, () -> router.flush(id));
            assertEquals("EVENT_SEQUENCE_CONFLICT", failure.getMessage());
            assertDoesNotThrow(() -> router.flush(id));
            assertEquals(
                    List.of(0L, 1L),
                    router.readAfter(id, -1).stream().map(ExecutionEvent::sequence).toList());
        }
    }

    private MySqlTenantStoreRouter router(MySqlTenantStoreRouter.WriteBehind options) {
        return new MySqlTenantStoreRouter(
                db, Map.of(), MySqlRetentionPolicy.defaults(), 1048576, Duration.ofHours(1), options);
    }

    private static NodeExecutionRecord node(String id, String node, ExecutionStatus status) {
        Instant now = Instant.now();
        return new NodeExecutionRecord(
                node,
                id,
                "node",
                "node",
                ComponentType.SCRIPT,
                "root",
                1,
                status,
                now,
                now,
                status == ExecutionStatus.RUNNING ? null : now,
                null,
                null,
                null,
                null);
    }

    private static ExecutionLogRecord log(String id, String node, long sequence) {
        return new ExecutionLogRecord(
                UUID.randomUUID().toString(),
                id,
                node,
                Instant.now(),
                ExecutionLogLevel.INFO,
                "engine",
                sequence,
                "node started",
                Map.of(),
                null);
    }

    private static MysqlDataSource plain() {
        var source = new MysqlDataSource();
        source.setURL(MYSQL.getJdbcUrl());
        source.setUser(MYSQL.getUsername());
        source.setPassword(MYSQL.getPassword());
        return source;
    }

    /** 统计真实 JDBC 提交次数，用于证明批量合并而不是逐条事务。 */
    private DataSource countingCommits(DataSource target) {
        return (DataSource)
                Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {DataSource.class},
                        (proxy, method, args) -> {
                            Object value = invoke(target, method, args);
                            if (value instanceof Connection connection)
                                return Proxy.newProxyInstance(
                                        getClass().getClassLoader(),
                                        new Class<?>[] {Connection.class},
                                        (p, m, a) -> {
                                            if (m.getName().equals("commit"))
                                                commits.incrementAndGet();
                                            return invoke(connection, m, a);
                                        });
                            return value;
                        });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (java.lang.reflect.InvocationTargetException failure) {
            throw failure.getCause();
        }
    }
}
