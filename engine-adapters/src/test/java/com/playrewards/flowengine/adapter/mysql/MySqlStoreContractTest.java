package com.playrewards.flowengine.adapter.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

/** Real MySQL contracts: no in-memory replacement or Docker-unavailable skip. */
class MySqlStoreContractTest {
    static final TestMySql MYSQL = TestMySql.start();
    MySqlRuntimeDatabase db;

    @BeforeEach
    void setup() {
        MysqlDataSource source = new MysqlDataSource();
        source.setURL(MYSQL.getJdbcUrl());
        source.setUser(MYSQL.getUsername());
        source.setPassword(MYSQL.getPassword());
        db = new MySqlRuntimeDatabase(source);
        db.migrate();
    }

    @Test
    void recoveryBatchExcludesLiveLeasesBeforeApplyingLimit() {
        String tenant = "recovery-" + UUID.randomUUID();
        var leases = new MySqlLeaseRepository(db, tenant);
        var states = new MySqlExecutionStateStore(db, tenant);
        for (String id : List.of("busy-1", "busy-2", "expired", "missing")) {
            var lease = leases.acquire(id, "owner", Duration.ofMinutes(5)).orElseThrow();
            states.save(new ExecutionState(id, 0, ExecutionStatus.RUNNING, Map.of()), 0, lease);
            if (id.equals("expired")) assertTrue(leases.release(lease));
        }
        // Older live executions must not consume the bounded scan; absent leases are eligible too.
        db.transaction(tx -> {
            tx.update("UPDATE engine_execution_state SET updated_at='2000-01-01'"
                    + " WHERE tenant_id=#{args[0]}", tenant);
            tx.update("UPDATE engine_execution_state SET updated_at='1999-01-01'"
                    + " WHERE tenant_id=#{args[0]} AND execution_id IN ('busy-1','busy-2')", tenant);
            tx.update("DELETE FROM engine_lease WHERE tenant_id=#{args[0]}"
                    + " AND execution_id='missing'", tenant);
            return null;
        });
        // An identically named execution in another tenant must not hide the missing lease.
        new MySqlLeaseRepository(db, tenant + "-other")
                .acquire("missing", "other", Duration.ofMinutes(5)).orElseThrow();
        var candidates = new MySqlTenantStoreRouter(db).recoverable(2);
        assertEquals(Set.of(
                new MySqlTenantStoreRouter.RecoveryCandidate(tenant, "expired"),
                new MySqlTenantStoreRouter.RecoveryCandidate(tenant, "missing")),
                new HashSet<>(candidates));
    }

    @Test
    void leasesFenceExpiredOwnersAndCheckpointCompareAndSet() throws Exception {
        String id = UUID.randomUUID().toString();
        var leases = new MySqlLeaseRepository(db, "tenant-a");
        var state = new MySqlExecutionStateStore(db, "tenant-a");
        var first = leases.acquire(id, "first", Duration.ofMillis(150)).orElseThrow();
        assertTrue(leases.acquire(id, "second", Duration.ofSeconds(5)).isEmpty());
        var saved =
                state.save(
                        new ExecutionState(
                                id, 0, ExecutionStatus.RUNNING, Map.of("ctx", Map.of("x", 7))),
                        0,
                        first);
        assertEquals(1, saved.revision());
        assertThrows(IllegalStateException.class, () -> state.save(saved, 0, first));
        var second = acquireAfterDatabaseExpiry(leases, id, "second", Duration.ofSeconds(5));
        assertTrue(second.fencingToken() > first.fencingToken());
        assertFalse(leases.heartbeat(first, Duration.ofSeconds(5)));
        assertThrows(IllegalStateException.class, () -> state.save(saved, 1, first));
        assertTrue(leases.heartbeat(second, Duration.ofSeconds(5)));
        assertTrue(
                state.transition(
                        id,
                        1,
                        ExecutionStatus.RUNNING,
                        ExecutionStatus.WAITING_RETRY,
                        Map.of("retryAt", "2030-01-01T00:00:00Z"),
                        second));
        assertFalse(
                state.transition(
                        id,
                        1,
                        ExecutionStatus.RUNNING,
                        ExecutionStatus.SUCCEEDED,
                        Map.of(),
                        second));
        assertTrue(new MySqlExecutionStateStore(db, "tenant-b").find(id).isEmpty());
    }

    private static ExecutionLease acquireAfterDatabaseExpiry(
            MySqlLeaseRepository leases, String id, String owner, Duration duration)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        Optional<ExecutionLease> acquired;
        do {
            acquired = leases.acquire(id, owner, duration);
            if (acquired.isPresent()) return acquired.orElseThrow();
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        return fail("database lease did not expire within 5 seconds");
    }

    @Test
    void leaseHeartbeatsUseReservedControlConnectionWhenBusinessPoolIsFull() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(1);
        source.setPoolMaximumIdleConnections(1);
        source.setPoolTimeToWait(5000);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            concurrentDb.migrate();
            String tenant = "heartbeat-control-" + UUID.randomUUID();
            var leases = new MySqlLeaseRepository(concurrentDb, tenant);
            List<ExecutionLease> activeLeases = new ArrayList<>();
            for (int index = 0; index < 50; index++)
                activeLeases.add(
                        leases.acquire(
                                        "execution-" + index,
                                        "owner",
                                        Duration.ofSeconds(15))
                                .orElseThrow());
            var occupied = new java.util.concurrent.CountDownLatch(1);
            var holder =
                    java.util.concurrent.CompletableFuture.runAsync(
                            () -> {
                                try (var ignored = source.getConnection()) {
                                    occupied.countDown();
                                    release.await();
                                } catch (Exception failure) {
                                    throw new IllegalStateException(failure);
                                }
                            },
                            callers);
            assertTrue(occupied.await(5, java.util.concurrent.TimeUnit.SECONDS));
            try {
                assertTrue(
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                        () ->
                                                activeLeases.stream()
                                                        .allMatch(
                                                                lease ->
                                                                        leases.heartbeat(
                                                                                lease,
                                                                                Duration.ofSeconds(
                                                                                        15))),
                                        callers)
                                .get(5, java.util.concurrent.TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
            holder.get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            release.countDown();
            source.forceCloseAll();
        }
    }

    @Test
    void idempotencyHasOneWinnerAndIsTenantIsolated() {
        var store = new MySqlIdempotencyStore(db);
        var key = new IdempotencyKey("tenant-a", UUID.randomUUID().toString());
        var winner = store.claim(new IdempotencyRecord(key, "first"));
        assertEquals(winner, store.claim(new IdempotencyRecord(key, "second")));
        assertTrue(store.find(new IdempotencyKey("tenant-b", key.value())).isEmpty());
    }

    @Test
    void terminalCommitRollsBackRecordWhenEventSequenceConflicts() {
        String id = UUID.randomUUID().toString();
        var store = new MySqlObservationStore(db, "tenant-a");
        var identity =
                new com.playrewards.flowengine.contract.bundle.FlowIdentity(
                        "tenant-a", "flow", "1");
        var initial =
                new ExecutionRecord(
                        id,
                        id,
                        null,
                        identity,
                        "hash",
                        com.playrewards.flowengine.contract.execution.ExecutionMode.DURABLE,
                        com.playrewards.flowengine.contract.execution.ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        Instant.now(),
                        null,
                        null,
                        null,
                        null,
                        -1);
        store.saveExecution(initial);
        var first =
                new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                        id + ":0",
                        id,
                        "STARTED",
                        ExecutionStatus.RUNNING,
                        null,
                        null,
                        null,
                        0,
                        0,
                        Instant.now(),
                        Map.of());
        store.append(first);
        var terminal =
                new ExecutionRecord(
                        id,
                        id,
                        null,
                        identity,
                        "hash",
                        initial.mode(),
                        initial.purpose(),
                        ExecutionStatus.SUCCEEDED,
                        initial.startedAt(),
                        Instant.now(),
                        null,
                        null,
                        null,
                        0);
        assertThrows(IllegalStateException.class, () -> store.commitTerminal(terminal, first));
        assertEquals(ExecutionStatus.RUNNING, store.execution(id).orElseThrow().status());
        var event =
                new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                        id + ":1",
                        id,
                        "COMPLETED",
                        ExecutionStatus.SUCCEEDED,
                        null,
                        null,
                        null,
                        0,
                        1,
                        Instant.now(),
                        Map.of());
        store.commitTerminal(terminal, event);
        assertEquals(
                ExecutionStatus.SUCCEEDED,
                new MySqlObservationStore(db, "tenant-a").execution(id).orElseThrow().status());
        assertEquals(
                List.of(0L, 1L),
                store.readAfter(id, -1).stream()
                        .map(com.playrewards.flowengine.contract.execution.ExecutionEvent::sequence)
                        .toList());
        assertTrue(new MySqlObservationStore(db, "tenant-b").execution(id).isEmpty());
    }

    @Test
    void terminalNodeAttemptsRemainQueryableWhileExecutionSummaryExists() {
        String tenant = "node-retention-" + UUID.randomUUID();
        String id = UUID.randomUUID().toString();
        String nodeId = UUID.randomUUID().toString();
        var store = new MySqlObservationStore(db, tenant);
        var identity =
                new com.playrewards.flowengine.contract.bundle.FlowIdentity(tenant, "flow", "1");
        Instant now = Instant.now();
        store.saveExecution(
                new ExecutionRecord(
                        id,
                        id,
                        null,
                        identity,
                        "hash",
                        com.playrewards.flowengine.contract.execution.ExecutionMode.DURABLE,
                        com.playrewards.flowengine.contract.execution.ExecutionPurpose.TRIAL,
                        ExecutionStatus.SUCCEEDED,
                        now,
                        now,
                        null,
                        null,
                        null,
                        -1));
        store.saveNode(
                new NodeExecutionRecord(
                        nodeId,
                        id,
                        "node",
                        "node",
                        com.playrewards.flowengine.contract.component.ComponentType.BUSINESS,
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
        // Simulate an old terminal attempt without waiting for the production retention window.
        db.transaction(
                tx -> {
                    tx.update(
                            "UPDATE engine_node_attempt SET updated_at='2000-01-01'"
                                    + " WHERE tenant_id=#{args[0]} AND node_execution_id=#{args[1]}",
                            tenant,
                            nodeId);
                    return null;
                });

        String triggerId = UUID.randomUUID().toString();
        store.saveExecution(
                new ExecutionRecord(
                        triggerId,
                        triggerId,
                        null,
                        identity,
                        "hash",
                        com.playrewards.flowengine.contract.execution.ExecutionMode.DURABLE,
                        com.playrewards.flowengine.contract.execution.ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        now,
                        null,
                        null,
                        null,
                        null,
                        -1));
        store.append(
                new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                        triggerId + ":0",
                        triggerId,
                        "STARTED",
                        ExecutionStatus.RUNNING,
                        null,
                        null,
                        null,
                        0,
                        0,
                        now,
                        Map.of()));

        assertTrue(store.execution(id).isPresent());
        assertEquals(
                List.of(nodeId),
                store.nodes(id).stream().map(NodeExecutionRecord::nodeExecutionId).toList());
    }

    @Test
    void payloadChecksDigestTenantExpiryAndHardSizeBound() throws Exception {
        var payloads = new MySqlPayloadStore(db, "tenant-a", 1024, Duration.ofMillis(150));
        var ref = payloads.store("execution", Map.of("amount", 23));
        assertEquals(Map.of("amount", 23), payloads.find(ref).orElseThrow());
        assertEquals("execution", payloads.executionId(ref).orElseThrow());
        assertTrue(
                new MySqlPayloadStore(db, "tenant-b", 1024, Duration.ofHours(1))
                        .find(ref)
                        .isEmpty());
        assertThrows(
                IllegalArgumentException.class,
                () -> payloads.store("execution", "x".repeat(1025)));
        Thread.sleep(180);
        assertTrue(payloads.find(ref).isEmpty());
    }

    @Test
    void checkpointsKeepOnlyTwoRevisionsAndReferencedPinnedPayloads() {
        String tenant = "retention-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
        var state = new MySqlExecutionStateStore(db, tenant);
        var lease =
                new MySqlLeaseRepository(db, tenant)
                        .acquire(id, "owner", Duration.ofSeconds(5))
                        .orElseThrow();
        var payloads = new MySqlPayloadStore(db, tenant, 1024, Duration.ofMillis(1));
        List<PayloadReference> refs = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            var ref = payloads.storeCheckpoint(id, Map.of("index", index));
            refs.add(ref);
            state.save(
                    new ExecutionState(
                            id, index, ExecutionStatus.RUNNING, Map.of("document", ref.value())),
                    index,
                    lease);
        }
        assertTrue(payloads.find(refs.getFirst()).isEmpty());
        assertTrue(payloads.find(refs.get(3)).isPresent());
        assertTrue(payloads.find(refs.getLast()).isPresent());
        long rows =
                db.transaction(
                        tx ->
                                MySqlRuntimeDatabase.number(
                                        tx.row(
                                                        "SELECT COUNT(*) AS n FROM"
                                                                + " engine_checkpoint WHERE"
                                                                + " tenant_id=#{args[0]} AND"
                                                                + " execution_id=#{args[1]}",
                                                        tenant,
                                                        id)
                                                .orElseThrow(),
                                        "n"));
        assertEquals(2, rows);
    }

    @Test
    void oldOwnerCannotCommitTerminalAfterLeaseTransfer() throws Exception {
        String tenant = "audit-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
        var leases = new MySqlLeaseRepository(db, tenant);
        var old = leases.acquire(id, "old", Duration.ofMillis(50)).orElseThrow();
        var store = new MySqlObservationStore(db, tenant);
        var identity =
                new com.playrewards.flowengine.contract.bundle.FlowIdentity(tenant, "flow", "1");
        var initial =
                new ExecutionRecord(
                        id,
                        id,
                        null,
                        identity,
                        "hash",
                        com.playrewards.flowengine.contract.execution.ExecutionMode.DURABLE,
                        com.playrewards.flowengine.contract.execution.ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        Instant.now(),
                        null,
                        null,
                        null,
                        null,
                        -1);
        store.saveExecution(initial);
        Thread.sleep(80);
        var fresh = leases.acquire(id, "new", Duration.ofSeconds(5)).orElseThrow();
        var terminal =
                new ExecutionRecord(
                        id,
                        id,
                        null,
                        identity,
                        "hash",
                        initial.mode(),
                        initial.purpose(),
                        ExecutionStatus.SUCCEEDED,
                        initial.startedAt(),
                        Instant.now(),
                        null,
                        null,
                        null,
                        0);
        var event =
                new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                        id + ":0",
                        id,
                        "COMPLETED",
                        ExecutionStatus.SUCCEEDED,
                        null,
                        null,
                        null,
                        0,
                        0,
                        Instant.now(),
                        Map.of());
        assertThrows(IllegalStateException.class, () -> store.commitTerminal(terminal, event, old));
        assertEquals(ExecutionStatus.RUNNING, store.execution(id).orElseThrow().status());
        store.commitTerminal(terminal, event, fresh);
        assertEquals(ExecutionStatus.SUCCEEDED, store.execution(id).orElseThrow().status());
    }

    @Test
    void businessLockIsSharedAndOldFenceCannotReleaseTransferredLock() throws Exception {
        var one = new MySqlDistributedLockProvider(db);
        var two = new MySqlDistributedLockProvider(db);
        String name = "tenant:dGVuYW50LWE:flow:Zmxvdw:version:MQ:key:" + UUID.randomUUID();
        var old =
                one.tryAcquire(new LockRequest(name, "first", Duration.ofMillis(100)))
                        .orElseThrow();
        assertTrue(
                two.tryAcquire(new LockRequest(name, "second", Duration.ofSeconds(5))).isEmpty());
        Thread.sleep(130);
        var fresh =
                two.tryAcquire(new LockRequest(name, "second", Duration.ofSeconds(5)))
                        .orElseThrow();
        assertTrue(fresh.fencingToken() > old.fencingToken());
        assertFalse(one.release(old));
        assertTrue(two.release(fresh));
    }

    @Test
    void observationCapacityRejectsNewEventsAndDuplicateAttemptIdentity() {
        String tenant = "bounds-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
        var store = new MySqlObservationStore(db, tenant, 2, Duration.ofHours(1));
        var identity =
                new com.playrewards.flowengine.contract.bundle.FlowIdentity(tenant, "flow", "1");
        store.saveExecution(
                new ExecutionRecord(
                        id,
                        id,
                        null,
                        identity,
                        "hash",
                        com.playrewards.flowengine.contract.execution.ExecutionMode.DURABLE,
                        com.playrewards.flowengine.contract.execution.ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        Instant.now(),
                        null,
                        null,
                        null,
                        null,
                        -1));
        for (int n = 0; n < 2; n++)
            store.append(
                    new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                            id + ":" + n,
                            id,
                            "STARTED",
                            ExecutionStatus.RUNNING,
                            null,
                            null,
                            null,
                            0,
                            n,
                            Instant.now(),
                            Map.of()));
        assertThrows(
                IllegalStateException.class,
                () ->
                        store.append(
                                new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                                        id + ":2",
                                        id,
                                        "STARTED",
                                        ExecutionStatus.RUNNING,
                                        null,
                                        null,
                                        null,
                                        0,
                                        2,
                                        Instant.now(),
                                        Map.of())));
        var first =
                new NodeExecutionRecord(
                        "n1",
                        id,
                        "node",
                        "node",
                        com.playrewards.flowengine.contract.component.ComponentType.BUSINESS,
                        "root",
                        1,
                        ExecutionStatus.RUNNING,
                        Instant.now(),
                        Instant.now(),
                        null,
                        null,
                        null,
                        null,
                        null);
        store.saveNode(first);
        var conflict =
                new NodeExecutionRecord(
                        "n2",
                        id,
                        "node",
                        "node",
                        first.componentType(),
                        "root",
                        1,
                        ExecutionStatus.RUNNING,
                        first.queuedAt(),
                        first.startedAt(),
                        null,
                        null,
                        null,
                        null,
                        null);
        assertThrows(IllegalStateException.class, () -> store.saveNode(conflict));
        assertEquals(1, store.nodes(id).size());
    }

    @Test
    void boundedPoolSupportsOneHundredConcurrentSimpleTransactions() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(16);
        source.setPoolMaximumIdleConnections(4);
        source.setPoolTimeToWait(5000);
        try {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            runOneHundredConcurrent(
                    value ->
                            concurrentDb.transaction(
                                    tx ->
                                            tx.row("SELECT #{args[0]} AS n", value)
                                                    .orElseThrow()));
        } finally {
            source.forceCloseAll();
        }
    }

    @Test
    void boundedPoolSerializesOneHundredConcurrentTenantCapacityLocks() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(16);
        source.setPoolMaximumIdleConnections(4);
        source.setPoolTimeToWait(5000);
        try {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            String tenant = "concurrent-lock-" + UUID.randomUUID();
            runOneHundredConcurrent(
                    value ->
                            concurrentDb.transaction(
                                    tx -> {
                                        MySqlCapacity.lock(tx, tenant);
                                        return null;
                                    }));
        } finally {
            source.forceCloseAll();
        }
    }

    @Test
    void boundedPoolSupportsOneHundredConcurrentPinnedPayloadWrites() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(16);
        source.setPoolMaximumIdleConnections(4);
        source.setPoolTimeToWait(5000);
        try {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            String tenant = "concurrent-payload-" + UUID.randomUUID();
            var payloads =
                    new MySqlPayloadStore(
                            concurrentDb, tenant, 1024, Duration.ofMinutes(1));
            var ready = new java.util.concurrent.CountDownLatch(100);
            var start = new java.util.concurrent.CountDownLatch(1);
            try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                List<java.util.concurrent.CompletableFuture<PayloadReference>> writes =
                        new ArrayList<>();
                for (int index = 0; index < 100; index++) {
                    int value = index;
                    writes.add(
                            java.util.concurrent.CompletableFuture.supplyAsync(
                                    () -> {
                                        ready.countDown();
                                        try {
                                            start.await();
                                        } catch (InterruptedException failure) {
                                            Thread.currentThread().interrupt();
                                            throw new IllegalStateException(failure);
                                        }
                                        return payloads.storeCheckpoint(
                                                "execution-" + value, Map.of("value", value));
                                    },
                                    callers));
                }
                assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
                start.countDown();
                java.util.concurrent.CompletableFuture.allOf(
                                writes.toArray(java.util.concurrent.CompletableFuture[]::new))
                        .get(30, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(100, writes.stream().map(java.util.concurrent.CompletableFuture::join).distinct().count());
            }
        } finally {
            source.forceCloseAll();
        }
    }

    @Test
    void concurrentCheckpointCleanupDoesNotDeadlockWithPinnedPayloadInserts() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(16);
        source.setPoolMaximumIdleConnections(4);
        source.setPoolTimeToWait(5000);
        try {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            String tenant = "concurrent-checkpoint-" + UUID.randomUUID();
            var payloads =
                    new MySqlPayloadStore(
                            concurrentDb, tenant, 1024, Duration.ofMinutes(1));
            var states = new MySqlExecutionStateStore(concurrentDb, tenant);
            var leases = new MySqlLeaseRepository(concurrentDb, tenant);
            List<ExecutionLease> executionLeases = new ArrayList<>();
            for (int index = 0; index < 100; index++) {
                String executionId = "execution-" + index;
                var lease =
                        leases.acquire(executionId, "owner", Duration.ofMinutes(1)).orElseThrow();
                executionLeases.add(lease);
                var ref = payloads.storeCheckpoint(executionId, Map.of("revision", 1));
                states.save(
                        new ExecutionState(
                                executionId,
                                0,
                                ExecutionStatus.RUNNING,
                                Map.of("document", ref.value())),
                        0,
                        lease);
            }

            for (int revision = 2; revision <= 5; revision++) {
                int currentRevision = revision;
                runOneHundredConcurrent(
                        index -> {
                            String executionId = "execution-" + index;
                            var ref =
                                    payloads.storeCheckpoint(
                                            executionId, Map.of("revision", currentRevision));
                            states.save(
                                    new ExecutionState(
                                            executionId,
                                            currentRevision - 1,
                                            ExecutionStatus.RUNNING,
                                            Map.of("document", ref.value())),
                                    currentRevision - 1,
                                    executionLeases.get(index));
                        });
            }

            for (int index = 0; index < 100; index++)
                assertEquals(5, states.find("execution-" + index).orElseThrow().revision());
        } finally {
            source.forceCloseAll();
        }
    }

    @Test
    void checkpointPayloadCleanupSharesTenantCapacityMutex() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(4);
        source.setPoolMaximumIdleConnections(2);
        source.setPoolTimeToWait(5000);
        var locked = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            String tenant = "checkpoint-mutex-" + UUID.randomUUID();
            String executionId = UUID.randomUUID().toString();
            var states = new MySqlExecutionStateStore(concurrentDb, tenant);
            var lease =
                    new MySqlLeaseRepository(concurrentDb, tenant)
                            .acquire(executionId, "owner", Duration.ofMinutes(1))
                            .orElseThrow();
            var payloads =
                    new MySqlPayloadStore(
                            concurrentDb, tenant, 1024, Duration.ofMinutes(1));
            var first = payloads.storeCheckpoint(executionId, Map.of("revision", 1));
            states.save(
                    new ExecutionState(
                            executionId,
                            0,
                            ExecutionStatus.RUNNING,
                            Map.of("document", first.value())),
                    0,
                    lease);

            var holder =
                    java.util.concurrent.CompletableFuture.runAsync(
                            () ->
                                    concurrentDb.transaction(
                                            tx -> {
                                                MySqlCapacity.lock(tx, tenant);
                                                locked.countDown();
                                                try {
                                                    release.await();
                                                } catch (InterruptedException failure) {
                                                    Thread.currentThread().interrupt();
                                                    throw new IllegalStateException(failure);
                                                }
                                                return null;
                                            }),
                            callers);
            assertTrue(locked.await(5, java.util.concurrent.TimeUnit.SECONDS));
            var cleanup =
                    java.util.concurrent.CompletableFuture.runAsync(
                            () ->
                                    states.save(
                                            new ExecutionState(
                                                    executionId,
                                                    1,
                                                    ExecutionStatus.RUNNING,
                                                    Map.of("document", first.value())),
                                            1,
                                            lease),
                            callers);
            try {
                assertThrows(
                        java.util.concurrent.TimeoutException.class,
                        () -> cleanup.get(200, java.util.concurrent.TimeUnit.MILLISECONDS));
            } finally {
                release.countDown();
            }
            holder.get(5, java.util.concurrent.TimeUnit.SECONDS);
            cleanup.get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            release.countDown();
            source.forceCloseAll();
        }
    }

    @Test
    void tenantCapacityWaitersDoNotExhaustBusinessConnections() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(3);
        source.setPoolMaximumIdleConnections(1);
        source.setPoolTimeToWait(5000);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            String tenant = "capacity-pool-" + UUID.randomUUID();
            var locked = new java.util.concurrent.CountDownLatch(1);
            var holder =
                    java.util.concurrent.CompletableFuture.runAsync(
                            () ->
                                    concurrentDb.transaction(
                                            tx -> {
                                                MySqlCapacity.lock(tx, tenant);
                                                locked.countDown();
                                                try {
                                                    release.await();
                                                } catch (InterruptedException failure) {
                                                    Thread.currentThread().interrupt();
                                                    throw new IllegalStateException(failure);
                                                }
                                                return null;
                                            }),
                            callers);
            assertTrue(locked.await(5, java.util.concurrent.TimeUnit.SECONDS));
            var payloads =
                    new MySqlPayloadStore(
                            concurrentDb, tenant, 1024, Duration.ofMinutes(1));
            List<java.util.concurrent.CompletableFuture<PayloadReference>> writers =
                    new ArrayList<>();
            for (int index = 0; index < 8; index++) {
                int value = index;
                writers.add(
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                () ->
                                        payloads.storeCheckpoint(
                                                "execution-" + value, Map.of("value", value)),
                                callers));
            }
            try {
                long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
                while (source.getPoolState().getActiveConnectionCount() < 2
                        && System.nanoTime() < until) Thread.onSpinWait();
                Thread.sleep(100);
                assertEquals(2, source.getPoolState().getActiveConnectionCount());
                assertEquals(
                        1L,
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                        () ->
                                                concurrentDb.transaction(
                                                        tx ->
                                                                MySqlRuntimeDatabase.number(
                                                                        tx.row("SELECT 1 AS n")
                                                                                .orElseThrow(),
                                                                        "n")),
                                        callers)
                                .get(1, java.util.concurrent.TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
            holder.get(5, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.CompletableFuture.allOf(
                            writers.toArray(java.util.concurrent.CompletableFuture[]::new))
                    .get(10, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            release.countDown();
            source.forceCloseAll();
        }
    }

    @Test
    void tenantCapacityAdmissionRejectsWorkAboveTheBoundedWaiterLimit() throws Exception {
        var source =
                new org.apache.ibatis.datasource.pooled.PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL.getJdbcUrl(),
                        MYSQL.getUsername(),
                        MYSQL.getPassword());
        source.setPoolMaximumActiveConnections(3);
        source.setPoolMaximumIdleConnections(1);
        source.setPoolTimeToWait(5000);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var concurrentDb = new MySqlRuntimeDatabase(source);
            String tenant = "capacity-admission-" + UUID.randomUUID();
            var held = new java.util.concurrent.CountDownLatch(1);
            var holder =
                    java.util.concurrent.CompletableFuture.runAsync(
                            () ->
                                    concurrentDb.capacityTransaction(
                                            tenant,
                                            tx -> {
                                                held.countDown();
                                                try {
                                                    release.await();
                                                } catch (InterruptedException failure) {
                                                    Thread.currentThread().interrupt();
                                                    throw new IllegalStateException(failure);
                                                }
                                                return null;
                                            }),
                            callers);
            assertTrue(held.await(5, java.util.concurrent.TimeUnit.SECONDS));

            var ready = new java.util.concurrent.CountDownLatch(256);
            var start = new java.util.concurrent.CountDownLatch(1);
            List<java.util.concurrent.CompletableFuture<String>> contenders = new ArrayList<>();
            for (int index = 0; index < 256; index++) {
                contenders.add(
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                () -> {
                                    ready.countDown();
                                    try {
                                        start.await();
                                        concurrentDb.capacityTransaction(tenant, tx -> null);
                                        return "accepted";
                                    } catch (InterruptedException failure) {
                                        Thread.currentThread().interrupt();
                                        throw new IllegalStateException(failure);
                                    } catch (IllegalStateException rejected) {
                                        return rejected.getMessage();
                                    }
                                },
                                callers));
            }
            assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            long rejectionDeadline =
                    System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (contenders.stream().noneMatch(java.util.concurrent.CompletableFuture::isDone)
                    && System.nanoTime() < rejectionDeadline) Thread.onSpinWait();
            try {
                assertTrue(
                        contenders.stream()
                                .filter(java.util.concurrent.CompletableFuture::isDone)
                                .map(java.util.concurrent.CompletableFuture::join)
                                .anyMatch("RUNTIME_STORAGE_CAPACITY_BUSY"::equals));
            } finally {
                release.countDown();
            }
            holder.get(5, java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.CompletableFuture.allOf(
                            contenders.toArray(java.util.concurrent.CompletableFuture[]::new))
                    .get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(
                    1,
                    contenders.stream()
                            .map(java.util.concurrent.CompletableFuture::join)
                            .filter("RUNTIME_STORAGE_CAPACITY_BUSY"::equals)
                            .count());
        } finally {
            release.countDown();
            source.forceCloseAll();
        }
    }

    @Test
    void interruptedTenantCapacityWaiterLeavesWithoutEnteringTheTransaction() throws Exception {
        MysqlDataSource source = new MysqlDataSource();
        source.setURL(MYSQL.getJdbcUrl());
        source.setUser(MYSQL.getUsername());
        source.setPassword(MYSQL.getPassword());
        var concurrentDb = new MySqlRuntimeDatabase(source);
        String tenant = "capacity-interrupt-" + UUID.randomUUID();
        var held = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var outcome = new java.util.concurrent.atomic.AtomicReference<String>();
        Thread holder =
                Thread.ofVirtual()
                        .start(
                                () ->
                                        concurrentDb.capacityTransaction(
                                                tenant,
                                                tx -> {
                                                    held.countDown();
                                                    try {
                                                        release.await();
                                                    } catch (InterruptedException failure) {
                                                        Thread.currentThread().interrupt();
                                                        throw new IllegalStateException(failure);
                                                    }
                                                    return null;
                                                }));
        assertTrue(held.await(5, java.util.concurrent.TimeUnit.SECONDS));
        Thread waiter =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    try {
                                        concurrentDb.capacityTransaction(tenant, tx -> null);
                                        outcome.set("entered");
                                    } catch (IllegalStateException failure) {
                                        outcome.set(failure.getMessage());
                                    }
                                });
        long waitingDeadline =
                System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        while (waiter.getState() != Thread.State.WAITING
                && System.nanoTime() < waitingDeadline) Thread.onSpinWait();
        try {
            assertEquals(Thread.State.WAITING, waiter.getState());
            waiter.interrupt();
            waiter.join(500);
            assertFalse(waiter.isAlive());
            assertEquals("RUNTIME_STORAGE_CAPACITY_INTERRUPTED", outcome.get());
        } finally {
            release.countDown();
            holder.join(5000);
            waiter.join(5000);
        }
    }

    private static void runOneHundredConcurrent(java.util.function.IntConsumer operation)
            throws Exception {
        var ready = new java.util.concurrent.CountDownLatch(100);
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var callers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.CompletableFuture<Void>> calls = new ArrayList<>();
            for (int index = 0; index < 100; index++) {
                int value = index;
                calls.add(
                        java.util.concurrent.CompletableFuture.runAsync(
                                () -> {
                                    ready.countDown();
                                    try {
                                        start.await();
                                    } catch (InterruptedException failure) {
                                        Thread.currentThread().interrupt();
                                        throw new IllegalStateException(failure);
                                    }
                                    operation.accept(value);
                                },
                                callers));
            }
            assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            java.util.concurrent.CompletableFuture.allOf(
                            calls.toArray(java.util.concurrent.CompletableFuture[]::new))
                    .get(30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void terminalCheckpointCannotBeReopenedByCasOrSave() {
        String tenant = "terminal-" + UUID.randomUUID(), id = UUID.randomUUID().toString();
        var store = new MySqlExecutionStateStore(db, tenant);
        var lease =
                new MySqlLeaseRepository(db, tenant)
                        .acquire(id, "owner", Duration.ofSeconds(5))
                        .orElseThrow();
        store.save(new ExecutionState(id, 0, ExecutionStatus.RUNNING, Map.of()), 0, lease);
        assertTrue(
                store.transition(
                        id,
                        1,
                        ExecutionStatus.RUNNING,
                        ExecutionStatus.SUCCEEDED,
                        Map.of(),
                        lease));
        assertFalse(
                store.transition(
                        id,
                        2,
                        ExecutionStatus.SUCCEEDED,
                        ExecutionStatus.RUNNING,
                        Map.of(),
                        lease));
        assertThrows(
                IllegalStateException.class,
                () ->
                        store.save(
                                new ExecutionState(id, 2, ExecutionStatus.RUNNING, Map.of()),
                                2,
                                lease));
    }
}
