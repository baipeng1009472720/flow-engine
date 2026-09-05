package com.playrewards.flowengine.adapter.mysql;

import static org.junit.jupiter.api.Assertions.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.*;

import java.time.*;
import java.util.*;

/** Real MySQL contracts: no in-memory replacement or Docker-unavailable skip. */
@Testcontainers
class MySqlStoreContractTest {
    @Container static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4");
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
        Thread.sleep(180);
        var second = leases.acquire(id, "second", Duration.ofSeconds(5)).orElseThrow();
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
