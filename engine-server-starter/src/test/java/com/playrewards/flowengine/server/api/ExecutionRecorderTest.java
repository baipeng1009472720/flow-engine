package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.adapter.groovy.SandboxedGroovyRuntime;
import com.playrewards.flowengine.adapter.memory.InMemoryObservationStore;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.core.idempotency.IdempotencyCoordinator;
import com.playrewards.flowengine.core.node.*;
import com.playrewards.flowengine.core.node.builtin.HttpNodeExecutor;
import com.playrewards.flowengine.core.observation.ExecutionRecorder;
import com.playrewards.flowengine.core.retry.RetryPolicy;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real scheduler and HTTP retry policy evidence, with only the network response selected
 * deterministically.
 */
class ExecutionRecorderTest {
    @Test
    void error_log_failure_does_not_replace_the_already_observed_business_cause() {
        var store = new InMemoryObservationStore();
        var records = faultStore(store, "appendLog");
        var recorder =
                new ExecutionRecorder(
                        records,
                        store,
                        new BoundedExecutionEventBus(records, 2, 32),
                        Clock.systemUTC(),
                        true);
        var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
        recorder.created(
                "business-first",
                plan,
                new ExecutionRequest(
                        "test:flow",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        Map.of(),
                        Map.of(),
                        null,
                        "key",
                        "tenant",
                        Instant.now().plusSeconds(5),
                        Map.of()),
                null);
        var attempt =
                recorder.queued("business-first", plan.nodesById().get("start"), ScopePath.root());
        var failure = new NodeExecutionException("BUSINESS_REJECTED", "private");
        assertDoesNotThrow(() -> attempt.failed(failure));
        var result =
                recorder.finished(
                        recorder.identify(
                                new ExecutionResult(
                                        "business-first",
                                        ExecutionStatus.FAILED,
                                        null,
                                        "BUSINESS_REJECTED",
                                        "safe",
                                        null,
                                        Instant.now(),
                                        Instant.now()),
                                failure));
        assertEquals("BUSINESS_REJECTED", result.errorCode());
        assertEquals(store.nodes("business-first").getFirst().errorId(), result.errorId());
    }

    @Test
    void repeated_failed_admissions_are_reclaimed_under_a_one_execution_budget() {
        var now = new java.util.concurrent.atomic.AtomicReference<>(Instant.now());
        Clock clock =
                new Clock() {
                    public ZoneId getZone() {
                        return ZoneOffset.UTC;
                    }

                    public Clock withZone(ZoneId zone) {
                        return this;
                    }

                    public Instant instant() {
                        return now.get();
                    }
                };
        var store = new InMemoryObservationStore(1, 32, Duration.ofSeconds(1), clock);
        var records = faultStore(store, "append");
        var recorder =
                new ExecutionRecorder(
                        records, store, new BoundedExecutionEventBus(records, 2, 16), clock, true);
        var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
        for (int index = 0; index < 4; index++) {
            String id = "failed-admission-" + index;
            assertThrows(
                    IllegalStateException.class,
                    () ->
                            recorder.created(
                                    id,
                                    plan,
                                    new ExecutionRequest(
                                            "test:flow",
                                            ExecutionMode.EPHEMERAL,
                                            ExecutionPurpose.TRIAL,
                                            Map.of(),
                                            Map.of(),
                                            null,
                                            id,
                                            "tenant",
                                            now.get().plusSeconds(5),
                                            Map.of()),
                                    null));
            assertNotNull(store.execution(id).orElseThrow().finishedAt());
            now.set(now.get().plusSeconds(2));
            assertTrue(store.execution(id).isEmpty());
        }
    }

    @Test
    void oversized_content_metadata_is_rejected_before_drain_or_store_side_effects() {
        var chunk = new ContentChunk("content-size", "start", "root", 0, "x".repeat(70000), null);
        var accepted = new AtomicInteger();
        var cancelled = new AtomicInteger();
        try (var drain = new ContentDrain(ignored -> accepted.incrementAndGet())) {
            drain.onSubscribe(
                    new Flow.Subscription() {
                        public void request(long count) {}

                        public void cancel() {
                            cancelled.incrementAndGet();
                        }
                    });
            drain.onNext(chunk);
            assertEquals(0, accepted.get());
            assertEquals(1, cancelled.get());
        }
        var store = new InMemoryObservationStore();
        var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
        var recorder =
                new ExecutionRecorder(
                        store,
                        store,
                        new BoundedExecutionEventBus(store, 2, 16),
                        Clock.systemUTC(),
                        true);
        recorder.created(
                "content-size",
                plan,
                new ExecutionRequest(
                        "test:flow",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        Map.of(),
                        Map.of(),
                        null,
                        "key",
                        "tenant",
                        Instant.now().plusSeconds(5),
                        Map.of()),
                null);
        recorder.queued("content-size", plan.nodesById().get("start"), ScopePath.root());
        String node = store.nodes("content-size").getFirst().nodeExecutionId();
        assertThrows(IllegalArgumentException.class, () -> store.appendContent(node, chunk));
        assertTrue(store.content(node).isEmpty());
    }

    @Test
    void failed_created_append_and_terminal_payload_do_not_leave_active_or_unfinished_rows()
            throws Exception {
        for (String fault : List.of("append", "payload")) {
            var store = new InMemoryObservationStore();
            ExecutionRecordStore records = faultStore(store, fault);
            PayloadStore payloads =
                    fault.equals("payload")
                            ? new PayloadStore() {
                                public PayloadReference store(String id, Object value) {
                                    throw new IllegalStateException("private");
                                }

                                public Optional<Object> find(PayloadReference ref) {
                                    return Optional.empty();
                                }
                            }
                            : store;
            var recorder =
                    new ExecutionRecorder(
                            records,
                            payloads,
                            new BoundedExecutionEventBus(records, 2, 32),
                            Clock.systemUTC(),
                            true);
            var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
            var request =
                    new ExecutionRequest(
                            "test:flow",
                            ExecutionMode.EPHEMERAL,
                            ExecutionPurpose.TRIAL,
                            Map.of(),
                            Map.of(),
                            null,
                            "key",
                            "tenant",
                            Instant.now().plusSeconds(5),
                            Map.of());
            try {
                recorder.created(fault, plan, request, null);
                recorder.finished(
                        new ExecutionResult(
                                fault,
                                ExecutionStatus.SUCCEEDED,
                                "value",
                                null,
                                null,
                                null,
                                Instant.now(),
                                Instant.now()));
            } catch (IllegalStateException expected) {
            }
            var active = ExecutionRecorder.class.getDeclaredField("active");
            active.setAccessible(true);
            assertTrue(((Map<?, ?>) active.get(recorder)).isEmpty(), fault);
            assertNotNull(store.execution(fault).orElseThrow().finishedAt(), fault);
            assertEquals(ExecutionStatus.FAILED, store.execution(fault).orElseThrow().status());
        }
    }

    @Test
    void strict_flush_failure_never_exposes_success_terminal() throws Exception {
        var store = new InMemoryObservationStore();
        var records = faultStore(store, "flush");
        var recorder =
                new ExecutionRecorder(
                        records,
                        store,
                        new BoundedExecutionEventBus(records, 2, 64),
                        Clock.systemUTC(),
                        true);
        try (var scheduler =
                new DagScheduler(
                        NodeExecutorRegistry.builtIns(),
                        ConcurrencyGovernor.defaults(),
                        Clock.systemUTC(),
                        recorder)) {
            var result =
                    scheduler
                            .schedule(
                                    new FlowCompiler()
                                            .compile(ExecutionRequestContractTest.bundle()),
                                    new ExecutionRequest(
                                            "test:flow",
                                            ExecutionMode.EPHEMERAL,
                                            ExecutionPurpose.TRIAL,
                                            Map.of(),
                                            Map.of(),
                                            null,
                                            "key",
                                            "tenant",
                                            Instant.now().plusSeconds(5),
                                            Map.of()))
                            .completion()
                            .toCompletableFuture()
                            .get(5, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals(
                    result.status(), store.execution(result.executionId()).orElseThrow().status());
            assertEquals("OBSERVATION_AUDIT_FAILED", result.errorCode());
            assertTrue(
                    store.readAfter(result.executionId(), -1).stream()
                            .noneMatch(e -> e.eventType().equals("COMPLETED")));
        }
    }

    private ExecutionRecordStore faultStore(InMemoryObservationStore store, String fault) {
        return (ExecutionRecordStore)
                java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {ExecutionRecordStore.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals(fault))
                                throw new IllegalStateException("private");
                            try {
                                return method.invoke(store, args);
                            } catch (java.lang.reflect.InvocationTargetException failure) {
                                throw failure.getCause();
                            }
                        });
    }

    @Test
    void independent_same_code_failures_keep_distinct_identity_and_selected_cause_wins() {
        var store = new InMemoryObservationStore();
        var recorder =
                new ExecutionRecorder(
                        store,
                        store,
                        new BoundedExecutionEventBus(store, 2, 32),
                        Clock.systemUTC(),
                        true);
        var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
        var request =
                new ExecutionRequest(
                        "test:flow",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        Map.of(),
                        Map.of(),
                        null,
                        "key",
                        "tenant",
                        Instant.now().plusSeconds(5),
                        Map.of());
        recorder.created("two-causes", plan, request, null);
        var first = recorder.queued("two-causes", plan.nodesById().get("start"), ScopePath.root());
        var second =
                recorder.queued(
                        "two-causes",
                        plan.nodesById().get("start"),
                        ScopePath.root().child("branch-2"));
        var earlier = new NodeExecutionException("SAME_CODE", "private earlier");
        var winner = new NodeExecutionException("SAME_CODE", "private winner");
        first.failed(earlier);
        second.failed(winner);
        var nodes = store.nodes("two-causes");
        assertNotEquals(nodes.get(0).errorId(), nodes.get(1).errorId());
        assertNotEquals(nodes.get(0).scopePath(), nodes.get(1).scopePath());
        var selected =
                recorder.identify(
                        new ExecutionResult(
                                "two-causes",
                                ExecutionStatus.FAILED,
                                null,
                                "SAME_CODE",
                                "safe",
                                null,
                                Instant.now(),
                                Instant.now()),
                        winner);
        var completed = recorder.finished(selected);
        assertEquals(nodes.get(1).errorId(), completed.errorId());
        assertEquals(completed.errorId(), store.execution("two-causes").orElseThrow().errorId());
        var repeated =
                recorder.finished(
                        new ExecutionResult(
                                "two-causes",
                                ExecutionStatus.SUCCEEDED,
                                null,
                                null,
                                null,
                                null,
                                Instant.now(),
                                Instant.now()));
        assertEquals(completed.status(), repeated.status(), "terminal reentry must be stable");
        assertEquals(completed.errorId(), repeated.errorId());
    }

    @Test
    void observation_admission_failure_does_not_leak_the_acquired_node_permit() throws Exception {
        var store = new InMemoryObservationStore();
        ExecutionRecordStore rejecting =
                (ExecutionRecordStore)
                        java.lang.reflect.Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {ExecutionRecordStore.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("saveNode"))
                                        throw new IllegalStateException(
                                                "NODE_RECORD_CAPACITY_EXCEEDED");
                                    try {
                                        return method.invoke(store, args);
                                    } catch (java.lang.reflect.InvocationTargetException failure) {
                                        throw failure.getCause();
                                    }
                                });
        var recorder =
                new ExecutionRecorder(
                        rejecting,
                        store,
                        new BoundedExecutionEventBus(rejecting, 2, 8),
                        Clock.systemUTC(),
                        false);
        try (var scheduler =
                new DagScheduler(
                        NodeExecutorRegistry.builtIns(),
                        ConcurrencyGovernor.defaults(),
                        Clock.systemUTC(),
                        recorder)) {
            var field = DagScheduler.class.getDeclaredField("nodeTaskAdmission");
            field.setAccessible(true);
            var permits = (Semaphore) field.get(scheduler);
            int before = permits.availablePermits();
            scheduler
                    .schedule(
                            new FlowCompiler().compile(ExecutionRequestContractTest.bundle()),
                            new ExecutionRequest(
                                    "test:flow",
                                    ExecutionMode.EPHEMERAL,
                                    ExecutionPurpose.TRIAL,
                                    Map.of(),
                                    Map.of(),
                                    null,
                                    "key",
                                    "tenant",
                                    Instant.now().plusSeconds(5),
                                    Map.of()))
                    .completion()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            assertEquals(
                    before,
                    permits.availablePermits(),
                    "observation capacity rejection must release the already acquired submission"
                            + " permit");
        }
    }

    @Test
    void lifecycle_events_reference_content_without_exposing_the_private_value() {
        var store = new InMemoryObservationStore();
        var recorder =
                new ExecutionRecorder(
                        store,
                        store,
                        new BoundedExecutionEventBus(store, 2, 8),
                        Clock.systemUTC(),
                        false);
        var plan = new FlowCompiler().compile(ExecutionRequestContractTest.bundle());
        var request =
                new ExecutionRequest(
                        "test:flow",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        Map.of(),
                        Map.of(),
                        null,
                        "key",
                        "tenant",
                        Instant.now().plusSeconds(5),
                        Map.of());
        recorder.created("content-run", plan, request, null);
        var attempt =
                recorder.queued("content-run", plan.nodesById().get("start"), ScopePath.root());
        attempt.started();
        attempt.content(
                new ContentChunk(
                        "forged", "forged", "forged", 0, "text/plain", "private-content-value"));
        var events = store.readAfter("content-run", -1);
        assertFalse(
                com.alibaba.fastjson2.JSON.toJSONString(events).contains("private-content-value"));
        assertTrue(
                events.stream()
                        .filter(e -> e.eventType().equals("CONTENT"))
                        .findFirst()
                        .orElseThrow()
                        .attributes()
                        .containsKey("contentPayloadRef"));
        assertDoesNotThrow(
                () ->
                        attempt.content(
                                new ContentChunk(
                                        "forged",
                                        "forged",
                                        "forged",
                                        1,
                                        "application/json",
                                        null)));
    }

    @Test
    void diagnostic_failure_warns_without_replacing_success_and_strict_audit_fails_closed()
            throws Exception {
        for (boolean strict : List.of(false, true)) {
            var store = new InMemoryObservationStore();
            ExecutionRecordStore faulting =
                    (ExecutionRecordStore)
                            java.lang.reflect.Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class<?>[] {ExecutionRecordStore.class},
                                    (proxy, method, args) -> {
                                        if (method.getName().equals("appendLog"))
                                            throw new IllegalStateException(
                                                    "private adapter error must not be exposed");
                                        try {
                                            return method.invoke(store, args);
                                        } catch (java.lang.reflect.InvocationTargetException e) {
                                            throw e.getCause();
                                        }
                                    });
            var recorder =
                    new ExecutionRecorder(
                            faulting,
                            store,
                            new BoundedExecutionEventBus(faulting, 8, 32),
                            Clock.systemUTC(),
                            strict);
            try (var engine =
                    new FlowEngine(
                            NodeExecutorRegistry.builtIns(),
                            ConcurrencyGovernor.defaults(),
                            Clock.systemUTC(),
                            recorder)) {
                var result =
                        engine.execute(
                                        new FlowCompiler()
                                                .compile(ExecutionRequestContractTest.bundle()),
                                        new ExecutionRequest(
                                                "test:flow",
                                                ExecutionMode.EPHEMERAL,
                                                ExecutionPurpose.TRIAL,
                                                Map.of(),
                                                Map.of(),
                                                null,
                                                "key",
                                                "tenant",
                                                Instant.now().plusSeconds(5),
                                                Map.of()))
                                .completion()
                                .toCompletableFuture()
                                .get(5, TimeUnit.SECONDS);
                assertTrue(recorder.warningCount() > 0);
                assertEquals(
                        strict ? ExecutionStatus.FAILED : ExecutionStatus.SUCCEEDED,
                        result.status());
                assertFalse(
                        com.alibaba
                                .fastjson2
                                .JSON
                                .toJSONString(result)
                                .contains("private adapter"));
            }
        }
    }

    @Test
    void each_real_http_retry_has_its_own_record_and_monotonic_logs() throws Exception {
        var clock = Clock.systemUTC();
        var store = new InMemoryObservationStore();
        var recorder =
                new ExecutionRecorder(
                        store, store, new BoundedExecutionEventBus(store, 8, 64), clock, false);
        AtomicInteger calls = new AtomicInteger();
        HttpNodeClient client =
                request ->
                        CompletableFuture.completedFuture(
                                new HttpNodeClient.HttpResponse(
                                        calls.incrementAndGet() < 3 ? 503 : 200,
                                        Map.of(),
                                        new byte[0]));
        var base = ExecutionRequestContractTest.bundle();
        var caps = new ComponentCapabilities(ComponentEffect.PURE, true, false, true, 4);
        var contracts = new HashMap<>(base.componentContracts());
        contracts.put(
                "http",
                new CompiledComponentContract(
                        "http", ComponentType.HTTP, null, Map.of(), Map.of(), Map.of(), caps));
        var nodes = new ArrayList<>(base.flowDefinition().nodes());
        nodes.add(
                new FlowNode(
                        "http",
                        "http",
                        "http",
                        "http",
                        Map.of(
                                "method",
                                "GET",
                                "url",
                                "https://example.invalid/path",
                                "requestParamsScript",
                                "return [:]",
                                "retryCount",
                                3,
                                "timeoutMillis",
                                1000)));
        var bundle =
                new ExecutableFlowBundle(
                                "1.0",
                                base.flowIdentity(),
                                new FlowDefinition(
                                        "2.0",
                                        nodes,
                                        List.of(
                                                new FlowEdge("a", "start", "http", "NEXT", "IN"),
                                                new FlowEdge("b", "http", "end", "NEXT", "IN"))),
                                contracts,
                                Map.of(),
                                Map.of(),
                                null,
                                null)
                        .withCalculatedHash();
        try (var scripts = new SandboxedGroovyRuntime();
                var engine =
                        new FlowEngine(
                                NodeExecutorRegistry.withBuiltIns(
                                        List.of(
                                                new HttpNodeExecutor(
                                                        scripts,
                                                        client,
                                                        new RetryPolicy(
                                                                3,
                                                                Duration.ofMillis(1),
                                                                Duration.ofMillis(2)),
                                                        new IdempotencyCoordinator(),
                                                        clock,
                                                        Duration.ofSeconds(1),
                                                        1024))),
                                ConcurrencyGovernor.defaults(),
                                clock,
                                recorder)) {
            var handle =
                    engine.execute(
                            new FlowCompiler().compile(bundle),
                            new ExecutionRequest(
                                    "test:flow",
                                    ExecutionMode.EPHEMERAL,
                                    ExecutionPurpose.TRIAL,
                                    Map.of(),
                                    Map.of(),
                                    null,
                                    "key",
                                    "tenant",
                                    Instant.now().plusSeconds(10),
                                    Map.of()));
            var result = handle.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(3, calls.get());
            var attempts =
                    store.nodes(handle.executionId()).stream()
                            .filter(n -> n.nodeCode().equals("http"))
                            .toList();
            assertEquals(
                    List.of(1, 2, 3), attempts.stream().map(NodeExecutionRecord::attempt).toList());
            assertEquals(
                    3,
                    attempts.stream().map(NodeExecutionRecord::nodeExecutionId).distinct().count());
            assertEquals(
                    List.of(
                            ExecutionStatus.RETRYING,
                            ExecutionStatus.RETRYING,
                            ExecutionStatus.SUCCEEDED),
                    attempts.stream().map(NodeExecutionRecord::status).toList());
            for (var node : store.nodes(handle.executionId())) {
                assertNotNull(node.finishedAt());
                var logs = store.logs(node.nodeExecutionId());
                for (int n = 0; n < logs.size(); n++) assertEquals(n, logs.get(n).sequence());
            }
            assertEquals(
                    1,
                    store.readAfter(handle.executionId(), -1).stream()
                            .filter(
                                    e ->
                                            e.eventType().equals("NODE_STARTED")
                                                    && e.nodeCode().equals("start"))
                            .count());
        }
    }

    @Test
    void capacity_and_retention_preserve_active_executions_and_reject_orphan_rows() {
        var clock = new MutableClock();
        var store = new InMemoryObservationStore(1, 16, Duration.ofSeconds(1), clock);
        var record =
                new ExecutionRecord(
                        "a",
                        "a",
                        null,
                        new FlowIdentity("tenant", "flow", "v1"),
                        "hash",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        clock.instant(),
                        null,
                        null,
                        null,
                        null,
                        0);
        store.saveExecution(record);
        clock.now = clock.now.plusSeconds(60);
        assertTrue(store.execution("a").isPresent());
        assertThrows(
                IllegalStateException.class,
                () ->
                        store.saveExecution(
                                new ExecutionRecord(
                                        "b",
                                        "b",
                                        null,
                                        record.flowIdentity(),
                                        "hash",
                                        record.mode(),
                                        record.purpose(),
                                        record.status(),
                                        record.startedAt(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        0)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        store.appendLog(
                                new ExecutionLogRecord(
                                        "l",
                                        "a",
                                        "missing",
                                        clock.instant(),
                                        ExecutionLogLevel.INFO,
                                        "engine",
                                        0,
                                        "safe",
                                        Map.of(),
                                        null)));
        store.saveExecution(
                new ExecutionRecord(
                        "a",
                        "a",
                        null,
                        record.flowIdentity(),
                        "hash",
                        record.mode(),
                        record.purpose(),
                        ExecutionStatus.SUCCEEDED,
                        record.startedAt(),
                        clock.instant(),
                        null,
                        null,
                        null,
                        0));
        clock.now = clock.now.plusSeconds(2);
        assertTrue(store.execution("a").isEmpty());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        store.appendContent(
                                "missing",
                                new ContentChunk("a", "node", "root", 0, "text/plain", "x")));
    }

    private static class MutableClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        public Instant instant() {
            return now;
        }

        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
