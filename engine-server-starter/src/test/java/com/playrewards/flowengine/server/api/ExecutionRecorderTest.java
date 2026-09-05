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
