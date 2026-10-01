package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.adapter.memory.InMemoryObservationStore;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.core.compile.FlowCompiler;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.core.node.*;
import com.playrewards.flowengine.core.node.builtin.EndNodeExecutor;
import com.playrewards.flowengine.core.observation.ExecutionRecorder;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 失败可定位：执行结果与查询摘要不再统一为固定文案，而是包含失败节点、错误码和节点给出的安全消息；
 * 失败 END 配置的业务错误码与消息原样透传。
 */
class ExecutionErrorSummaryTest {

    @Test
    void failedEndPassesConfiguredBusinessCodeAndMessageThrough() throws Exception {
        var store = new InMemoryObservationStore();
        var result =
                run(
                        store,
                        pass(),
                        Map.of(
                                "resultStatus",
                                "FAILED",
                                "errorCode",
                                "STOCK_EMPTY",
                                "errorMessage",
                                "库存不足"));
        assertEquals("STOCK_EMPTY", result.errorCode());
        assertEquals("库存不足", result.errorMessage());
        var record = store.execution(result.executionId()).orElseThrow();
        assertEquals("库存不足", record.errorMessage());
        var end =
                store.nodes(result.executionId()).stream()
                        .filter(n -> n.nodeCode().equals("end"))
                        .findFirst()
                        .orElseThrow();
        assertEquals("STOCK_EMPTY", end.errorCode());
        assertEquals("库存不足", end.errorMessage());
        var summary = service(store).summary(record);
        @SuppressWarnings("unchecked")
        var error = (Map<String, Object>) summary.get("error");
        assertEquals("库存不足", error.get("message"));
        assertEquals("end", error.get("nodeCode"));
    }

    @Test
    void nodeFailureSummaryNamesNodeCodeAndMessage() throws Exception {
        var store = new InMemoryObservationStore();
        NodeExecutor failing =
                executor(
                        () -> {
                            throw new NodeExecutionException(
                                    "SCRIPT_RUNTIME_ERROR",
                                    "script runtime error at line 2: null value has no property"
                                            + " 'value'");
                        });
        var result = run(store, failing, Map.of("resultStatus", "SUCCESS"));
        assertEquals("SCRIPT_RUNTIME_ERROR", result.errorCode());
        assertTrue(result.errorMessage().contains("start"), result.errorMessage());
        assertTrue(result.errorMessage().contains("SCRIPT_RUNTIME_ERROR"), result.errorMessage());
        assertTrue(result.errorMessage().contains("line 2"), result.errorMessage());
        var node =
                store.nodes(result.executionId()).stream()
                        .filter(n -> n.status() == ExecutionStatus.FAILED)
                        .findFirst()
                        .orElseThrow();
        assertTrue(node.errorMessage().contains("line 2"));
        var record = store.execution(result.executionId()).orElseThrow();
        assertEquals(result.errorMessage(), record.errorMessage());
        assertTrue(
                store.readAfter(result.executionId(), -1).stream()
                        .filter(e -> e.eventType().equals("FAILED"))
                        .anyMatch(e -> result.errorMessage().equals(e.attributes().get("message"))));
    }

    @Test
    void unknownExceptionsKeepTheirTextOutOfPublicMessages() throws Exception {
        var store = new InMemoryObservationStore();
        NodeExecutor failing =
                executor(
                        () -> {
                            throw new IllegalStateException("/private/host/path secret");
                        });
        var result = run(store, failing, Map.of("resultStatus", "SUCCESS"));
        assertFalse(result.errorMessage().contains("/private"), result.errorMessage());
        var node =
                store.nodes(result.executionId()).stream()
                        .filter(n -> n.status() == ExecutionStatus.FAILED)
                        .findFirst()
                        .orElseThrow();
        assertNull(node.errorMessage());
    }

    private static ExecutionResult run(
            InMemoryObservationStore store, NodeExecutor start, Map<String, Object> endConfig)
            throws Exception {
        var recorder =
                new ExecutionRecorder(
                        store, store, new BoundedExecutionEventBus(store, 2, 64), Clock.systemUTC(), true);
        var registry = new NodeExecutorRegistry(List.of(start, new EndNodeExecutor()));
        try (var engine =
                new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC(), recorder)) {
            return engine.execute(
                            new FlowCompiler().compile(bundle(endConfig)),
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
        }
    }

    private static ExecutionService service(InMemoryObservationStore store) {
        return new ExecutionService(
                null, null, null, List.of(), null, null, store, store, Duration.ofMinutes(1));
    }

    private static NodeExecutor pass() {
        return executor(() -> NodeResult.continueWith(Map.of(), Set.of("NEXT")));
    }

    private static NodeExecutor executor(java.util.function.Supplier<NodeResult> body) {
        return new NodeExecutor() {
            public ComponentType supports() {
                return ComponentType.START;
            }

            public NodeInvocation execute(NodeExecutionContext context) {
                return NodeInvocation.completed(body.get());
            }
        };
    }

    private static ExecutableFlowBundle bundle(Map<String, Object> endConfig) {
        var base = ExecutionRequestContractTest.bundle();
        return new ExecutableFlowBundle(
                        base.contractVersion(),
                        base.flowIdentity(),
                        new FlowDefinition(
                                "2.0",
                                List.of(
                                        new FlowNode("start", "start", "start", "start", Map.of()),
                                        new FlowNode("end", "end", "end", "end", endConfig)),
                                List.of(new FlowEdge("a", "start", "end", "NEXT", "IN"))),
                        base.componentContracts(),
                        base.protocolSchemas(),
                        base.externalScopeSchemas(),
                        null,
                        null)
                .withCalculatedHash();
    }
}
