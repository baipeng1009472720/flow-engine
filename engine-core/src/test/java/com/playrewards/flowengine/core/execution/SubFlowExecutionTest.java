package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.ANY;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.await;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.binding;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.edge;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.node;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.output;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.plan;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.registry;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.request;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.requiredInput;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.scripts;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.ComponentInvocation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Verifies embedded child plans execute with independent ctx and stable child identity. */
class SubFlowExecutionTest {

    @Test
    void parent_cancellation_does_not_publish_before_a_non_future_child_source_is_clean() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        NodeExecutor child = cancellationAwareNonFutureChild(started, cancellationObserved, cleaned);
        try (FlowEngine engine = new FlowEngine(
                registry(child, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(parentPlan(childPlan()), request(Map.of("value", "x")));
            assertTrue(started.await(2, TimeUnit.SECONDS));

            assertTrue(handle.cancel());
            assertTrue(cancellationObserved.await(2, TimeUnit.SECONDS));
            assertTrue(!handle.completion().toCompletableFuture().isDone(),
                    "parent completion must remain pending during child source cleanup");
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.CANCELLED, result.status());
            assertEquals(0, cleaned.getCount());
        }
    }

    @Test
    void parent_deadline_does_not_publish_before_a_non_future_child_source_is_clean() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        NodeExecutor child = cancellationAwareNonFutureChild(started, cancellationObserved, cleaned);
        try (FlowEngine engine = new FlowEngine(
                registry(child, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(parentPlan(childPlan()),
                    request(Map.of("value", "x"), Map.of(), Instant.now().plusMillis(500)));
            assertTrue(started.await(2, TimeUnit.SECONDS));

            assertTrue(cancellationObserved.await(2, TimeUnit.SECONDS));
            assertTrue(!handle.completion().toCompletableFuture().isDone(),
                    "parent completion must remain pending during deadline cleanup");
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
            assertEquals(0, cleaned.getCount());
        }
    }

    @Test
    void nested_fail_fast_waits_for_a_non_future_source_to_observe_local_cancellation() throws Exception {
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch slowCleaned = new CountDownLatch(1);
        NodeExecutor branches = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                if (context.node().node().nodeCode().equals("childFail")) {
                    try {
                        assertTrue(slowStarted.await(2, TimeUnit.SECONDS));
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new java.util.concurrent.CancellationException("failure branch interrupted");
                    }
                    throw new com.playrewards.flowengine.core.node.NodeExecutionException(
                            "CHILD_BRANCH_FAILED", "child branch failed");
                }
                CompletableFuture<NodeResult> source = new CompletableFuture<>();
                slowStarted.countDown();
                Thread.ofVirtual().start(() -> {
                    try {
                        while (!context.cancellationToken().isCancellationRequested()) {
                            Thread.sleep(5);
                        }
                        Thread.sleep(100);
                        slowCleaned.countDown();
                        source.completeExceptionally(
                                new java.util.concurrent.CancellationException("slow branch cancelled"));
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        source.completeExceptionally(failure);
                    }
                });
                return new NodeInvocation(nonFutureStage(source), ComponentInvocation.emptyContent());
            }
        };
        try (FlowEngine engine = new FlowEngine(
                registry(branches, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(
                    parentPlan(parallelChildPlan()), request(Map.of("value", "x"))));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("CHILD_BRANCH_FAILED", result.errorCode());
            assertEquals(0, slowCleaned.getCount(),
                    "parent result must wait for the nested asynchronous source cleanup");
        }
    }

    @Test
    void parent_cancellation_reaches_child_work_and_waits_for_child_cleanup() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        NodeExecutor blocking = blockingChild(started, cleaned);
        try (FlowEngine engine = new FlowEngine(
                registry(blocking, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(parentPlan(childPlan()), request(Map.of("value", "x")));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertTrue(handle.cancel());
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.CANCELLED, result.status());
            assertTrue(cleaned.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void parent_deadline_reaches_child_work_and_returns_the_same_deadline_failure() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        NodeExecutor blocking = blockingChild(started, cleaned);
        try (FlowEngine engine = new FlowEngine(
                registry(blocking, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(parentPlan(childPlan()),
                    request(Map.of("value", "x"), Map.of(), java.time.Instant.now().plusMillis(100))));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
            assertTrue(cleaned.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void runtime_depth_limit_fails_closed_even_for_prebuilt_plans() throws Exception {
        ExecutionPlan nested = terminalChild();
        for (int depth = 0; depth < 33; depth++) {
            nested = wrappingPlan("depth-" + depth, nested);
        }
        try (FlowEngine engine = new FlowEngine(
                registry(null, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(nested, request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("SUB_FLOW_DEPTH_EXCEEDED", result.errorCode());
        }
    }

    @Test
    void maps_parent_inputs_to_child_start_and_child_result_to_parent_output() throws Exception {
        AtomicReference<ExecutionContext> childContext = new AtomicReference<>();
        AtomicReference<Map<String, Map<String, Object>>> externalScopes = new AtomicReference<>();
        NodeExecutor childWork = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                childContext.set(context.executionContext());
                externalScopes.set(context.externalScopes());
                return NodeInvocation.completed(NodeResult.continueWith(
                        Map.of("result", context.inputs().requiredJson("value")), Set.of("NEXT")));
            }
        };
        ExecutionPlan plan = parentPlan(childPlan());
        try (FlowEngine engine = new FlowEngine(
                registry(childWork, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(plan,
                    request(Map.of("value", "child-input"), Map.of("play", Map.of("id", "p-7")),
                            java.time.Instant.now().plusSeconds(5)));
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals("child-input", result.result());
            assertNotSame(handle.context(), childContext.get());
            assertEquals("p-7", externalScopes.get().get("play").get("id"));
            Map<?, ?> children = childExecutions(handle.context());
            assertEquals(1, children.size());
            Object child = children.values().iterator().next();
            Method scopePath = child.getClass().getMethod("scopePath");
            String scope = ((ScopePath) scopePath.invoke(child)).value();
            assertTrue(scope.startsWith("root/subflow:sub/execution:"));
        }
    }

    @Test
    void child_failed_end_stops_the_parent_without_starting_its_successor() throws Exception {
        ExecutionPlan child = childPlanWithFailure();
        ExecutionPlan parent = parentPlan(child);
        try (FlowEngine engine = new FlowEngine(
                registry(null, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(parent, request(Map.of("value", "x"))));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("CHILD_REJECTED", result.errorCode());
        }
    }

    private static ExecutionPlan childPlan() {
        CompiledNode start = node("childStart", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "value", "name", "value",
                        "protocolCode", ANY.code(), "required", true, "schema", ANY.schema()))),
                Map.of("value", output("value", ANY)));
        CompiledNode work = node("childWork", ComponentType.SCRIPT, Map.of(),
                Map.of("value", requiredInput("value", ANY)), Map.of("result", output("result", ANY)),
                Map.of("value", binding("value", "childStart", "value")));
        CompiledNode end = node("childEnd", ComponentType.END, Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", Map.of("sourceNodeId", "childWork", "sourceOutputKey", "result")), Map.of());
        return plan("child", List.of(start, work, end), List.of(
                edge("childStart", "childWork", "NEXT", "IN"),
                edge("childWork", "childEnd", "NEXT", "IN")), Map.of(), Map.of(), Map.of());
    }

    private static ExecutionPlan childPlanWithFailure() {
        CompiledNode start = node("childStart", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "value", "name", "value",
                        "protocolCode", ANY.code(), "required", true, "schema", ANY.schema()))),
                Map.of("value", output("value", ANY)));
        CompiledNode end = node("childEnd", ComponentType.END,
                Map.of("resultStatus", "FAILED", "errorCode", "CHILD_REJECTED"), Map.of());
        return plan("child-fail", List.of(start, end), List.of(
                edge("childStart", "childEnd", "NEXT", "IN")), Map.of(), Map.of(), Map.of());
    }

    private static ExecutionPlan parallelChildPlan() {
        CompiledNode start = node("childStart", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "value", "name", "value",
                        "protocolCode", ANY.code(), "required", true, "schema", ANY.schema()))),
                Map.of("value", output("value", ANY)));
        CompiledNode parallel = node("childParallel", ComponentType.PARALLEL, Map.of(), Map.of());
        CompiledNode failure = node("childFail", ComponentType.SCRIPT, Map.of(), Map.of());
        CompiledNode slow = node("childSlow", ComponentType.SCRIPT, Map.of(), Map.of());
        CompiledNode join = node("childJoin", ComponentType.IF, Map.of(
                "conditionMode", "DEFAULT",
                "conditionBinding", Map.of("sourceNodeId", "childStart", "sourceOutputKey", "value")),
                Map.of());
        CompiledNode end = node("childEnd", ComponentType.END,
                Map.of("resultStatus", "SUCCESS"), Map.of());
        return plan("child-parallel", List.of(start, parallel, failure, slow, join, end), List.of(
                edge("childStart", "childParallel", "NEXT", "IN"),
                edge("childParallel", "childFail", "BRANCH", "IN"),
                edge("childParallel", "childSlow", "BRANCH", "IN"),
                edge("childFail", "childJoin", "NEXT", "IN"),
                edge("childSlow", "childJoin", "NEXT", "IN"),
                edge("childJoin", "childEnd", "TRUE", "IN")), Map.of(), Map.of(), Map.of());
    }

    private static ExecutionPlan parentPlan(ExecutionPlan child) {
        CompiledNode start = node("parentStart", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "value", "name", "value",
                        "protocolCode", ANY.code(), "required", true, "schema", ANY.schema()))),
                Map.of("value", output("value", ANY)));
        CompiledNode sub = node("sub", ComponentType.SUB_FLOW, Map.of("targetFlowId", child.flowIdentity().flowId()),
                Map.of("value", requiredInput("value", ANY)), Map.of("result", output("result", ANY)),
                Map.of("value", binding("value", "parentStart", "value")));
        CompiledNode end = node("parentEnd", ComponentType.END, Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", Map.of("sourceNodeId", "sub", "sourceOutputKey", "result")), Map.of());
        return plan("parent", List.of(start, sub, end), List.of(
                edge("parentStart", "sub", "NEXT", "IN"), edge("sub", "parentEnd", "NEXT", "IN")),
                Map.of(), Map.of(), Map.of("sub", child));
    }

    private static NodeExecutor blockingChild(CountDownLatch started, CountDownLatch cleaned) {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                started.countDown();
                try {
                    while (!context.cancellationToken().isCancellationRequested()) {
                        Thread.sleep(5);
                    }
                    throw new java.util.concurrent.CancellationException("child cancelled");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new java.util.concurrent.CancellationException("child interrupted");
                } finally {
                    cleaned.countDown();
                }
            }
        };
    }

    private static NodeExecutor cancellationAwareNonFutureChild(CountDownLatch started,
            CountDownLatch cancellationObserved, CountDownLatch cleaned) {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                CompletableFuture<NodeResult> source = new CompletableFuture<>();
                started.countDown();
                Thread.ofVirtual().start(() -> {
                    try {
                        while (!context.cancellationToken().isCancellationRequested()) {
                            Thread.sleep(5);
                        }
                        cancellationObserved.countDown();
                        Thread.sleep(100);
                        cleaned.countDown();
                        source.completeExceptionally(
                                new java.util.concurrent.CancellationException("child source cancelled"));
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        source.completeExceptionally(failure);
                    }
                });
                return new NodeInvocation(nonFutureStage(source), ComponentInvocation.emptyContent());
            }
        };
    }

    private static ExecutionPlan terminalChild() {
        CompiledNode start = node("leafStart", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode end = node("leafEnd", ComponentType.END, Map.of("resultStatus", "SUCCESS"), Map.of());
        return plan("leaf", List.of(start, end), List.of(
                edge("leafStart", "leafEnd", "NEXT", "IN")), Map.of(), Map.of(), Map.of());
    }

    private static ExecutionPlan wrappingPlan(String flowId, ExecutionPlan child) {
        CompiledNode start = node(flowId + "Start", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode sub = node(flowId + "Sub", ComponentType.SUB_FLOW,
                Map.of("targetFlowId", child.flowIdentity().flowId()), Map.of());
        CompiledNode end = node(flowId + "End", ComponentType.END,
                Map.of("resultStatus", "SUCCESS"), Map.of());
        return plan(flowId, List.of(start, sub, end), List.of(
                edge(start.node().id(), sub.node().id(), "NEXT", "IN"),
                edge(sub.node().id(), end.node().id(), "NEXT", "IN")),
                Map.of(), Map.of(), Map.of(sub.node().id(), child));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> childExecutions(ExecutionContext context) throws Exception {
        return (Map<String, ?>) context.getClass().getMethod("childExecutions").invoke(context);
    }

    private static DistributedLockProvider noLocks() {
        return new DistributedLockProvider() {
            @Override
            public java.util.Optional<com.playrewards.flowengine.spi.LockLease> tryAcquire(
                    com.playrewards.flowengine.spi.LockRequest request) {
                return java.util.Optional.empty();
            }

            @Override
            public boolean release(com.playrewards.flowengine.spi.LockLease lease) {
                return false;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<T> nonFutureStage(CompletableFuture<T> delegate) {
        return (CompletionStage<T>) Proxy.newProxyInstance(
                SubFlowExecutionTest.class.getClassLoader(),
                new Class<?>[]{CompletionStage.class},
                (proxy, method, arguments) -> {
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }
}
