package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.ANY;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.ARRAY;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.INTEGER;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.await;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.edge;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.node;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.output;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.plan;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.registry;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.request;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.scripts;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.playrewards.flowengine.spi.ScriptRuntime;
import com.playrewards.flowengine.spi.ScriptRuntime.ScriptExecutionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Verifies real LOOP frame scopes, iteration barriers, collection modes and ordered aggregation. */
class LoopExecutionTest {

    @Test
    void done_scope_never_exposes_empty_single_or_multi_iteration_frame_outputs() throws Exception {
        List<Map<String, Object>> doneViews = new java.util.concurrent.CopyOnWriteArrayList<>();
        NodeExecutor nodes = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                if (context.node().node().nodeCode().equals("after")) {
                    doneViews.add(context.ctxView());
                    return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                }
                Map<?, ?> frame = (Map<?, ?>) context.ctxView().get("loop");
                return NodeInvocation.completed(NodeResult.continueWith(
                        Map.of("value", frame.get("item")), Set.of("NEXT")));
            }
        };
        try (FlowEngine engine = new FlowEngine(
                registry(nodes, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            for (List<Integer> items : List.of(List.<Integer>of(), List.of(7), List.of(7, 8))) {
                assertEquals(ExecutionStatus.SUCCEEDED,
                        await(engine.execute(loopVisibilityPlan(), request(Map.of("items", items)))).status());
            }
        }

        assertEquals(3, doneViews.size());
        for (Map<String, Object> view : doneViews) {
            Map<?, ?> loop = (Map<?, ?>) view.get("loop");
            assertFalse(loop.containsKey("item"));
            assertFalse(loop.containsKey("index"));
            assertFalse(view.containsKey("body"));
        }
    }

    @Test
    void synchronous_collection_script_failure_keeps_safe_code_and_message() throws Exception {
        ScriptRuntime runtime = invocation -> {
            throw new ScriptExecutionException("SCRIPT_POLICY_DENIED", "safe loop policy failure");
        };
        try (FlowEngine engine = new FlowEngine(
                registry(bodyExecutor(context -> NodeResult.continueWith(Map.of(), Set.of("NEXT"))),
                        runtime, noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(groovyLoopPlan(), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("SCRIPT_POLICY_DENIED", result.errorCode());
            assertEquals("safe loop policy failure", result.errorMessage());
        }
    }

    @Test
    void asynchronous_collection_script_failure_keeps_safe_code_and_message() throws Exception {
        ScriptRuntime runtime = invocation -> CompletableFuture.failedFuture(
                new ScriptExecutionException("SCRIPT_RUNTIME_LIMIT", "safe loop runtime failure"));
        try (FlowEngine engine = new FlowEngine(
                registry(bodyExecutor(context -> NodeResult.continueWith(Map.of(), Set.of("NEXT"))),
                        runtime, noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(groovyLoopPlan(), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("SCRIPT_RUNTIME_LIMIT", result.errorCode());
            assertEquals("safe loop runtime failure", result.errorMessage());
        }
    }

    @Test
    void cancellation_waits_for_a_non_future_groovy_collection_source_to_clean_up() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        ScriptRuntime runtime = cancellationAwareCollection(started, cancellationObserved, cleaned);
        try (FlowEngine engine = new FlowEngine(
                registry(bodyExecutor(context -> NodeResult.continueWith(Map.of(), Set.of("NEXT"))),
                        runtime, noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(groovyLoopPlan(), request(Map.of()));
            assertTrue(started.await(2, TimeUnit.SECONDS));

            assertTrue(handle.cancel());
            assertTrue(cancellationObserved.await(2, TimeUnit.SECONDS));
            assertFalse(handle.completion().toCompletableFuture().isDone(),
                    "execution completion must remain pending while the collection source cleans up");
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.CANCELLED, result.status());
            assertEquals(0, cleaned.getCount());
        }
    }

    @Test
    void deadline_waits_for_a_non_future_groovy_collection_source_to_clean_up() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancellationObserved = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        ScriptRuntime runtime = cancellationAwareCollection(started, cancellationObserved, cleaned);
        try (FlowEngine engine = new FlowEngine(
                registry(bodyExecutor(context -> NodeResult.continueWith(Map.of(), Set.of("NEXT"))),
                        runtime, noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(groovyLoopPlan(),
                    request(Map.of(), Map.of(), Instant.now().plusMillis(500)));
            assertTrue(started.await(2, TimeUnit.SECONDS));

            assertTrue(cancellationObserved.await(2, TimeUnit.SECONDS));
            assertFalse(handle.completion().toCompletableFuture().isDone(),
                    "execution completion must remain pending while deadline cleanup is running");
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
            assertEquals(0, cleaned.getCount());
        }
    }

    @Test
    void waits_for_every_activated_body_path_before_completing_each_iteration() throws Exception {
        AtomicInteger pathRuns = new AtomicInteger();
        NodeExecutor body = bodyExecutor(context -> {
            pathRuns.incrementAndGet();
            return NodeResult.continueWith(Map.of("value", context.node().node().nodeCode()), Set.of("NEXT"));
        });
        try (FlowEngine engine = engine(body, invocation -> null)) {
            ExecutionResult result = await(engine.execute(multiPathLoopPlan(),
                    request(Map.of("items", List.of("x", "y")))));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(List.of("body", "body"), result.result());
            assertEquals(4, pathRuns.get());
        }
    }

    @Test
    void body_failure_stops_all_later_source_indexes() throws Exception {
        AtomicInteger bodyRuns = new AtomicInteger();
        NodeExecutor body = bodyExecutor(context -> {
            bodyRuns.incrementAndGet();
            throw new com.playrewards.flowengine.core.node.NodeExecutionException(
                    "LOOP_BODY_FAILED", "loop body failed");
        });
        try (FlowEngine engine = engine(body, invocation -> null)) {
            ExecutionResult result = await(engine.execute(upstreamLoopPlan(true, 10),
                    request(Map.of("items", List.of(1, 2, 3)))));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("LOOP_BODY_FAILED", result.errorCode());
            assertEquals(1, bodyRuns.get());
        }
    }

    @Test
    void nested_loop_frames_are_isolated_and_outer_results_follow_source_order() throws Exception {
        NodeExecutor body = bodyExecutor(context -> {
            Map<?, ?> inner = (Map<?, ?>) context.ctxView().get("inner");
            return NodeResult.continueWith(Map.of("value", inner.get("innerItem")), Set.of("NEXT"));
        });
        try (FlowEngine engine = engine(body, invocation -> {
            Map<?, ?> outer = (Map<?, ?>) invocation.ctx().get("outer");
            int item = ((Number) outer.get("outerItem")).intValue();
            return List.of(item * 10, item * 10 + 1);
        })) {
            ExecutionHandle handle = engine.execute(nestedLoopPlan(),
                    request(Map.of("items", List.of(2, 1))));
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(List.of(List.of(20, 21), List.of(10, 11)), result.result());
            assertEquals(11, handle.context().required("inner",
                    new ScopePath("root/loop:outer/index:1/loop:inner/index:1"), "innerItem"));
        }
    }

    @Test
    void nested_structure_does_not_deadlock_when_node_admission_is_one() throws Exception {
        NodeExecutor body = bodyExecutor(context -> {
            Map<?, ?> inner = (Map<?, ?>) context.ctxView().get("inner");
            return NodeResult.continueWith(Map.of("value", inner.get("innerItem")), Set.of("NEXT"));
        });
        var runtime = scripts(invocation -> {
            Map<?, ?> outer = (Map<?, ?>) invocation.ctx().get("outer");
            return List.of(((Number) outer.get("outerItem")).intValue());
        });
        try (DagScheduler scheduler = new DagScheduler(
                registry(body, runtime, noLocks()), ConcurrencyGovernor.defaults(), Clock.systemUTC(), 1, 2)) {
            ExecutionResult result = await(scheduler.schedule(
                    nestedLoopPlan(), request(Map.of("items", List.of(7)))));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(List.of(List.of(7)), result.result());
        }
    }

    @Test
    void rejects_invalid_item_and_aggregate_protocols() throws Exception {
        NodeExecutor body = bodyExecutor(context ->
                NodeResult.continueWith(Map.of("value", 1), Set.of("NEXT")));
        try (FlowEngine engine = engine(body, invocation -> null)) {
            ExecutionResult invalidItem = await(engine.execute(loopPlanWithItemProtocol(),
                    request(Map.of("items", List.of("not-integer")))));
            ExecutionResult invalidAggregate = await(engine.execute(loopPlanWithInvalidResultProtocol(),
                    request(Map.of("items", List.of(1)))));

            assertEquals("LOOP_ITEM_INVALID", invalidItem.errorCode());
            assertEquals("NODE_OUTPUT_INVALID", invalidAggregate.errorCode());
        }
    }

    @Test
    void executes_each_source_index_in_an_isolated_scope_and_preserves_result_order() throws Exception {
        AtomicInteger bodyRuns = new AtomicInteger();
        NodeExecutor body = bodyExecutor(context -> {
            bodyRuns.incrementAndGet();
            Map<?, ?> frame = (Map<?, ?>) context.ctxView().get("loop");
            return NodeResult.continueWith(Map.of("value", frame.get("item")), Set.of("NEXT"));
        });
        ExecutionPlan plan = upstreamLoopPlan(true, 10);
        try (FlowEngine engine = engine(body, invocation -> null)) {
            ExecutionHandle handle = engine.execute(plan, request(Map.of("items", List.of("c", "a", "b"))));
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(List.of("c", "a", "b"), result.result());
            assertEquals(3, bodyRuns.get());
            assertEquals("c", handle.context().required("loop",
                    new ScopePath("root/loop:loop/index:0"), "item"));
            assertEquals(2, handle.context().required("loop",
                    new ScopePath("root/loop:loop/index:2"), "index"));
            assertFalse(handle.context().lookup("loop",
                    new ScopePath("root/loop:loop/index:0"), "count").present());
        }
    }

    @Test
    void handles_empty_and_missing_result_binding_without_starting_extra_body_work() throws Exception {
        AtomicInteger bodyRuns = new AtomicInteger();
        NodeExecutor body = bodyExecutor(context -> {
            bodyRuns.incrementAndGet();
            return NodeResult.continueWith(Map.of("value", "ignored"), Set.of("NEXT"));
        });
        try (FlowEngine engine = engine(body, invocation -> null)) {
            ExecutionResult empty = await(engine.execute(
                    upstreamLoopPlan(false, 10), request(Map.of("items", List.of()))));
            ExecutionResult twoItems = await(engine.execute(
                    upstreamLoopPlan(false, 10), request(Map.of("items", List.of(1, 2)))));

            assertEquals(List.of(), empty.result());
            assertEquals(Arrays.asList(null, null), twoItems.result());
            assertEquals(2, bodyRuns.get());
        }
    }

    @Test
    void rejects_oversized_and_non_array_collections_before_running_the_body() throws Exception {
        AtomicInteger bodyRuns = new AtomicInteger();
        NodeExecutor body = bodyExecutor(context -> {
            bodyRuns.incrementAndGet();
            return NodeResult.continueWith(Map.of("value", 1), Set.of("NEXT"));
        });
        try (FlowEngine engine = engine(body, invocation -> "not-an-array")) {
            ExecutionResult oversized = await(engine.execute(
                    upstreamLoopPlan(true, 1), request(Map.of("items", List.of(1, 2)))));
            ExecutionResult invalidGroovy = await(engine.execute(
                    groovyLoopPlan(), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, oversized.status());
            assertEquals("LOOP_MAX_ITERATIONS_EXCEEDED", oversized.errorCode());
            assertEquals(ExecutionStatus.FAILED, invalidGroovy.status());
            assertEquals("LOOP_COLLECTION_INVALID", invalidGroovy.errorCode());
            assertEquals(0, bodyRuns.get());
        }
    }

    private static FlowEngine engine(NodeExecutor body,
            java.util.function.Function<com.playrewards.flowengine.spi.ScriptRuntime.ScriptInvocation, Object> eval) {
        var runtime = scripts(eval);
        return new FlowEngine(registry(body, runtime, noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC());
    }

    private static ExecutionPlan upstreamLoopPlan(boolean withResult, int maxIterations) {
        CompiledNode start = node("start", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "items", "name", "items",
                        "protocolCode", ARRAY.code(), "required", true, "schema", ARRAY.schema()))),
                Map.of("items", output("items", ARRAY)));
        Map<String, Object> loopConfig = new java.util.LinkedHashMap<>();
        loopConfig.put("role", "START");
        loopConfig.put("pairNodeId", "loopEnd");
        loopConfig.put("collectionMode", "UPSTREAM");
        loopConfig.put("collectionBinding", Map.of("sourceNodeId", "start", "sourceOutputKey", "items"));
        loopConfig.put("itemAlias", "item");
        loopConfig.put("indexAlias", "index");
        loopConfig.put("maxIterations", maxIterations);
        if (withResult) {
            loopConfig.put("resultBinding", Map.of("sourceNodeId", "body", "sourceOutputKey", "value"));
        }
        CompiledNode loop = node("loop", ComponentType.LOOP, loopConfig, Map.of(
                "count", output("count", INTEGER), "results", output("results", ARRAY)));
        CompiledNode body = node("body", ComponentType.SCRIPT, Map.of(), Map.of("value", output("value", ANY)));
        CompiledNode loopEnd = node("loopEnd", ComponentType.LOOP,
                Map.of("role", "END", "pairNodeId", "loop"), Map.of());
        CompiledNode end = node("end", ComponentType.END, Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", Map.of("sourceNodeId", "loop", "sourceOutputKey", "results")), Map.of());
        return plan("loop-flow", List.of(start, loop, body, loopEnd, end), List.of(
                edge("start", "loop", "NEXT", "ENTRY"),
                edge("loop", "body", "BODY", "IN"),
                edge("body", "loopEnd", "NEXT", "IN"),
                edge("loopEnd", "loop", "NEXT", "NEXT"),
                edge("loop", "end", "DONE", "IN")),
                Map.of("loop", "loopEnd", "loopEnd", "loop"), Map.of(), Map.of());
    }

    private static ExecutionPlan groovyLoopPlan() {
        ExecutionPlan base = upstreamLoopPlan(true, 10);
        CompiledNode old = base.nodesById().get("loop");
        Map<String, Object> config = new java.util.LinkedHashMap<>(old.node().config());
        config.remove("collectionBinding");
        config.put("collectionMode", "GROOVY");
        config.put("collectionExpression", "items");
        CompiledNode replacement = node("loop", ComponentType.LOOP, config,
                old.componentContract().outputSlots());
        List<CompiledNode> nodes = new java.util.ArrayList<>(base.nodesById().values());
        nodes.removeIf(node -> node.node().id().equals("loop"));
        nodes.removeIf(node -> node.node().id().equals("start"));
        nodes.add(node("start", ComponentType.START, Map.of("inputProtocols", List.of()), Map.of()));
        nodes.add(replacement);
        return plan("groovy-loop", nodes, List.of(
                edge("start", "loop", "NEXT", "ENTRY"), edge("loop", "body", "BODY", "IN"),
                edge("body", "loopEnd", "NEXT", "IN"), edge("loopEnd", "loop", "NEXT", "NEXT"),
                edge("loop", "end", "DONE", "IN")),
                Map.of("loop", "loopEnd", "loopEnd", "loop"), Map.of(), Map.of());
    }

    private static ExecutionPlan loopVisibilityPlan() {
        ExecutionPlan base = upstreamLoopPlan(true, 10);
        CompiledNode after = node("after", ComponentType.SCRIPT, Map.of(), Map.of());
        List<CompiledNode> nodes = new java.util.ArrayList<>(base.nodesById().values());
        nodes.add(after);
        return plan("loop-visibility", nodes, List.of(
                edge("start", "loop", "NEXT", "ENTRY"),
                edge("loop", "body", "BODY", "IN"),
                edge("body", "loopEnd", "NEXT", "IN"),
                edge("loopEnd", "loop", "NEXT", "NEXT"),
                edge("loop", "after", "DONE", "IN"),
                edge("after", "end", "NEXT", "IN")),
                Map.of("loop", "loopEnd", "loopEnd", "loop"), Map.of(), Map.of());
    }

    private static ExecutionPlan multiPathLoopPlan() {
        ExecutionPlan base = upstreamLoopPlan(true, 10);
        CompiledNode right = node("right", ComponentType.SCRIPT, Map.of(),
                Map.of("value", output("value", ANY)));
        List<CompiledNode> nodes = new java.util.ArrayList<>(base.nodesById().values());
        nodes.add(right);
        return plan("multi-loop", nodes, List.of(
                edge("start", "loop", "NEXT", "ENTRY"),
                edge("loop", "body", "BODY", "IN"), edge("loop", "right", "BODY", "IN"),
                edge("body", "loopEnd", "NEXT", "IN"), edge("right", "loopEnd", "NEXT", "IN"),
                edge("loopEnd", "loop", "NEXT", "NEXT"), edge("loop", "end", "DONE", "IN")),
                Map.of("loop", "loopEnd", "loopEnd", "loop"), Map.of(), Map.of());
    }

    private static ExecutionPlan nestedLoopPlan() {
        CompiledNode start = node("start", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "items", "name", "items",
                        "protocolCode", ARRAY.code(), "required", true, "schema", ARRAY.schema()))),
                Map.of("items", output("items", ARRAY)));
        CompiledNode outer = node("outer", ComponentType.LOOP, Map.of(
                "role", "START", "pairNodeId", "outerEnd", "collectionMode", "UPSTREAM",
                "collectionBinding", Map.of("sourceNodeId", "start", "sourceOutputKey", "items"),
                "itemAlias", "outerItem", "indexAlias", "outerIndex", "maxIterations", 10,
                "resultBinding", Map.of("sourceNodeId", "inner", "sourceOutputKey", "results")), Map.of(
                "count", output("count", INTEGER), "results", output("results", ARRAY)));
        CompiledNode inner = node("inner", ComponentType.LOOP, Map.of(
                "role", "START", "pairNodeId", "innerEnd", "collectionMode", "GROOVY",
                "collectionExpression", "inner-items", "itemAlias", "innerItem",
                "indexAlias", "innerIndex", "maxIterations", 10,
                "resultBinding", Map.of("sourceNodeId", "innerBody", "sourceOutputKey", "value")), Map.of(
                "count", output("count", INTEGER), "results", output("results", ARRAY)));
        CompiledNode innerBody = node("innerBody", ComponentType.SCRIPT, Map.of(),
                Map.of("value", output("value", ANY)));
        CompiledNode innerEnd = node("innerEnd", ComponentType.LOOP,
                Map.of("role", "END", "pairNodeId", "inner"), Map.of());
        CompiledNode outerEnd = node("outerEnd", ComponentType.LOOP,
                Map.of("role", "END", "pairNodeId", "outer"), Map.of());
        CompiledNode end = node("end", ComponentType.END, Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", Map.of("sourceNodeId", "outer", "sourceOutputKey", "results")), Map.of());
        return plan("nested-loop", List.of(start, outer, inner, innerBody, innerEnd, outerEnd, end), List.of(
                edge("start", "outer", "NEXT", "ENTRY"), edge("outer", "inner", "BODY", "ENTRY"),
                edge("inner", "innerBody", "BODY", "IN"), edge("innerBody", "innerEnd", "NEXT", "IN"),
                edge("innerEnd", "inner", "NEXT", "NEXT"), edge("inner", "outerEnd", "DONE", "IN"),
                edge("outerEnd", "outer", "NEXT", "NEXT"), edge("outer", "end", "DONE", "IN")),
                Map.of("outer", "outerEnd", "outerEnd", "outer",
                        "inner", "innerEnd", "innerEnd", "inner"), Map.of(), Map.of());
    }

    private static ExecutionPlan loopPlanWithItemProtocol() {
        ExecutionPlan base = upstreamLoopPlan(true, 10);
        CompiledNode old = base.nodesById().get("loop");
        Map<String, Object> config = new java.util.LinkedHashMap<>(old.node().config());
        config.put("itemProtocol", Map.of("code", INTEGER.code(), "version", INTEGER.version(),
                "schema", INTEGER.schema()));
        return replaceLoop(base, node("loop", ComponentType.LOOP, config,
                old.componentContract().outputSlots()), "invalid-item");
    }

    private static ExecutionPlan loopPlanWithInvalidResultProtocol() {
        ExecutionPlan base = upstreamLoopPlan(true, 10);
        CompiledNode old = base.nodesById().get("loop");
        return replaceLoop(base, node("loop", ComponentType.LOOP, old.node().config(), Map.of(
                "count", output("count", INTEGER), "results", output("results", INTEGER))),
                "invalid-result");
    }

    private static ExecutionPlan replaceLoop(ExecutionPlan base, CompiledNode replacement, String flowId) {
        List<CompiledNode> nodes = new java.util.ArrayList<>(base.nodesById().values());
        nodes.removeIf(node -> node.node().id().equals("loop"));
        nodes.add(replacement);
        return plan(flowId, nodes, List.of(
                edge("start", "loop", "NEXT", "ENTRY"), edge("loop", "body", "BODY", "IN"),
                edge("body", "loopEnd", "NEXT", "IN"), edge("loopEnd", "loop", "NEXT", "NEXT"),
                edge("loop", "end", "DONE", "IN")),
                Map.of("loop", "loopEnd", "loopEnd", "loop"), Map.of(), Map.of());
    }

    private static NodeExecutor bodyExecutor(Body behavior) {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                return NodeInvocation.completed(behavior.execute(context));
            }
        };
    }

    private static ScriptRuntime cancellationAwareCollection(CountDownLatch started,
            CountDownLatch cancellationObserved, CountDownLatch cleaned) {
        return invocation -> {
            CompletableFuture<Object> source = new CompletableFuture<>();
            started.countDown();
            Thread.ofVirtual().start(() -> {
                try {
                    while (!invocation.cancellationToken().isCancellationRequested()) {
                        Thread.sleep(5);
                    }
                    cancellationObserved.countDown();
                    Thread.sleep(100);
                    cleaned.countDown();
                    source.completeExceptionally(
                            new java.util.concurrent.CancellationException("collection cancelled"));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    source.completeExceptionally(failure);
                }
            });
            return nonFutureStage(source);
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletionStage<T> nonFutureStage(CompletableFuture<T> delegate) {
        return (CompletionStage<T>) Proxy.newProxyInstance(
                LoopExecutionTest.class.getClassLoader(),
                new Class<?>[]{CompletionStage.class},
                (proxy, method, arguments) -> {
                    try {
                        return method.invoke(delegate, arguments);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }

    private static DistributedLockProvider noLocks() {
        return new ParallelExecutionTestNoLocks();
    }

    @FunctionalInterface
    private interface Body {
        NodeResult execute(NodeExecutionContext context);
    }

    private static final class ParallelExecutionTestNoLocks implements DistributedLockProvider {
        @Override
        public java.util.Optional<com.playrewards.flowengine.spi.LockLease> tryAcquire(
                com.playrewards.flowengine.spi.LockRequest request) {
            return java.util.Optional.empty();
        }

        @Override
        public boolean release(com.playrewards.flowengine.spi.LockLease lease) {
            return false;
        }
    }
}
