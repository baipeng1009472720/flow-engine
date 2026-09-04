package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.ANY;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.await;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.edge;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.node;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.output;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.plan;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.registry;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.request;
import static com.playrewards.flowengine.core.execution.StructuredExecutionTestSupport.scripts;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Verifies real scheduler fan-out, stable branch scopes, natural join and fail-fast cleanup. */
class ParallelExecutionTest {

    @Test
    void inactive_if_path_inside_a_branch_does_not_block_the_parallel_join() throws Exception {
        AtomicInteger selectedRuns = new AtomicInteger();
        AtomicInteger skippedRuns = new AtomicInteger();
        AtomicInteger joinRuns = new AtomicInteger();
        NodeExecutor scripts = executor(context -> {
            switch (context.node().node().nodeCode()) {
                case "selected" -> selectedRuns.incrementAndGet();
                case "skipped" -> skippedRuns.incrementAndGet();
                case "join" -> joinRuns.incrementAndGet();
                default -> { }
            }
            return NodeResult.continueWith(Map.of("value", context.node().node().nodeCode()), Set.of("NEXT"));
        });
        CompiledNode start = node("start", ComponentType.START, Map.of(
                "inputProtocols", List.of(Map.of("key", "flag", "name", "flag",
                        "protocolCode", "boolean", "required", true,
                        "schema", Map.of("type", "boolean")))),
                Map.of("flag", output("flag", new com.playrewards.flowengine.contract.protocol.ProtocolSchema(
                        "boolean", "1.0", Map.of("type", "boolean")))));
        CompiledNode parallel = node("parallel", ComponentType.PARALLEL, Map.of(), Map.of());
        CompiledNode decision = node("decision", ComponentType.IF, Map.of(
                "conditionMode", "DEFAULT",
                "conditionBinding", Map.of("sourceNodeId", "start", "sourceOutputKey", "flag")), Map.of());
        CompiledNode selected = node("selected", ComponentType.SCRIPT, Map.of(),
                Map.of("value", output("value", ANY)));
        CompiledNode skipped = node("skipped", ComponentType.SCRIPT, Map.of(),
                Map.of("value", output("value", ANY)));
        CompiledNode other = node("other", ComponentType.SCRIPT, Map.of(),
                Map.of("value", output("value", ANY)));
        CompiledNode join = node("join", ComponentType.SCRIPT, Map.of(),
                Map.of("value", output("value", ANY)));
        CompiledNode end = node("end", ComponentType.END, Map.of("resultStatus", "SUCCESS"), Map.of());
        ExecutionPlan plan = plan("parallel-if", List.of(start, parallel, decision, selected, skipped, other,
                join, end), List.of(
                edge("start", "parallel", "NEXT", "IN"),
                edge("parallel", "decision", "BRANCH", "IN"),
                edge("parallel", "other", "BRANCH", "IN"),
                edge("decision", "selected", "TRUE", "IN"),
                edge("decision", "skipped", "FALSE", "IN"),
                edge("selected", "join", "NEXT", "IN"),
                edge("skipped", "join", "NEXT", "IN"),
                edge("other", "join", "NEXT", "IN"),
                edge("join", "end", "NEXT", "IN")), Map.of(), Map.of(), Map.of());
        try (FlowEngine engine = new FlowEngine(registry(scripts, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of("flag", true))));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(1, selectedRuns.get());
            assertEquals(0, skippedRuns.get());
            assertEquals(1, joinRuns.get());
        }
    }

    @Test
    void fans_out_concurrently_and_runs_the_natural_join_once_in_parent_scope() throws Exception {
        CountDownLatch branchesStarted = new CountDownLatch(2);
        CountDownLatch releaseBranches = new CountDownLatch(1);
        AtomicInteger joinRuns = new AtomicInteger();
        Map<String, String> scopes = new java.util.concurrent.ConcurrentHashMap<>();
        NodeExecutor scripts = executor(context -> {
            String code = context.node().node().nodeCode();
            scopes.put(code, context.scopePath().value());
            if (code.equals("left") || code.equals("right")) {
                branchesStarted.countDown();
                assertTrue(branchesStarted.await(2, TimeUnit.SECONDS));
                releaseBranches.await(2, TimeUnit.SECONDS);
                return NodeResult.continueWith(Map.of("value", code), Set.of("NEXT"));
            }
            joinRuns.incrementAndGet();
            return NodeResult.continueWith(Map.of("value", "joined"), Set.of("NEXT"));
        });
        ExecutionPlan plan = parallelPlan();
        try (FlowEngine engine = new FlowEngine(registry(scripts, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionHandle handle = engine.execute(plan, request(Map.of()));
            assertTrue(branchesStarted.await(2, TimeUnit.SECONDS));
            releaseBranches.countDown();
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals("joined", result.result());
            assertEquals(1, joinRuns.get());
            assertEquals("root/parallel:parallel/branch:0", scopes.get("left"));
            assertEquals("root/parallel:parallel/branch:1", scopes.get("right"));
            assertEquals("root", scopes.get("join"));
        } finally {
            releaseBranches.countDown();
        }
    }

    @Test
    void branch_failure_cancels_a_running_sibling_and_waits_for_its_cleanup() throws Exception {
        CountDownLatch siblingStarted = new CountDownLatch(1);
        CountDownLatch siblingCleaned = new CountDownLatch(1);
        NodeExecutor scripts = executor(context -> {
            if (context.node().node().nodeCode().equals("left")) {
                assertTrue(siblingStarted.await(2, TimeUnit.SECONDS));
                throw new NodeExecutionException("BRANCH_FAILED", "branch failed");
            }
            if (context.node().node().nodeCode().equals("right")) {
                siblingStarted.countDown();
                try {
                    while (!context.cancellationToken().isCancellationRequested()) {
                        Thread.sleep(5);
                    }
                    throw new java.util.concurrent.CancellationException("cancelled sibling");
                } finally {
                    siblingCleaned.countDown();
                }
            }
            return NodeResult.continueWith(Map.of("value", "unexpected"), Set.of("NEXT"));
        });
        try (FlowEngine engine = new FlowEngine(registry(scripts, scripts(invocation -> null), noLocks()),
                ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(parallelPlan(), request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("BRANCH_FAILED", result.errorCode());
            assertEquals(0, siblingCleaned.getCount(), "parent completion must wait for sibling cleanup");
        }
    }

    private static ExecutionPlan parallelPlan() {
        CompiledNode start = node("start", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode parallel = node("parallel", ComponentType.PARALLEL, Map.of(), Map.of());
        CompiledNode left = node("left", ComponentType.SCRIPT, Map.of(), Map.of("value", output("value", ANY)));
        CompiledNode right = node("right", ComponentType.SCRIPT, Map.of(), Map.of("value", output("value", ANY)));
        CompiledNode join = node("join", ComponentType.SCRIPT, Map.of(), Map.of("value", output("value", ANY)));
        CompiledNode end = node("end", ComponentType.END, Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", Map.of("sourceNodeId", "join", "sourceOutputKey", "value")), Map.of());
        return plan("parallel", List.of(start, parallel, left, right, join, end), List.of(
                edge("start", "parallel", "NEXT", "IN"),
                edge("parallel", "left", "BRANCH", "IN"),
                edge("parallel", "right", "BRANCH", "IN"),
                edge("left", "join", "NEXT", "IN"),
                edge("right", "join", "NEXT", "IN"),
                edge("join", "end", "NEXT", "IN")), Map.of(), Map.of(), Map.of());
    }

    private static NodeExecutor executor(ThrowingFunction behavior) {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                try {
                    return NodeInvocation.completed(behavior.apply(context));
                } catch (RuntimeException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            }
        };
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

    @FunctionalInterface
    private interface ThrowingFunction {
        NodeResult apply(NodeExecutionContext context) throws Exception;
    }
}
