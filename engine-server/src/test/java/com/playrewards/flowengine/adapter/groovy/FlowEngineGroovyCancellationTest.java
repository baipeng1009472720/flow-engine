package com.playrewards.flowengine.adapter.groovy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.execution.ConcurrencyGovernor;
import com.playrewards.flowengine.core.execution.ExecutionHandle;
import com.playrewards.flowengine.core.execution.FlowEngine;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Verifies engine cancellation/deadline propagation against the real Groovy worker lifecycle. */
class FlowEngineGroovyCancellationTest {

    private static final ComponentCapabilities PURE =
            new ComponentCapabilities(ComponentEffect.PURE, true, false, true, 4);
    private static final ProtocolSchema INTEGER =
            new ProtocolSchema("integer", "1.0", Map.of("type", "integer"));

    @Test
    void flow_engine_cancel_stops_the_worker_before_runtime_reuse() throws Exception {
        try (SandboxedGroovyRuntime runtime = runtime();
                FlowEngine engine = engine(runtime)) {
            ExecutionHandle handle = engine.execute(plan("while (true) { }"), request(Duration.ofSeconds(5)));
            awaitActiveInvocations(runtime, 1);

            assertTrue(handle.cancel());
            ExecutionResult cancelled = handle.completion().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(ExecutionStatus.CANCELLED, cancelled.status());
            awaitActiveInvocations(runtime, 0);

            assertSuccessfulReuse(engine);
        }
    }

    @Test
    void flow_engine_deadline_stops_the_worker_before_runtime_reuse() throws Exception {
        try (SandboxedGroovyRuntime runtime = runtime();
                FlowEngine engine = engine(runtime)) {
            ExecutionResult expired = engine.execute(
                            plan("while (true) { }"), request(Duration.ofMillis(100)))
                    .completion().toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertEquals(ExecutionStatus.FAILED, expired.status());
            awaitActiveInvocations(runtime, 0);
            assertSuccessfulReuse(engine);
        }
    }

    private static SandboxedGroovyRuntime runtime() {
        return new SandboxedGroovyRuntime(Duration.ofSeconds(5), 10_000, 32_000,
                8, Duration.ofMinutes(1), Clock.systemUTC(), 1, 1_024, 8_192, 32);
    }

    private static FlowEngine engine(SandboxedGroovyRuntime runtime) {
        return new FlowEngine(NodeExecutorRegistry.runtimeBuiltIns(
                runtime, new FlowBusinessComponentRegistry(List.of())),
                ConcurrencyGovernor.defaults(), Clock.systemUTC());
    }

    private static void assertSuccessfulReuse(FlowEngine engine) throws Exception {
        ExecutionResult reused = engine.execute(plan("return 7"), request(Duration.ofSeconds(2)))
                .completion().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(ExecutionStatus.SUCCEEDED, reused.status());
        assertEquals(7, reused.result());
    }

    private static void awaitActiveInvocations(SandboxedGroovyRuntime runtime, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (runtime.activeInvocationCount() != expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, runtime.activeInvocationCount());
    }

    private static ExecutionRequest request(Duration deadline) {
        return new ExecutionRequest("inline:flow", ExecutionMode.EPHEMERAL, ExecutionPurpose.TRIAL,
                Map.of(), Map.of(), null, "key", "tenant", Instant.now().plus(deadline), Map.of());
    }

    private static ExecutionPlan plan(String scriptSource) {
        CompiledNode start = node("start", ComponentType.START,
                Map.of("inputProtocols", List.of()), Map.of());
        CompiledNode script = node("script", ComponentType.SCRIPT,
                Map.of("groovyScript", scriptSource),
                Map.of("result", new OutputSlotContract("result", INTEGER)));
        CompiledNode end = node("end", ComponentType.END,
                Map.of("resultStatus", "SUCCESS",
                        "returnBinding", Map.of("sourceNodeId", "script", "sourceOutputKey", "result")),
                Map.of());
        return new ExecutionPlan(new FlowIdentity("tenant", "flow", "v1"), "hash",
                Map.of("start", start, "script", script, "end", end),
                Map.of("start", List.of("script"), "script", List.of("end"), "end", List.of()),
                Map.of("start", List.of(), "script", List.of("start"), "end", List.of("script")),
                Map.of("start", Map.of("NEXT", List.of("script")),
                        "script", Map.of("NEXT", List.of("end")), "end", Map.of()),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static CompiledNode node(String id, ComponentType type,
            Map<String, Object> config, Map<String, OutputSlotContract> outputs) {
        FlowNode node = new FlowNode(id, "component." + id, id, id, config);
        return new CompiledNode(node, new CompiledComponentContract(
                node.componentCode(), type, null, Map.of(), Map.of(), outputs, PURE), Map.of());
    }
}
