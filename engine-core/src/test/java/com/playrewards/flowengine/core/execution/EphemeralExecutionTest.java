package com.playrewards.flowengine.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.compile.FlowCompiler;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Verifies the Task 5 built-in node semantics and per-run context isolation. */
class EphemeralExecutionTest {

    private static final ProtocolSchema ANY = protocol("any", Map.of());
    private static final ProtocolSchema BOOLEAN = protocol("boolean", Map.of("type", "boolean"));

    @Test
    void writes_validated_start_inputs_and_returns_success_end_binding() throws Exception {
        FlowNode start = node("start-id", "builtin.start", "entry", Map.of(
                "inputProtocols", List.of(inputProtocol("requestId", true, ANY))));
        FlowNode variable = node("variable-id", "builtin.variable", "selected", Map.of(
                "valueMode", "FIXED",
                "valueSources", List.of(source("start-id", "requestId"))));
        FlowNode end = node("end-id", "builtin.end", "finish", Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", source("variable-id", "value")));
        ExecutionPlan plan = compile(List.of(start, variable, end), List.of(
                edge("e1", "start-id", "variable-id", "NEXT", "IN"),
                edge("e2", "variable-id", "end-id", "NEXT", "IN")), Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                        Map.of("requestId", output(ANY))),
                "builtin.variable", contract("builtin.variable", ComponentType.VARIABLE, Map.of(),
                        Map.of("value", output(ANY))),
                "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionHandle handle = engine.execute(plan, request(Map.of("requestId", "req-7")));
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals("req-7", result.result());
            assertNull(result.errorCode());
            assertNull(result.errorMessage());
            assertNull(result.errorId());
            assertEquals("req-7", handle.context().required("entry", ScopePath.root(), "requestId"));
            assertEquals("req-7", handle.context().requiredPath("ctx.selected", ScopePath.root()));
        }
    }

    @Test
    void rejects_missing_required_start_input_and_invalid_end_field_combinations() throws Exception {
        FlowNode start = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", List.of(inputProtocol("required", true, ANY))));
        FlowNode invalidSuccessEnd = node("end", "builtin.end", "end", Map.of(
                "resultStatus", "SUCCESS", "errorCode", "MUST_NOT_BE_PRESENT"));
        ExecutionPlan successPlan = compile(List.of(start, invalidSuccessEnd),
                List.of(edge("e1", "start", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                                Map.of("required", output(ANY))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult missing = await(engine.execute(successPlan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, missing.status());
            assertEquals("START_INPUT_REQUIRED", missing.errorCode());

            ExecutionResult invalidEnd = await(engine.execute(successPlan, request(Map.of("required", "ok"))));
            assertEquals(ExecutionStatus.FAILED, invalidEnd.status());
            assertEquals("END_CONFIG_INVALID", invalidEnd.errorCode());
            assertNull(invalidEnd.result());
        }
    }

    @Test
    void validates_start_values_against_the_declared_protocol() throws Exception {
        FlowNode start = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", List.of(inputProtocol("flag", false, BOOLEAN))));
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, end),
                List.of(edge("e1", "start", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                                Map.of("flag", output(BOOLEAN))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of("flag", "not-a-boolean"))));
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("START_INPUT_INVALID", result.errorCode());
        }
    }

    @Test
    void rejects_start_instance_contract_drift_and_treats_explicit_empty_as_no_inputs() throws Exception {
        FlowNode driftedStart = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", List.of(inputProtocol("flag", true, BOOLEAN))));
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan driftedPlan = compile(List.of(driftedStart, end),
                List.of(edge("e1", "start", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                                Map.of("flag", output(ANY))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        FlowNode emptyStart = node("empty-start", "builtin.empty-start", "empty", Map.of(
                "inputProtocols", List.of()));
        FlowNode emptyEnd = node("empty-end", "builtin.empty-end", "emptyEnd", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan emptyPlan = compile(List.of(emptyStart, emptyEnd),
                List.of(edge("e2", "empty-start", "empty-end", "NEXT", "IN")), Map.of(
                        "builtin.empty-start", contract("builtin.empty-start", ComponentType.START,
                                Map.of(), Map.of()),
                        "builtin.empty-end", contract("builtin.empty-end", ComponentType.END,
                                Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult drifted = await(engine.execute(driftedPlan, request(Map.of("flag", true))));
            assertEquals(ExecutionStatus.FAILED, drifted.status());
            assertEquals("START_CONFIG_INVALID", drifted.errorCode());

            ExecutionResult unexpected = await(engine.execute(emptyPlan, request(Map.of("extra", "value"))));
            assertEquals(ExecutionStatus.FAILED, unexpected.status());
            assertEquals("START_INPUT_UNKNOWN", unexpected.errorCode());
        }
    }

    @Test
    void missing_start_instance_contract_preserves_required_flags_from_the_legacy_snapshot() throws Exception {
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        CompiledComponentContract legacyStart = new CompiledComponentContract(
                "builtin.start", ComponentType.START, null,
                Map.of("type", "object", "properties", Map.of("inputBindings", Map.of(
                        "type", "array", "items", Map.of("type", "object"),
                        "x-playrewards-protocols", List.of(inputProtocol("required", true, ANY))))),
                Map.of(), Map.of("required", output(ANY)),
                new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 1024));
        ExecutionPlan plan = compile(List.of(start, end),
                List.of(edge("e1", "start", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", legacyStart,
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("START_INPUT_REQUIRED", result.errorCode());
        }
    }

    @Test
    void failed_end_has_an_error_code_and_never_returns_business_data() throws Exception {
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of(
                "resultStatus", "FAILED",
                "errorCode", "DECLINED",
                "errorMessage", "the operation was declined"));
        ExecutionPlan plan = compile(List.of(start, end),
                List.of(edge("e1", "start", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of())));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("DECLINED", result.errorCode());
            assertEquals("the operation was declined", result.errorMessage());
            assertNull(result.result());
        }
    }

    @Test
    void if_activates_only_one_branch_and_inactive_branch_does_not_block_natural_join() throws Exception {
        AtomicInteger trueRuns = new AtomicInteger();
        AtomicInteger falseRuns = new AtomicInteger();
        NodeExecutor scripts = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                AtomicInteger counter = context.node().node().nodeCode().equals("trueBranch")
                        ? trueRuns : falseRuns;
                counter.incrementAndGet();
                return NodeInvocation.completed(NodeResult.continueWith(
                        Map.of("value", context.node().node().nodeCode()), Set.of("NEXT")));
            }
        };
        FlowNode start = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", List.of(inputProtocol("flag", false, BOOLEAN))));
        FlowNode decision = node("if", "builtin.if", "decision", Map.of(
                "conditionMode", "DEFAULT", "conditionBinding", source("start", "flag")));
        FlowNode whenTrue = node("true", "test.script", "trueBranch", Map.of());
        FlowNode whenFalse = node("false", "test.script", "falseBranch", Map.of());
        FlowNode join = node("join", "builtin.variable", "joined", Map.of(
                "valueMode", "FIXED",
                "valueSources", List.of(source("true", "value"), source("false", "value"))));
        FlowNode end = node("end", "builtin.end", "end", Map.of(
                "resultStatus", "SUCCESS", "returnBinding", source("join", "value")));
        ExecutionPlan plan = compile(List.of(start, decision, whenTrue, whenFalse, join, end), List.of(
                edge("e1", "start", "if", "NEXT", "IN"),
                edge("e2", "if", "true", "TRUE", "IN"),
                edge("e3", "if", "false", "FALSE", "IN"),
                edge("e4", "true", "join", "NEXT", "IN"),
                edge("e5", "false", "join", "NEXT", "IN"),
                edge("e6", "join", "end", "NEXT", "IN")), Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                        Map.of("flag", output(BOOLEAN))),
                "builtin.if", contract("builtin.if", ComponentType.IF, Map.of(), Map.of()),
                "test.script", contract("test.script", ComponentType.SCRIPT, Map.of(),
                        Map.of("value", output(ANY))),
                "builtin.variable", contract("builtin.variable", ComponentType.VARIABLE, Map.of(),
                        Map.of("value", output(ANY))),
                "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(scripts));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of("flag", true))));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals("trueBranch", result.result());
            assertEquals(1, trueRuns.get());
            assertEquals(0, falseRuns.get());

            ExecutionResult falseResult = await(engine.execute(plan, request(Map.of("flag", false))));
            assertEquals(ExecutionStatus.SUCCEEDED, falseResult.status());
            assertEquals("falseBranch", falseResult.result());
            assertEquals(1, trueRuns.get());
            assertEquals(1, falseRuns.get());
        }
    }

    @Test
    void execution_context_addresses_are_scope_aware_append_only_and_preserve_json_null() {
        ExecutionContext context = new ExecutionContext(Map.of());
        ScopePath root = ScopePath.root();
        ScopePath branch = root.child("branch:true");

        context.writeOnce("node", root, "value", null);
        context.writeOnce("node", branch, "value", "branch-value");

        assertTrue(context.lookup("node", root, "value").present());
        assertNull(context.required("node", root, "value"));
        assertEquals("branch-value", context.required("node", branch, "value"));
        assertThrows(IllegalStateException.class, () -> context.writeOnce("node", root, "value", "overwrite"));
    }

    @Test
    void variable_uses_first_non_empty_source_but_preserves_false_and_zero() throws Exception {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("nullValue", null);
        values.put("emptyString", "");
        values.put("emptyList", List.of());
        values.put("emptyMap", Map.of());
        values.put("falseValue", false);
        values.put("zeroValue", 0);
        values.put("fallback", "later");

        assertEquals(false, executeVariable(values,
                List.of("nullValue", "emptyString", "emptyList", "emptyMap", "falseValue", "fallback")));
        assertEquals(0, executeVariable(values,
                List.of("nullValue", "emptyString", "emptyList", "emptyMap", "zeroValue", "fallback")));
        assertNull(executeVariable(values, List.of("nullValue", "emptyString", "emptyList", "emptyMap")));
    }

    @Test
    void variable_rejects_a_source_schema_that_is_not_assignable_to_its_output() throws Exception {
        ProtocolSchema booleanAlias = protocol("boolean-alias", Map.of("type", "boolean"));
        ProtocolSchema stringValue = protocol("text", Map.of("type", "string"));
        FlowNode start = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", List.of(
                        inputProtocol("flag", false, booleanAlias),
                        inputProtocol("text", false, stringValue))));
        FlowNode variable = node("variable", "builtin.variable", "variable", Map.of(
                "valueMode", "FIXED", "valueSources", List.of(
                        source("start", "flag"), source("start", "text"))));
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, variable, end), List.of(
                edge("e1", "start", "variable", "NEXT", "IN"),
                edge("e2", "variable", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                                Map.of("flag", output(booleanAlias), "text", output(stringValue))),
                        "builtin.variable", contract("builtin.variable", ComponentType.VARIABLE, Map.of(),
                                Map.of("value", output(BOOLEAN))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of("flag", true, "text", "later"))));
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("VARIABLE_SOURCE_PROTOCOL_MISMATCH", result.errorCode());
            assertTrue(result.errorMessage().contains("start.text"));
        }
    }

    @Test
    void first_end_cancels_a_still_running_sibling_instead_of_waiting_for_it() throws Exception {
        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch slowStopped = new CountDownLatch(1);
        NodeExecutor scripts = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                if (context.node().node().nodeCode().equals("gate")) {
                    try {
                        if (!slowStarted.await(2, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("slow sibling did not start");
                        }
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(exception);
                    }
                    return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
                }
                slowStarted.countDown();
                try {
                    while (true) {
                        context.throwIfStopped();
                        Thread.sleep(25);
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    context.throwIfStopped();
                    throw new IllegalStateException(exception);
                } finally {
                    if (context.cancellationToken().isCancellationRequested()) {
                        slowStopped.countDown();
                    }
                }
            }
        };
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode gate = node("gate", "test.script", "gate", Map.of());
        FlowNode slow = node("slow", "test.script", "slow", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, gate, slow, end), List.of(
                edge("e1", "start", "gate", "NEXT", "IN"),
                edge("e2", "start", "slow", "NEXT", "IN"),
                edge("e3", "gate", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "test.script", contract("test.script", ComponentType.SCRIPT, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(scripts));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of())));
            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertTrue(slowStopped.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void unexpected_node_failures_do_not_expose_exception_secrets() throws Exception {
        NodeExecutor leaking = new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                throw new IllegalStateException("password=top-secret token=private-value");
            }
        };
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode script = node("script", "test.script", "script", Map.of());
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, script, end), List.of(
                edge("e1", "start", "script", "NEXT", "IN"),
                edge("e2", "script", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "test.script", contract("test.script", ComponentType.SCRIPT, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(leaking));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("NODE_EXECUTION_FAILED", result.errorCode());
            assertEquals("node execution failed", result.errorMessage());
            assertFalse(JSON.toJSONString(result).contains("top-secret"));
            assertFalse(JSON.toJSONString(result).contains("private-value"));
        }
    }

    @Test
    void fails_when_a_bound_required_input_is_absent_but_allows_an_absent_optional_input() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        NodeExecutor script = scriptExecutor(executions);
        ExecutionPlan requiredPlan = inputPlan(true);
        ExecutionPlan optionalPlan = inputPlan(false);
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(script));

        try (FlowEngine engine = new FlowEngine(registry, ConcurrencyGovernor.defaults(), Clock.systemUTC())) {
            ExecutionResult required = await(engine.execute(requiredPlan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, required.status());
            assertEquals("NODE_REQUIRED_INPUT_MISSING", required.errorCode());
            assertEquals(0, executions.get());

            ExecutionResult optional = await(engine.execute(optionalPlan, request(Map.of())));
            assertEquals(ExecutionStatus.SUCCEEDED, optional.status());
            assertEquals("executed", optional.result());
            assertEquals(1, executions.get());
        }
    }

    @Test
    void rejects_task_6_groovy_modes_explicitly() throws Exception {
        FlowNode start = node("start", "builtin.start", "start", Map.of());
        FlowNode variable = node("variable", "builtin.variable", "variable", Map.of(
                "valueMode", "GROOVY", "groovyScript", "return true"));
        FlowNode end = node("end", "builtin.end", "end", Map.of("resultStatus", "SUCCESS"));
        ExecutionPlan plan = compile(List.of(start, variable, end), List.of(
                edge("e1", "start", "variable", "NEXT", "IN"),
                edge("e2", "variable", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), Map.of()),
                        "builtin.variable", contract("builtin.variable", ComponentType.VARIABLE, Map.of(),
                                Map.of("value", output(ANY))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult result = await(engine.execute(plan, request(Map.of())));
            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("NODE_MODE_UNSUPPORTED", result.errorCode());
        }
    }

    @Test
    void two_hundred_repeated_executions_share_no_mutable_context() throws Exception {
        FlowNode start = node("start", "builtin.start", "entry", Map.of(
                "inputProtocols", List.of(inputProtocol("value", false, ANY))));
        FlowNode variable = node("variable", "builtin.variable", "copy", Map.of(
                "valueMode", "FIXED", "valueSources", List.of(source("start", "value"))));
        FlowNode end = node("end", "builtin.end", "end", Map.of(
                "resultStatus", "SUCCESS", "returnBinding", source("variable", "value")));
        ExecutionPlan plan = compile(List.of(start, variable, end), List.of(
                edge("e1", "start", "variable", "NEXT", "IN"),
                edge("e2", "variable", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                                Map.of("value", output(ANY))),
                        "builtin.variable", contract("builtin.variable", ComponentType.VARIABLE, Map.of(),
                                Map.of("value", output(ANY))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));

        try (FlowEngine engine = new FlowEngine()) {
            List<ExecutionHandle> handles = new ArrayList<>();
            for (int index = 0; index < 200; index++) {
                handles.add(engine.execute(plan, request(Map.of("value", "execution-" + index))));
            }
            CompletableFuture.allOf(handles.stream()
                    .map(handle -> handle.completion().toCompletableFuture())
                    .toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);

            for (int index = 0; index < handles.size(); index++) {
                ExecutionHandle handle = handles.get(index);
                assertEquals("execution-" + index, handle.completion().toCompletableFuture().join().result());
                assertEquals("execution-" + index,
                        handle.context().required("entry", ScopePath.root(), "value"));
            }
        }
    }

    private static Object executeVariable(Map<String, Object> inputs, List<String> sourceKeys) throws Exception {
        Map<String, OutputSlotContract> startOutputs = new LinkedHashMap<>();
        sourceKeys.forEach(key -> startOutputs.put(key, output(ANY)));
        FlowNode start = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", sourceKeys.stream()
                        .map(key -> inputProtocol(key, false, ANY)).toList()));
        FlowNode variable = node("variable", "builtin.variable", "variable", Map.of(
                "valueMode", "FIXED",
                "valueSources", sourceKeys.stream().map(key -> source("start", key)).toList()));
        FlowNode end = node("end", "builtin.end", "end", Map.of(
                "resultStatus", "SUCCESS", "returnBinding", source("variable", "value")));
        ExecutionPlan plan = compile(List.of(start, variable, end), List.of(
                edge("e1", "start", "variable", "NEXT", "IN"),
                edge("e2", "variable", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(), startOutputs),
                        "builtin.variable", contract("builtin.variable", ComponentType.VARIABLE, Map.of(),
                                Map.of("value", output(ANY))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
        Map<String, Object> declaredInputs = new LinkedHashMap<>();
        sourceKeys.forEach(key -> declaredInputs.put(key, inputs.get(key)));
        try (FlowEngine engine = new FlowEngine()) {
            ExecutionResult result = await(engine.execute(plan, request(declaredInputs)));
            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            return result.result();
        }
    }

    private static ExecutionPlan inputPlan(boolean required) {
        FlowNode start = node("start", "builtin.start", "start", Map.of(
                "inputProtocols", List.of(inputProtocol("source", false, ANY))));
        FlowNode script = node("script", "test.script", "script", Map.of(
                "inputBindings", List.of(Map.of(
                        "inputKey", "input", "sourceNodeId", "start", "sourceOutputKey", "source"))));
        FlowNode end = node("end", "builtin.end", "end", Map.of(
                "resultStatus", "SUCCESS", "returnBinding", source("script", "value")));
        return compile(List.of(start, script, end), List.of(
                edge("e1", "start", "script", "NEXT", "IN"),
                edge("e2", "script", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, Map.of(),
                                Map.of("source", output(ANY))),
                        "test.script", contract("test.script", ComponentType.SCRIPT,
                                Map.of("input", new InputSlotContract("input", required, ANY)),
                                Map.of("value", output(ANY))),
                        "builtin.end", contract("builtin.end", ComponentType.END, Map.of(), Map.of())));
    }

    private static NodeExecutor scriptExecutor(AtomicInteger executions) {
        return new NodeExecutor() {
            @Override
            public ComponentType supports() {
                return ComponentType.SCRIPT;
            }

            @Override
            public NodeInvocation execute(NodeExecutionContext context) {
                executions.incrementAndGet();
                assertFalse(context.inputs().optionalJson("input").isPresent());
                return NodeInvocation.completed(NodeResult.continueWith(Map.of("value", "executed"), Set.of("NEXT")));
            }
        };
    }

    static ExecutionResult await(ExecutionHandle handle) throws Exception {
        return handle.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    static ExecutionRequest request(Map<String, Object> startInputs) {
        return request(startInputs, Instant.now().plus(10, ChronoUnit.SECONDS));
    }

    static ExecutionRequest request(Map<String, Object> startInputs, Instant deadline) {
        return new ExecutionRequest("inline:flow", ExecutionMode.EPHEMERAL, ExecutionPurpose.TRIAL,
                startInputs, Map.of(), null, "test-key", "tenant", deadline, Map.of());
    }

    static ExecutionPlan compile(List<FlowNode> nodes, List<FlowEdge> edges,
            Map<String, CompiledComponentContract> contracts) {
        Map<String, ProtocolSchema> protocols = new LinkedHashMap<>();
        contracts.values().forEach(contract -> {
            contract.inputSlots().values().forEach(slot -> protocols.put(slot.protocol().code(), slot.protocol()));
            contract.outputSlots().values().forEach(slot -> protocols.put(slot.protocol().code(), slot.protocol()));
        });
        ExecutableFlowBundle unhashed = new ExecutableFlowBundle("1.0",
                new FlowIdentity("tenant", "flow", "v1"), new FlowDefinition("2.0", nodes, edges),
                contracts, protocols, Map.of(), null, null);
        return new FlowCompiler().compile(unhashed.withCalculatedHash());
    }

    static FlowNode node(String id, String componentCode, String nodeCode, Map<String, Object> config) {
        return new FlowNode(id, componentCode, nodeCode, nodeCode, config);
    }

    static FlowEdge edge(String id, String source, String target, String sourcePort, String targetPort) {
        return new FlowEdge(id, source, target, sourcePort, targetPort);
    }

    static Map<String, Object> source(String sourceNodeId, String sourceOutputKey) {
        return Map.of("sourceNodeId", sourceNodeId, "sourceOutputKey", sourceOutputKey);
    }

    static Map<String, Object> inputProtocol(String key, boolean required, ProtocolSchema protocol) {
        return Map.of(
                "key", key,
                "name", "output",
                "protocolCode", protocol.code(),
                "required", required,
                "schema", protocol.schema());
    }

    static CompiledComponentContract contract(String code, ComponentType type,
            Map<String, InputSlotContract> inputs, Map<String, OutputSlotContract> outputs) {
        return new CompiledComponentContract(code, type, null, Map.of(), inputs, outputs,
                new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 1024));
    }

    static OutputSlotContract output(ProtocolSchema protocol) {
        return new OutputSlotContract("output", protocol);
    }

    static ProtocolSchema protocol(String code, Map<String, Object> schema) {
        return new ProtocolSchema(code, "1.0", schema);
    }
}
