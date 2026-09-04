package com.playrewards.flowengine.core.compile;

import static com.playrewards.flowengine.core.compile.FlowCompilerTest.bundle;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.capabilities;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.contract;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.edge;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.node;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.output;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.protocol;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Verifies compile-time safety for bindings embedded in built-in node configuration. */
class ConfiguredBindingCompilationTest {

    private static final ProtocolSchema BOOLEAN_A = protocol("boolean.a", Map.of("type", "boolean"));
    private static final ProtocolSchema BOOLEAN_B = protocol("boolean.b", Map.of("type", "boolean"));
    private static final ProtocolSchema BOOLEAN_OUT = protocol("boolean.out", Map.of("type", "boolean"));
    private static final ProtocolSchema STRING = protocol("text", Map.of("type", "string"));

    @Test
    void rejects_if_condition_sources_that_are_missing_unknown_or_not_boolean_upstream_outputs() {
        assertIssueAt(ifBundle(binding("missing", "value"), BOOLEAN_A),
                "FLOW_IF_CONDITION_SOURCE_NOT_FOUND", "config.conditionBinding");
        assertIssueAt(ifBundle(binding("start", "missing"), BOOLEAN_A),
                "FLOW_IF_CONDITION_SOURCE_OUTPUT_NOT_FOUND", "config.conditionBinding");
        assertIssueAt(ifBundle(binding("left", "value"), BOOLEAN_A),
                "FLOW_IF_CONDITION_SOURCE_NOT_UPSTREAM", "config.conditionBinding");
        assertIssueAt(ifBundle(binding("start", "value"), STRING),
                "FLOW_IF_CONDITION_SCHEMA_MISMATCH", "config.conditionBinding");
    }

    @Test
    void rejects_end_return_sources_that_are_missing_unknown_or_not_upstream_outputs() {
        assertIssueAt(endBundle(binding("missing", "value"), BOOLEAN_A, Map.of()),
                "FLOW_END_RETURN_SOURCE_NOT_FOUND", "config.returnBinding");
        assertIssueAt(endBundle(binding("start", "missing"), BOOLEAN_A, Map.of()),
                "FLOW_END_RETURN_SOURCE_OUTPUT_NOT_FOUND", "config.returnBinding");
        assertIssueAt(endBundle(binding("end", "value"), BOOLEAN_A,
                        Map.of("value", output("value", BOOLEAN_A))),
                "FLOW_END_RETURN_SOURCE_NOT_UPSTREAM", "config.returnBinding");
    }

    @Test
    void variable_accepts_reachable_mutually_exclusive_sources_with_different_protocol_codes() {
        ExecutionPlan plan = new FlowCompiler().compile(variableBundle(BOOLEAN_B,
                List.of(binding("left", "value"), binding("right", "value"))));

        assertEquals(ComponentType.VARIABLE,
                plan.nodesById().get("variable").componentContract().componentType());
    }

    @Test
    void rejects_variable_sources_that_are_missing_unknown_not_upstream_or_schema_incompatible() {
        assertIssueAt(variableBundle(BOOLEAN_B, List.of(binding("missing", "value"))),
                "FLOW_VARIABLE_SOURCE_NOT_FOUND", "config.valueSources.0");
        assertIssueAt(variableBundle(BOOLEAN_B, List.of(binding("left", "missing"))),
                "FLOW_VARIABLE_SOURCE_OUTPUT_NOT_FOUND", "config.valueSources.0");
        assertIssueAt(variableBundle(BOOLEAN_B, List.of(binding("variable", "value"))),
                "FLOW_VARIABLE_SOURCE_NOT_UPSTREAM", "config.valueSources.0");
        assertIssueAt(variableBundle(STRING, List.of(binding("right", "value"))),
                "FLOW_VARIABLE_SOURCE_SCHEMA_MISMATCH", "config.valueSources.0");
    }

    private static ExecutableFlowBundle ifBundle(Map<String, Object> conditionBinding,
            ProtocolSchema startOutput) {
        FlowNode start = node("start", "builtin.start");
        FlowNode condition = configuredNode("if", "builtin.if", Map.of("conditionMode", "DEFAULT",
                "conditionBinding", conditionBinding));
        FlowNode left = node("left", "test.left");
        FlowNode right = node("right", "test.right");
        FlowNode end = node("end", "builtin.end");
        return bundle(List.of(start, condition, left, right, end), List.of(
                edge("e1", "start", "if", "NEXT", "IN"),
                edge("e2", "if", "left", "TRUE", "IN"),
                edge("e3", "if", "right", "FALSE", "IN"),
                edge("e4", "left", "end", "NEXT", "IN"),
                edge("e5", "right", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(),
                                Map.of("value", output("value", startOutput))),
                        "builtin.if", contract("builtin.if", ComponentType.IF, null, Map.of(), Map.of()),
                        "test.left", contract("test.left", ComponentType.SCRIPT, null, Map.of(),
                                Map.of("value", output("value", BOOLEAN_A))),
                        "test.right", contract("test.right", ComponentType.SCRIPT, null, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())),
                Map.of());
    }

    private static ExecutableFlowBundle endBundle(Map<String, Object> returnBinding,
            ProtocolSchema startOutput, Map<String, com.playrewards.flowengine.contract.bundle.OutputSlotContract>
                    endOutputs) {
        FlowNode start = node("start", "builtin.start");
        FlowNode end = configuredNode("end", "builtin.end", Map.of(
                "resultStatus", "SUCCESS", "returnBinding", returnBinding));
        return bundle(List.of(start, end), List.of(edge("e1", "start", "end", "NEXT", "IN")), Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(),
                        Map.of("value", output("value", startOutput))),
                "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), endOutputs)), Map.of());
    }

    private static ExecutableFlowBundle variableBundle(ProtocolSchema rightOutput,
            List<Map<String, Object>> valueSources) {
        FlowNode start = node("start", "builtin.start");
        FlowNode condition = configuredNode("if", "builtin.if", Map.of("conditionMode", "DEFAULT",
                "conditionBinding", binding("start", "condition")));
        FlowNode left = node("left", "test.left");
        FlowNode right = node("right", "test.right");
        FlowNode variable = configuredNode("variable", "builtin.variable", Map.of(
                "valueMode", "FIXED", "valueSources", valueSources));
        FlowNode end = node("end", "builtin.end");
        return bundle(List.of(start, condition, left, right, variable, end), List.of(
                edge("e1", "start", "if", "NEXT", "IN"),
                edge("e2", "if", "left", "TRUE", "IN"),
                edge("e3", "if", "right", "FALSE", "IN"),
                edge("e4", "left", "variable", "NEXT", "IN"),
                edge("e5", "right", "variable", "NEXT", "IN"),
                edge("e6", "variable", "end", "NEXT", "IN")), Map.of(
                        "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(),
                                Map.of("condition", output("condition", BOOLEAN_A))),
                        "builtin.if", contract("builtin.if", ComponentType.IF, null, Map.of(), Map.of()),
                        "test.left", contract("test.left", ComponentType.SCRIPT, null, Map.of(),
                                Map.of("value", output("value", BOOLEAN_A))),
                        "test.right", contract("test.right", ComponentType.SCRIPT, null, Map.of(),
                                Map.of("value", output("value", rightOutput))),
                        "builtin.variable", new CompiledComponentContract(
                                "builtin.variable", ComponentType.VARIABLE, null, Map.of(), Map.of(),
                                Map.of("value", output("value", BOOLEAN_OUT)), capabilities()),
                        "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())),
                Map.of());
    }

    private static FlowNode configuredNode(String id, String componentCode, Map<String, Object> config) {
        return new FlowNode(id, componentCode, id, id, config);
    }

    private static Map<String, Object> binding(String sourceNodeId, String sourceOutputKey) {
        return Map.of("sourceNodeId", sourceNodeId, "sourceOutputKey", sourceOutputKey);
    }

    private static void assertIssueAt(ExecutableFlowBundle bundle, String code, String path) {
        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(bundle));
        assertEquals(1, failure.issues().size(), () -> "unexpected issue noise: " + failure.issues());
        assertEquals(code, failure.issues().getFirst().code());
        assertEquals(path, failure.issues().getFirst().path());
    }
}
