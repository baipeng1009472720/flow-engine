package com.playrewards.flowengine.core.compile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Drives deterministic compilation of immutable executable bundles. */
class FlowCompilerTest {

    private static final ProtocolSchema ORDER = protocol("order", Map.of(
            "type", "object", "required", List.of("id"),
            "properties", Map.of("id", Map.of("type", "string")), "additionalProperties", false));

    @Test
    void compiles_a_valid_bundle_into_a_deeply_immutable_indexed_plan() {
        FlowNode start = node("start", "builtin.start");
        FlowNode work = node("work", "business.work");
        FlowNode end = node("end", "builtin.end");
        ExecutableFlowBundle bundle = bundle(List.of(start, work, end), List.of(
                edge("e1", "start", "work", "NEXT", "IN"),
                edge("e2", "work", "end", "NEXT", "IN")), Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(),
                        Map.of("order", output("订单", ORDER))),
                "business.work", contract("business.work", ComponentType.BUSINESS, "workExecutor",
                        Map.of("order", input("订单", true, ORDER)), Map.of()),
                "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())),
                Map.of("work", bindings(binding("order", "start", "order"))));

        ExecutionPlan plan = new FlowCompiler().compile(bundle);

        assertEquals(bundle.flowIdentity(), plan.flowIdentity());
        assertEquals(bundle.bundleHash(), plan.bundleHash());
        assertEquals(List.of("work"), plan.forwardAdjacency().get("start"));
        assertEquals(List.of("start"), plan.reverseAdjacency().get("work"));
        assertEquals("workExecutor", plan.nodesById().get("work").componentContract().executorRef());
        assertEquals(List.of("start"), plan.activationPredecessors().get("work"));
        assertThrows(UnsupportedOperationException.class,
                () -> plan.forwardAdjacency().get("start").add("end"));
        assertThrows(UnsupportedOperationException.class,
                () -> plan.nodesById().put("other", plan.nodesById().get("work")));
    }

    @Test
    void rejects_missing_or_changed_hash_before_compilation() {
        ExecutableFlowBundle valid = simpleBundle();
        ExecutableFlowBundle missing = copyWithHash(valid, null);
        ExecutableFlowBundle changed = copyWithHash(valid, "f".repeat(64));

        assertIssue(new FlowCompiler(), missing, "FLOW_BUNDLE_HASH_REQUIRED");
        assertIssue(new FlowCompiler(), changed, "FLOW_BUNDLE_HASH_MISMATCH");
    }

    @Test
    void aggregates_component_identity_unknown_component_missing_executor_and_legacy_type_deterministically() {
        List<FlowNode> nodes = List.of(
                node("start", "builtin.start"),
                new FlowNode("a", "missing", "duplicate", "a", Map.of()),
                new FlowNode("b", "business.no-executor", "duplicate", "b", Map.of()),
                node("merge", "legacy.merge"), node("end", "builtin.end"));
        Map<String, CompiledComponentContract> contracts = new HashMap<>();
        contracts.put("builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(), Map.of()));
        contracts.put("business.no-executor", contract("business.no-executor", ComponentType.BUSINESS, null,
                Map.of(), Map.of()));
        contracts.put("legacy.merge", contract("legacy.merge", ComponentType.MERGE, null, Map.of(), Map.of()));
        contracts.put("builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of()));
        ExecutableFlowBundle bundle = bundle(nodes, List.of(
                edge("e1", "start", "a", "NEXT", "IN"), edge("e2", "a", "b", "NEXT", "IN"),
                edge("e3", "b", "merge", "NEXT", "IN"), edge("e4", "merge", "end", "NEXT", "IN")),
                contracts, Map.of());

        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(bundle));
        List<String> codes = failure.issues().stream().map(CompilationIssue::code).toList();

        assertTrue(codes.contains("FLOW_NODE_CODE_DUPLICATE"));
        assertTrue(codes.contains("FLOW_COMPONENT_NOT_FOUND"));
        assertTrue(codes.contains("FLOW_COMPONENT_EXECUTOR_REQUIRED"));
        assertTrue(codes.contains("FLOW_COMPONENT_TYPE_LEGACY"));
        assertTrue(failure.getMessage().contains("MERGE"));
        assertTrue(failure.getMessage().contains("迁移"));
        assertEquals(failure.issues(), assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(bundle)).issues());
    }

    @Test
    void rejects_invalid_config_and_missing_required_binding_but_allows_optional_input_to_be_absent() {
        CompiledComponentContract work = new CompiledComponentContract(
                "business.work", ComponentType.BUSINESS, "workExecutor",
                Map.of("type", "object", "required", List.of("mode"),
                        "properties", Map.of("mode", Map.of("type", "string"))),
                Map.of("required", input("必填", true, ORDER), "optional", input("可选", false, ORDER)),
                Map.of(), capabilities());
        ExecutableFlowBundle bundle = linearBundle(work, Map.of("unexpected", true));

        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(bundle));

        assertTrue(hasIssue(failure, "FLOW_NODE_CONFIG_INVALID"));
        assertTrue(hasIssue(failure, "FLOW_INPUT_REQUIRED"));
        assertFalse(failure.issues().stream().anyMatch(issue -> issue.message().contains("可选")));
    }

    @Test
    void validates_binding_shape_source_slot_protocol_schema_and_dominance() {
        ProtocolSchema weakOrder = protocol("order", Map.of("type", "object"));
        CompiledComponentContract start = contract("builtin.start", ComponentType.START, null, Map.of(),
                Map.of("order", output("订单", weakOrder)));
        CompiledComponentContract work = contract("business.work", ComponentType.BUSINESS, "workExecutor",
                Map.of("order", input("订单", true, ORDER)), Map.of());
        ExecutableFlowBundle bundle = bundle(
                List.of(node("start", "builtin.start"), node("left", "business.work"),
                        node("right", "business.work"), node("end", "builtin.end")),
                List.of(edge("e1", "start", "left", "NEXT", "IN"),
                        edge("e2", "start", "right", "NEXT", "IN"),
                        edge("e3", "left", "end", "NEXT", "IN"),
                        edge("e4", "right", "end", "NEXT", "IN")),
                Map.of("builtin.start", start, "business.work", work,
                        "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())),
                Map.of("left", bindings(binding("order", "right", "missing")),
                        "right", bindings(binding("order", "start", "order"))));

        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(bundle));

        assertTrue(hasIssue(failure, "FLOW_INPUT_SOURCE_OUTPUT_NOT_FOUND"));
        assertTrue(hasIssue(failure, "FLOW_INPUT_SOURCE_NOT_UPSTREAM"));
        assertTrue(hasIssue(failure, "FLOW_INPUT_SCHEMA_MISMATCH"));
    }

    @Test
    void rejects_duplicate_input_keys_and_exact_protocol_code_mismatch() {
        ProtocolSchema customer = protocol("customer", Map.of("type", "object"));
        CompiledComponentContract start = contract("builtin.start", ComponentType.START, null, Map.of(),
                Map.of("customer", output("客户", customer)));
        CompiledComponentContract work = contract("business.work", ComponentType.BUSINESS, "workExecutor",
                Map.of("order", input("订单", true, ORDER)), Map.of());
        ExecutableFlowBundle bundle = bundle(
                List.of(node("start", "builtin.start"), node("work", "business.work"), node("end", "builtin.end")),
                List.of(edge("e1", "start", "work", "NEXT", "IN"),
                        edge("e2", "work", "end", "NEXT", "IN")),
                Map.of("builtin.start", start, "business.work", work,
                        "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())),
                Map.of("work", Map.of("inputBindings", List.of(
                        binding("order", "start", "customer"), binding("order", "start", "customer")))));

        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(bundle));

        assertTrue(hasIssue(failure, "FLOW_INPUT_BINDINGS_INVALID"));
        assertTrue(hasIssue(failure, "FLOW_INPUT_PROTOCOL_MISMATCH"));
    }

    @Test
    void excludes_all_engine_reserved_fields_from_strict_component_config_schema() {
        CompiledComponentContract work = new CompiledComponentContract(
                "business.work", ComponentType.BUSINESS, "workExecutor",
                Map.of("type", "object", "additionalProperties", false,
                        "required", List.of("mode"),
                        "properties", Map.of("mode", Map.of("type", "string"))),
                Map.of("order", input("订单", true, ORDER)), Map.of(), capabilities());
        List<FlowNode> nodes = List.of(node("start", "builtin.start"), node("work", "business.work"),
                node("end", "builtin.end"));
        List<FlowEdge> edges = List.of(edge("e1", "start", "work", "NEXT", "IN"),
                edge("e2", "work", "end", "NEXT", "IN"));
        Map<String, CompiledComponentContract> contracts = Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(),
                        Map.of("order", output("订单", ORDER))),
                "business.work", work,
                "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of()));
        Map<String, Object> validConfig = Map.of(
                "mode", "strict",
                "inputBindings", List.of(binding("order", "start", "order")),
                "resourceKey", "tenant:42");

        ExecutionPlan plan = new FlowCompiler().compile(bundle(nodes, edges, contracts, Map.of("work", validConfig)));
        assertTrue(plan.nodesById().get("work").inputBindings().containsKey("order"));

        Map<String, Object> unknownConfig = Map.of("mode", "strict", "unexpected", true, "inputBindings",
                List.of(binding("order", "start", "order")));
        assertIssue(new FlowCompiler(), bundle(nodes, edges, contracts, Map.of("work", unknownConfig)),
                "FLOW_NODE_CONFIG_INVALID");
    }

    @Test
    void config_validation_never_exposes_fastjson_messages_or_rejected_values() {
        String marker = "customer-private-marker";
        CompiledComponentContract work = new CompiledComponentContract(
                "business.work", ComponentType.BUSINESS, "workExecutor",
                Map.of("type", "object", "additionalProperties", false,
                        "required", List.of("mode"),
                        "properties", Map.of("mode", Map.of("const", "allowed"))),
                Map.of(), Map.of(), capabilities());

        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> new FlowCompiler().compile(linearBundle(work, Map.of("mode", marker))));
        CompilationIssue issue = failure.issues().stream()
                .filter(candidate -> candidate.code().equals("FLOW_NODE_CONFIG_INVALID"))
                .findFirst().orElseThrow();

        assertEquals("节点配置不符合组件 Schema", issue.message());
        assertFalse(issue.toString().contains(marker));
        assertFalse(failure.toString().contains(marker));
    }

    private static ExecutableFlowBundle linearBundle(CompiledComponentContract work, Map<String, Object> workConfig) {
        return bundle(List.of(node("start", "builtin.start"),
                        new FlowNode("work", "business.work", "work", "work", workConfig),
                        node("end", "builtin.end")),
                List.of(edge("e1", "start", "work", "NEXT", "IN"),
                        edge("e2", "work", "end", "NEXT", "IN")),
                Map.of("builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(), Map.of()),
                        "business.work", work,
                        "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())), Map.of());
    }

    static ExecutableFlowBundle simpleBundle() {
        return bundle(List.of(node("start", "builtin.start"), node("end", "builtin.end")),
                List.of(edge("e1", "start", "end", "NEXT", "IN")),
                Map.of("builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(), Map.of()),
                        "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of())), Map.of());
    }

    static ExecutableFlowBundle bundle(List<FlowNode> originalNodes, List<FlowEdge> edges,
            Map<String, CompiledComponentContract> contracts, Map<String, Map<String, Object>> configOverrides) {
        List<FlowNode> nodes = new ArrayList<>();
        for (FlowNode node : originalNodes) {
            Map<String, Object> config = configOverrides.getOrDefault(node.id(), node.config());
            nodes.add(new FlowNode(node.id(), node.componentCode(), node.nodeCode(), node.nodeName(), config));
        }
        Map<String, ProtocolSchema> protocols = new HashMap<>();
        contracts.values().forEach(contract -> {
            contract.inputSlots().values().forEach(slot -> protocols.put(slot.protocol().code(), slot.protocol()));
            contract.outputSlots().values().forEach(slot -> protocols.put(slot.protocol().code(), slot.protocol()));
        });
        ExecutableFlowBundle unhashed = new ExecutableFlowBundle("1.0", new FlowIdentity("tenant", "flow", "v1"),
                new FlowDefinition("2.0", nodes, edges), contracts, protocols, Map.of(), null, null);
        return unhashed.withCalculatedHash();
    }

    static FlowNode node(String id, String componentCode) {
        return new FlowNode(id, componentCode, id, id, Map.of());
    }

    static FlowEdge edge(String id, String source, String target, String sourcePort, String targetPort) {
        return new FlowEdge(id, source, target, sourcePort, targetPort);
    }

    static CompiledComponentContract contract(String code, ComponentType type, String executor,
            Map<String, InputSlotContract> inputs, Map<String, OutputSlotContract> outputs) {
        return new CompiledComponentContract(code, type, executor, Map.of(), inputs, outputs, capabilities());
    }

    static ComponentCapabilities capabilities() {
        return new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 1);
    }

    static ProtocolSchema protocol(String code, Map<String, Object> schema) {
        return new ProtocolSchema(code, "1.0", schema);
    }

    static InputSlotContract input(String name, boolean required, ProtocolSchema protocol) {
        return new InputSlotContract(name, required, protocol);
    }

    static OutputSlotContract output(String name, ProtocolSchema protocol) {
        return new OutputSlotContract(name, protocol);
    }

    static Map<String, Object> binding(String inputKey, String sourceNodeId, String sourceOutputKey) {
        return Map.of("inputKey", inputKey, "sourceNodeId", sourceNodeId, "sourceOutputKey", sourceOutputKey);
    }

    @SafeVarargs
    static Map<String, Object> bindings(Map<String, Object>... bindings) {
        return Map.of("inputBindings", List.of(bindings));
    }

    private static ExecutableFlowBundle copyWithHash(ExecutableFlowBundle bundle, String hash) {
        return new ExecutableFlowBundle(bundle.contractVersion(), bundle.flowIdentity(), bundle.flowDefinition(),
                bundle.componentContracts(), bundle.protocolSchemas(), bundle.externalScopeSchemas(), hash, bundle.trust());
    }

    static void assertIssue(FlowCompiler compiler, ExecutableFlowBundle bundle, String code) {
        FlowCompilationException failure = assertThrows(FlowCompilationException.class, () -> compiler.compile(bundle));
        assertTrue(hasIssue(failure, code), () -> "missing issue " + code + ": " + failure.issues());
    }

    static boolean hasIssue(FlowCompilationException failure, String code) {
        return failure.issues().stream().anyMatch(issue -> code.equals(issue.code()));
    }
}
