package com.playrewards.flowengine.core.compile;

import static com.playrewards.flowengine.core.compile.FlowCompilerTest.assertIssue;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.bundle;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.contract;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.edge;
import static com.playrewards.flowengine.core.compile.FlowCompilerTest.node;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.flow.FlowNode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Covers built-in graph structure rules and stable sub-flow resolution. */
class StructureCompilationTest {

    @Test
    void rejects_identity_endpoint_port_reachability_and_non_loop_cycles() {
        Map<String, CompiledComponentContract> contracts = baseContracts();
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"),
                        new FlowNode("start", "builtin.work", "work", "duplicate-id", Map.of()),
                        node("orphan", "builtin.work"), node("end", "builtin.end")),
                List.of(edge("edge", "start", "end", "BAD", "IN"),
                        edge("edge", "end", "start", "NEXT", "BAD"),
                        edge("missing", "ghost", "end", "NEXT", "IN")), contracts, Map.of());

        FlowCompiler compiler = new FlowCompiler();
        assertIssue(compiler, bundle, "FLOW_NODE_ID_DUPLICATE");
        assertIssue(compiler, bundle, "FLOW_EDGE_ID_DUPLICATE");
        assertIssue(compiler, bundle, "FLOW_EDGE_SOURCE_NOT_FOUND");
        assertIssue(compiler, bundle, "FLOW_EDGE_SOURCE_PORT_INVALID");
        assertIssue(compiler, bundle, "FLOW_EDGE_TARGET_PORT_INVALID");
        assertIssue(compiler, bundle, "FLOW_GRAPH_CYCLE");
        assertIssue(compiler, bundle, "FLOW_NODE_UNREACHABLE");
    }

    @Test
    void requires_at_least_one_end_node() {
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"),
                        node("work", "builtin.work")),
                List.of(edge("e1", "start", "work", "NEXT", "IN")), baseContracts(), Map.of());

        assertIssue(new FlowCompiler(), bundle, "FLOW_END_COUNT_INVALID");
    }

    @Test
    void requires_if_true_false_and_parallel_two_branch_edges() {
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"),
                        node("if", "builtin.if"), node("parallel", "builtin.parallel"), node("end", "builtin.end")),
                List.of(edge("e1", "start", "if", "NEXT", "IN"),
                        edge("e2", "if", "parallel", "TRUE", "IN"),
                        edge("e3", "parallel", "end", "BRANCH", "IN")), baseContracts(), Map.of());

        assertIssue(new FlowCompiler(), bundle, "FLOW_IF_BRANCHES_INVALID");
        assertIssue(new FlowCompiler(), bundle, "FLOW_PARALLEL_BRANCH_COUNT_INVALID");
    }

    @Test
    void accepts_multiple_parallel_targets_from_the_single_branch_port() {
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"),
                        node("parallel", "builtin.parallel"), node("leftEnd", "builtin.end"),
                        node("rightEnd", "builtin.end")),
                List.of(edge("e1", "start", "parallel", "NEXT", "IN"),
                        edge("e2", "parallel", "leftEnd", "BRANCH", "IN"),
                        edge("e3", "parallel", "rightEnd", "BRANCH", "IN")), baseContracts(), Map.of());

        ExecutionPlan plan = new FlowCompiler().compile(bundle);

        assertEquals(List.of("leftEnd", "rightEnd"), plan.outgoingByPort().get("parallel").get("BRANCH"));
    }

    @Test
    void accepts_paired_loop_back_edge_and_records_loop_pair() {
        Map<String, Map<String, Object>> configs = Map.of(
                "loop", Map.of("role", "START", "pairNodeId", "loopEnd", "maxIterations", 10),
                "loopEnd", Map.of("role", "END", "pairNodeId", "loop"));
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"), node("loop", "builtin.loop"),
                        node("body", "builtin.work"), node("loopEnd", "builtin.loop"), node("end", "builtin.end")),
                List.of(edge("entry", "start", "loop", "NEXT", "ENTRY"),
                        edge("body", "loop", "body", "BODY", "IN"),
                        edge("toEnd", "body", "loopEnd", "NEXT", "IN"),
                        edge("back", "loopEnd", "loop", "NEXT", "NEXT"),
                        edge("done", "loop", "end", "DONE", "IN")), baseContracts(), configs);

        ExecutionPlan plan = new FlowCompiler().compile(bundle);

        assertEquals("loopEnd", plan.loopPairs().get("loop"));
        assertEquals("loop", plan.loopPairs().get("loopEnd"));
    }

    @Test
    void rejects_invalid_loop_and_lock_pairing_and_lock_release_bypass() {
        Map<String, Map<String, Object>> configs = Map.of(
                "loop", Map.of("role", "START", "pairNodeId", "missing", "maxIterations", 0),
                "acquire", Map.of("role", "ACQUIRE", "pairNodeId", "release"),
                "release", Map.of("role", "RELEASE", "pairNodeId", "acquire"));
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"), node("loop", "builtin.loop"),
                        node("acquire", "builtin.lock"), node("release", "builtin.lock"), node("end", "builtin.end")),
                List.of(edge("e1", "start", "loop", "NEXT", "ENTRY"),
                        edge("e2", "loop", "acquire", "DONE", "IN"),
                        edge("e3", "acquire", "end", "NEXT", "IN")), baseContracts(), configs);

        assertIssue(new FlowCompiler(), bundle, "FLOW_LOOP_PAIR_INVALID");
        assertIssue(new FlowCompiler(), bundle, "FLOW_LOOP_MAX_ITERATIONS_INVALID");
        assertIssue(new FlowCompiler(), bundle, "FLOW_LOCK_RELEASE_BYPASS");
    }

    @Test
    void accepts_mutual_lock_pair_when_every_critical_path_reaches_release() {
        Map<String, Map<String, Object>> configs = Map.of(
                "acquire", Map.of("role", "ACQUIRE", "pairNodeId", "release", "waitMillis", 0),
                "release", Map.of("role", "RELEASE", "pairNodeId", "acquire", "leaseMillis", 100));
        ExecutableFlowBundle bundle = bundle(List.of(node("start", "builtin.start"),
                        node("acquire", "builtin.lock"), node("work", "builtin.work"),
                        node("release", "builtin.lock"), node("end", "builtin.end")),
                List.of(edge("e1", "start", "acquire", "NEXT", "IN"),
                        edge("e2", "acquire", "work", "NEXT", "IN"),
                        edge("e3", "work", "release", "NEXT", "IN"),
                        edge("e4", "release", "end", "NEXT", "IN")), baseContracts(), configs);

        ExecutionPlan plan = new FlowCompiler().compile(bundle);

        assertEquals("release", plan.lockPairs().get("acquire"));
        assertEquals("acquire", plan.lockPairs().get("release"));
    }

    @Test
    void resolves_and_freezes_sub_flow_plan_but_rejects_direct_and_indirect_recursion() {
        ExecutableFlowBundle child = withIdentity(FlowCompilerTest.simpleBundle(), "child");
        ExecutableFlowBundle parent = subFlowBundle("parent", "child");
        RelatedFlowResolver resolver = (identity, targetFlowId) -> child;

        ExecutionPlan plan = new FlowCompiler(resolver).compile(parent);

        assertEquals(child.flowIdentity(), plan.subFlowPlans().get("sub").flowIdentity());
        assertEquals(child.bundleHash(), plan.subFlowPlans().get("sub").bundleHash());
        assertIssue(new FlowCompiler(), subFlowBundle("parent", "parent"), "FLOW_SUB_FLOW_RECURSION");

        ExecutableFlowBundle childCallsParent = subFlowBundle("child", "parent");
        RelatedFlowResolver recursiveResolver = (identity, targetFlowId) -> switch (targetFlowId) {
            case "child" -> childCallsParent;
            case "parent" -> parent;
            default -> throw new IllegalArgumentException("not found");
        };
        assertIssue(new FlowCompiler(recursiveResolver), parent, "FLOW_SUB_FLOW_RECURSION");
    }

    private static ExecutableFlowBundle subFlowBundle(String flowId, String targetFlowId) {
        ExecutableFlowBundle raw = bundle(List.of(node("start", "builtin.start"), node("sub", "builtin.subflow"),
                        node("end", "builtin.end")),
                List.of(edge("e1", "start", "sub", "NEXT", "IN"),
                        edge("e2", "sub", "end", "NEXT", "IN")), baseContracts(),
                Map.of("sub", Map.of("targetFlowId", targetFlowId)));
        return withIdentity(raw, flowId);
    }

    private static ExecutableFlowBundle withIdentity(ExecutableFlowBundle bundle, String flowId) {
        ExecutableFlowBundle raw = new ExecutableFlowBundle(bundle.contractVersion(),
                new FlowIdentity(bundle.flowIdentity().tenantId(), flowId, bundle.flowIdentity().version()),
                bundle.flowDefinition(), bundle.componentContracts(), bundle.protocolSchemas(),
                bundle.externalScopeSchemas(), null, bundle.trust());
        return raw.withCalculatedHash();
    }

    private static Map<String, CompiledComponentContract> baseContracts() {
        return Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(), Map.of()),
                "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of()),
                "builtin.if", contract("builtin.if", ComponentType.IF, null, Map.of(), Map.of()),
                "builtin.parallel", contract("builtin.parallel", ComponentType.PARALLEL, null, Map.of(), Map.of()),
                "builtin.loop", contract("builtin.loop", ComponentType.LOOP, null, Map.of(), Map.of()),
                "builtin.lock", contract("builtin.lock", ComponentType.LOCK, null, Map.of(), Map.of()),
                "builtin.subflow", contract("builtin.subflow", ComponentType.SUB_FLOW, null, Map.of(), Map.of()),
                "builtin.work", contract("builtin.work", ComponentType.SCRIPT, null, Map.of(), Map.of()));
    }
}
