package com.playrewards.flowengine.core.compile;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.schema.JSONSchema;
import com.alibaba.fastjson2.schema.ValidateResult;
import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.core.compile.CompiledNode.InputBinding;
import com.playrewards.flowengine.core.protocol.JsonSchemaCompatibility;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Compiles an immutable bundle through deterministic contract, graph, structure and binding passes.
 *
 * <p>This is the single pre-execution rejection gate. It never resolves business beans and only uses
 * the optional host resolver for same-version sub-flow bundle snapshots.</p>
 */
public final class FlowCompiler {

    private static final Comparator<CompilationIssue> ISSUE_ORDER = Comparator
            .comparing(CompilationIssue::code)
            .thenComparing(issue -> issue.nodeId() == null ? "" : issue.nodeId())
            .thenComparing(CompilationIssue::path)
            .thenComparing(CompilationIssue::message);

    private final RelatedFlowResolver relatedFlowResolver;
    private final JsonSchemaCompatibility schemaCompatibility = new JsonSchemaCompatibility();

    /** Creates a compiler that rejects SUB_FLOW nodes because no host resolver is available. */
    public FlowCompiler() {
        this(null);
    }

    /**
     * Creates a compiler using a host-supplied same-version sub-flow resolver.
     *
     * @param relatedFlowResolver resolver that returns immutable related bundles, or null to disable
     */
    public FlowCompiler(RelatedFlowResolver relatedFlowResolver) {
        this.relatedFlowResolver = relatedFlowResolver;
    }

    /**
     * Validates and compiles the complete bundle into a deeply immutable plan.
     *
     * @param bundle exact static execution input
     * @return immutable execution plan
     * @throws FlowCompilationException when any deterministic compile issue is found
     */
    public ExecutionPlan compile(ExecutableFlowBundle bundle) {
        Objects.requireNonNull(bundle, "bundle must not be null");
        return compile(bundle, new LinkedHashSet<>(), new HashMap<>());
    }

    private ExecutionPlan compile(ExecutableFlowBundle bundle, LinkedHashSet<FlowIdentity> ancestors,
            Map<FlowIdentity, ExecutionPlan> compiledPlans) {
        List<CompilationIssue> issues = new ArrayList<>();
        issues.addAll(BundleHashValidator.issues(bundle));

        List<FlowNode> nodes = bundle.flowDefinition().nodes().stream()
                .sorted(Comparator.comparing(FlowNode::id).thenComparing(FlowNode::nodeCode))
                .toList();
        List<FlowEdge> edges = bundle.flowDefinition().edges().stream()
                .sorted(Comparator.comparing(FlowEdge::id)
                        .thenComparing(FlowEdge::sourceNodeId)
                        .thenComparing(FlowEdge::targetNodeId))
                .toList();
        Map<String, FlowNode> nodesById = indexNodes(nodes, issues);
        validateNodeCodes(nodes, issues);
        Map<String, CompiledComponentContract> contractsByNode = resolveComponents(
                nodesById.values(), bundle.componentContracts(), bundle.protocolSchemas(), issues);
        validateSchemaKeys(bundle.protocolSchemas(), "protocolSchemas", issues);
        validateSchemaKeys(bundle.externalScopeSchemas(), "externalScopeSchemas", issues);
        validateConfigs(nodesById, contractsByNode, issues);

        Map<String, String> loopPairs = validateLoopPairs(nodesById, contractsByNode, issues);
        Graph graph = inspectGraph(nodesById, edges, contractsByNode, loopPairs, issues);
        validateStartEndAndReachability(nodesById, contractsByNode, graph, issues);
        validateBranchStructures(nodesById, contractsByNode, graph, issues);
        validateLoopStructure(nodesById, contractsByNode, graph, loopPairs, issues);
        Map<String, String> lockPairs = validateLockStructure(nodesById, contractsByNode, graph, issues);

        Map<String, Map<String, InputBinding>> bindingsByNode = parseAndValidateBindings(
                nodesById, contractsByNode, graph, issues);
        Map<String, ExecutionPlan> subFlowPlans = resolveSubFlows(
                bundle, nodesById, contractsByNode, ancestors, compiledPlans, issues);

        if (!issues.isEmpty()) {
            issues.sort(ISSUE_ORDER);
            throw new FlowCompilationException(issues);
        }

        Map<String, CompiledNode> compiledNodes = new TreeMap<>();
        nodesById.forEach((id, node) -> compiledNodes.put(id, new CompiledNode(
                node, contractsByNode.get(id), bindingsByNode.getOrDefault(id, Map.of()))));
        ExecutionPlan plan = new ExecutionPlan(bundle.flowIdentity(), bundle.bundleHash(), compiledNodes,
                graph.rawForward(), graph.rawReverse(), graph.outgoingByPort(), graph.incomingByPort(),
                graph.rawReverse(), loopPairs, lockPairs, subFlowPlans);
        compiledPlans.put(bundle.flowIdentity(), plan);
        return plan;
    }

    private static Map<String, FlowNode> indexNodes(List<FlowNode> nodes, List<CompilationIssue> issues) {
        Map<String, FlowNode> result = new TreeMap<>();
        for (FlowNode node : nodes) {
            if (result.putIfAbsent(node.id(), node) != null) {
                issues.add(issue("FLOW_NODE_ID_DUPLICATE", node.id(), "nodes." + node.id() + ".id",
                        "流程节点 ID 不能重复: " + node.id()));
            }
        }
        return result;
    }

    private static void validateNodeCodes(List<FlowNode> nodes, List<CompilationIssue> issues) {
        Set<String> codes = new HashSet<>();
        for (FlowNode node : nodes) {
            if (!codes.add(node.nodeCode())) {
                issues.add(issue("FLOW_NODE_CODE_DUPLICATE", node.id(), "nodeCode",
                        "流程内节点编码不能重复: " + node.nodeCode()));
            }
        }
    }

    private static Map<String, CompiledComponentContract> resolveComponents(
            Collection<FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts,
            Map<String, ProtocolSchema> protocols,
            List<CompilationIssue> issues) {
        Map<String, CompiledComponentContract> result = new TreeMap<>();
        for (Map.Entry<String, CompiledComponentContract> entry : new TreeMap<>(contracts).entrySet()) {
            if (!entry.getKey().equals(entry.getValue().componentCode())) {
                issues.add(issue("FLOW_COMPONENT_CONTRACT_KEY_MISMATCH", null,
                        "componentContracts." + entry.getKey(), "组件契约 key 必须等于 componentCode"));
            }
        }
        for (FlowNode node : nodes) {
            CompiledComponentContract contract = contracts.get(node.componentCode());
            if (contract == null) {
                issues.add(issue("FLOW_COMPONENT_NOT_FOUND", node.id(), "componentCode",
                        "流程引用的组件不存在: " + node.componentCode()));
                continue;
            }
            result.put(node.id(), contract);
            if (contract.componentType() == ComponentType.BUSINESS && contract.executorRef() == null) {
                issues.add(issue("FLOW_COMPONENT_EXECUTOR_REQUIRED", node.id(), "componentCode.executorRef",
                        "BUSINESS 组件必须声明非空 executorRef"));
            } else if (contract.componentType() != ComponentType.BUSINESS && contract.executorRef() != null) {
                issues.add(issue("FLOW_BUILTIN_EXECUTOR_FORBIDDEN", node.id(), "componentCode.executorRef",
                        "内置节点不得依赖业务 executorRef"));
            }
            if (contract.componentType() == ComponentType.MERGE
                    || contract.componentType() == ComponentType.SWITCH) {
                issues.add(issue("FLOW_COMPONENT_TYPE_LEGACY", node.id(), "componentCode",
                        contract.componentType() + " 是历史组件类型，请迁移为 IF、PARALLEL 或普通节点"));
            }
            validateSlotProtocols(node, contract.inputSlots().values().stream()
                    .map(InputSlotContract::protocol).toList(), protocols, issues);
            validateSlotProtocols(node, contract.outputSlots().values().stream()
                    .map(OutputSlotContract::protocol).toList(), protocols, issues);
        }
        return result;
    }

    private static void validateSlotProtocols(FlowNode node, List<ProtocolSchema> slots,
            Map<String, ProtocolSchema> protocols, List<CompilationIssue> issues) {
        for (ProtocolSchema slot : slots) {
            ProtocolSchema resolved = protocols.get(slot.code());
            if (resolved == null) {
                issues.add(issue("FLOW_PROTOCOL_NOT_FOUND", node.id(), "componentCode",
                        "组件槽位协议未包含在 bundle: " + slot.code()));
            } else if (!resolved.equals(slot)) {
                issues.add(issue("FLOW_PROTOCOL_CONTRACT_MISMATCH", node.id(), "componentCode",
                        "组件槽位协议与 bundle 协议快照不一致: " + slot.code()));
            }
        }
    }

    private static void validateSchemaKeys(Map<String, ProtocolSchema> schemas, String path,
            List<CompilationIssue> issues) {
        for (Map.Entry<String, ProtocolSchema> entry : new TreeMap<>(schemas).entrySet()) {
            if (!entry.getKey().equals(entry.getValue().code())) {
                issues.add(issue("FLOW_PROTOCOL_KEY_MISMATCH", null, path + "." + entry.getKey(),
                        "协议 map key 必须等于协议 code: " + entry.getValue().code()));
            }
        }
    }

    private static void validateConfigs(Map<String, FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts, List<CompilationIssue> issues) {
        for (FlowNode node : nodes.values()) {
            CompiledComponentContract contract = contracts.get(node.id());
            if (contract == null || contract.configSchema().isEmpty()) {
                continue;
            }
            try {
                JSONSchema schema = JSONSchema.of(JSON.parseObject(JSON.toJSONString(contract.configSchema())));
                Map<String, Object> componentConfig = new LinkedHashMap<>(
                        JsonValues.copyObject(node.config(), "node config"));
                componentConfig.remove("inputBindings");
                ValidateResult result = schema.validate(componentConfig);
                if (!result.isSuccess()) {
                    issues.add(issue("FLOW_NODE_CONFIG_INVALID", node.id(), "config", result.getMessage()));
                }
            } catch (RuntimeException exception) {
                issues.add(issue("FLOW_COMPONENT_CONFIG_SCHEMA_INVALID", node.id(), "componentCode.configSchema",
                        "组件配置 Schema 无法校验: " + exception.getMessage()));
            }
        }
    }

    private static Map<String, String> validateLoopPairs(Map<String, FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts, List<CompilationIssue> issues) {
        Map<String, String> pairs = new TreeMap<>();
        for (FlowNode node : nodes.values()) {
            if (type(node, contracts) != ComponentType.LOOP) {
                continue;
            }
            String role = text(node.config().get("role"));
            String pairId = text(node.config().get("pairNodeId"));
            FlowNode pair = pairId == null ? null : nodes.get(pairId);
            String expectedRole = "START".equals(role) ? "END" : "END".equals(role) ? "START" : null;
            if (expectedRole == null || pair == null || type(pair, contracts) != ComponentType.LOOP
                    || !expectedRole.equals(text(pair.config().get("role")))
                    || !node.id().equals(text(pair.config().get("pairNodeId")))) {
                issues.add(issue("FLOW_LOOP_PAIR_INVALID", node.id(), "config.pairNodeId",
                        "LOOP START/END 必须使用 pairNodeId 互相配对"));
            } else {
                pairs.put(node.id(), pair.id());
            }
            if ("START".equals(role)) {
                Object maximum = node.config().get("maxIterations");
                if (!(maximum instanceof Number number) || number.doubleValue() < 1
                        || number.doubleValue() > 10000 || number.doubleValue() != Math.rint(number.doubleValue())) {
                    issues.add(issue("FLOW_LOOP_MAX_ITERATIONS_INVALID", node.id(), "config.maxIterations",
                            "LOOP 最大循环次数必须是 1-10000 的整数"));
                }
            }
        }
        return pairs;
    }

    private static Graph inspectGraph(Map<String, FlowNode> nodes, List<FlowEdge> edges,
            Map<String, CompiledComponentContract> contracts, Map<String, String> loopPairs,
            List<CompilationIssue> issues) {
        Map<String, List<String>> rawForward = initializedListMap(nodes.keySet());
        Map<String, List<String>> rawReverse = initializedListMap(nodes.keySet());
        Map<String, List<String>> controlForward = initializedListMap(nodes.keySet());
        Map<String, List<String>> controlReverse = initializedListMap(nodes.keySet());
        Map<String, Map<String, List<String>>> outgoingByPort = initializedNestedMap(nodes.keySet());
        Map<String, Map<String, List<String>>> incomingByPort = initializedNestedMap(nodes.keySet());
        Set<String> edgeIds = new HashSet<>();
        for (FlowEdge edge : edges) {
            if (!edgeIds.add(edge.id())) {
                issues.add(issue("FLOW_EDGE_ID_DUPLICATE", null, "edges." + edge.id() + ".id",
                        "流程边 ID 不能重复: " + edge.id()));
            }
            FlowNode source = nodes.get(edge.sourceNodeId());
            FlowNode target = nodes.get(edge.targetNodeId());
            if (source == null) {
                issues.add(issue("FLOW_EDGE_SOURCE_NOT_FOUND", null,
                        "edges." + edge.id() + ".sourceNodeId", "流程边起点节点不存在: " + edge.sourceNodeId()));
            }
            if (target == null) {
                issues.add(issue("FLOW_EDGE_TARGET_NOT_FOUND", null,
                        "edges." + edge.id() + ".targetNodeId", "流程边终点节点不存在: " + edge.targetNodeId()));
            }
            if (source == null || target == null) {
                continue;
            }
            if (!ports(source, contracts).outputs().contains(edge.sourcePort())) {
                issues.add(issue("FLOW_EDGE_SOURCE_PORT_INVALID", source.id(),
                        "edges." + edge.id() + ".sourcePort", "节点不支持源端口: " + edge.sourcePort()));
            }
            if (!ports(target, contracts).inputs().contains(edge.targetPort())) {
                issues.add(issue("FLOW_EDGE_TARGET_PORT_INVALID", target.id(),
                        "edges." + edge.id() + ".targetPort", "节点不支持目标端口: " + edge.targetPort()));
            }
            outgoingByPort.get(source.id()).computeIfAbsent(edge.sourcePort(), ignored -> new ArrayList<>())
                    .add(target.id());
            incomingByPort.get(target.id()).computeIfAbsent(edge.targetPort(), ignored -> new ArrayList<>())
                    .add(source.id());
            rawForward.get(source.id()).add(target.id());
            rawReverse.get(target.id()).add(source.id());
            if (!isLoopBack(edge, source, target, loopPairs)) {
                controlForward.get(source.id()).add(target.id());
                controlReverse.get(target.id()).add(source.id());
            }
        }
        reconnectLoopCompletions(nodes, loopPairs, outgoingByPort, controlForward, controlReverse);
        sortIndexes(rawForward);
        sortIndexes(rawReverse);
        sortIndexes(controlForward);
        sortIndexes(controlReverse);
        outgoingByPort.values().forEach(FlowCompiler::sortIndexes);
        incomingByPort.values().forEach(FlowCompiler::sortIndexes);
        if (!isAcyclic(nodes.keySet(), controlForward, controlReverse)) {
            issues.add(issue("FLOW_GRAPH_CYCLE", null, "edges", "流程存在非 LOOP 环路"));
        }
        return new Graph(rawForward, rawReverse, controlForward, controlReverse,
                outgoingByPort, incomingByPort);
    }

    private static void reconnectLoopCompletions(Map<String, FlowNode> nodes, Map<String, String> loopPairs,
            Map<String, Map<String, List<String>>> outgoingByPort,
            Map<String, List<String>> controlForward, Map<String, List<String>> controlReverse) {
        for (FlowNode loopEnd : nodes.values()) {
            if (!"END".equals(text(loopEnd.config().get("role")))) {
                continue;
            }
            String loopStartId = loopPairs.get(loopEnd.id());
            if (loopStartId == null) {
                continue;
            }
            for (String doneTarget : outgoingByPort.getOrDefault(loopStartId, Map.of())
                    .getOrDefault("DONE", List.of())) {
                if (!controlForward.get(loopEnd.id()).contains(doneTarget)) {
                    controlForward.get(loopEnd.id()).add(doneTarget);
                    controlReverse.get(doneTarget).add(loopEnd.id());
                }
            }
        }
    }

    private static void validateStartEndAndReachability(Map<String, FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts, Graph graph, List<CompilationIssue> issues) {
        List<FlowNode> starts = nodes.values().stream().filter(node -> type(node, contracts) == ComponentType.START).toList();
        List<FlowNode> ends = nodes.values().stream().filter(node -> type(node, contracts) == ComponentType.END).toList();
        if (starts.size() != 1) {
            issues.add(issue("FLOW_START_COUNT_INVALID", null, "nodes", "流程必须且只能有一个 START"));
        }
        if (ends.isEmpty()) {
            issues.add(issue("FLOW_END_COUNT_INVALID", null, "nodes", "流程至少需要一个 END"));
        }
        if (starts.size() == 1) {
            FlowNode start = starts.getFirst();
            if (!graph.rawReverse().getOrDefault(start.id(), List.of()).isEmpty()
                    || !graph.incomingByPort().getOrDefault(start.id(), Map.of()).isEmpty()) {
                issues.add(issue("FLOW_START_INCOMING_EDGE", start.id(), "edges", "START 不允许存在入边"));
            }
            Set<String> reachable = reachable(start.id(), graph.controlForward());
            for (FlowNode node : nodes.values()) {
                if (!reachable.contains(node.id())) {
                    issues.add(issue("FLOW_NODE_UNREACHABLE", node.id(), "id", "节点无法从 START 到达"));
                }
            }
        }
    }

    private static void validateBranchStructures(Map<String, FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts, Graph graph, List<CompilationIssue> issues) {
        for (FlowNode node : nodes.values()) {
            ComponentType type = type(node, contracts);
            Map<String, List<String>> outgoing = graph.outgoingByPort().getOrDefault(node.id(), Map.of());
            Map<String, List<String>> incoming = graph.incomingByPort().getOrDefault(node.id(), Map.of());
            if (type == ComponentType.IF && (size(outgoing, "TRUE") != 1 || size(outgoing, "FALSE") != 1
                    || total(outgoing) != 2)) {
                issues.add(issue("FLOW_IF_BRANCHES_INVALID", node.id(), "edges",
                        "IF 必须恰有 TRUE 和 FALSE 两条出边"));
            }
            if (type == ComponentType.PARALLEL) {
                if (size(incoming, "IN") != 1 || total(incoming) != 1) {
                    issues.add(issue("FLOW_PARALLEL_INPUT_INVALID", node.id(), "edges",
                            "PARALLEL 必须且只能有一个 IN 输入"));
                }
                if (size(outgoing, "BRANCH") < 2 || total(outgoing) != size(outgoing, "BRANCH")) {
                    issues.add(issue("FLOW_PARALLEL_BRANCH_COUNT_INVALID", node.id(), "edges",
                            "PARALLEL 至少需要两个 BRANCH 分支"));
                }
            }
        }
    }

    private static void validateLoopStructure(Map<String, FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts, Graph graph, Map<String, String> pairs,
            List<CompilationIssue> issues) {
        for (FlowNode loop : nodes.values()) {
            if (type(loop, contracts) != ComponentType.LOOP || !"START".equals(text(loop.config().get("role")))) {
                continue;
            }
            String endId = pairs.get(loop.id());
            FlowNode end = endId == null ? null : nodes.get(endId);
            if (end == null) {
                continue;
            }
            Map<String, List<String>> startIn = graph.incomingByPort().get(loop.id());
            Map<String, List<String>> startOut = graph.outgoingByPort().get(loop.id());
            Map<String, List<String>> endIn = graph.incomingByPort().get(end.id());
            Map<String, List<String>> endOut = graph.outgoingByPort().get(end.id());
            boolean portsValid = size(startIn, "ENTRY") == 1 && size(startIn, "NEXT") == 1 && total(startIn) == 2
                    && size(startOut, "BODY") == 1 && size(startOut, "DONE") == 1 && total(startOut) == 2
                    && size(endIn, "IN") >= 1 && total(endIn) == size(endIn, "IN")
                    && size(endOut, "NEXT") == 1 && total(endOut) == 1;
            if (!portsValid) {
                issues.add(issue("FLOW_LOOP_PORTS_INVALID", loop.id(), "edges",
                        "LOOP START 需要 ENTRY/NEXT 输入和 BODY/DONE 输出，LOOP END 需要 IN/NEXT"));
                continue;
            }
            String backTarget = endOut.get("NEXT").getFirst();
            boolean validBack = loop.id().equals(backTarget) && startIn.get("NEXT").contains(end.id());
            String bodyTarget = startOut.get("BODY").getFirst();
            String doneTarget = startOut.get("DONE").getFirst();
            if (!validBack || !allPathsReach(bodyTarget, end.id(), graph.controlForward(), new HashSet<>())
                    || reachable(doneTarget, graph.controlForward()).contains(end.id())) {
                issues.add(issue("FLOW_LOOP_BACK_EDGE_INVALID", loop.id(), "edges",
                        "循环体必须到达配套 LOOP END，且只允许固定 NEXT 回边"));
            }
            Set<String> bodyNodes = nodesBefore(bodyTarget, end.id(), graph.controlForward());
            boolean forbiddenBoundary = bodyNodes.stream().map(nodes::get)
                    .filter(Objects::nonNull)
                    .map(node -> type(node, contracts))
                    .anyMatch(type -> type == ComponentType.START || type == ComponentType.END);
            if (forbiddenBoundary) {
                issues.add(issue("FLOW_LOOP_BODY_NODE_INVALID", loop.id(), "edges",
                        "LOOP 循环体不能包含 START 或 END"));
            }
        }
    }

    private static Map<String, String> validateLockStructure(Map<String, FlowNode> nodes,
            Map<String, CompiledComponentContract> contracts, Graph graph, List<CompilationIssue> issues) {
        Map<String, String> pairs = new TreeMap<>();
        for (FlowNode lock : nodes.values()) {
            if (type(lock, contracts) != ComponentType.LOCK) {
                continue;
            }
            validateNonNegative(lock, "waitMillis", issues);
            validateNonNegative(lock, "leaseMillis", issues);
            String role = text(lock.config().get("role"));
            String pairId = text(lock.config().get("pairNodeId"));
            FlowNode pair = pairId == null ? null : nodes.get(pairId);
            String expectedRole = "ACQUIRE".equals(role) ? "RELEASE" : "RELEASE".equals(role) ? "ACQUIRE" : null;
            if (expectedRole == null || pair == null || type(pair, contracts) != ComponentType.LOCK
                    || !expectedRole.equals(text(pair.config().get("role")))
                    || !lock.id().equals(text(pair.config().get("pairNodeId")))) {
                issues.add(issue("FLOW_LOCK_PAIR_INVALID", lock.id(), "config.pairNodeId",
                        "LOCK ACQUIRE/RELEASE 必须互相配对"));
                continue;
            }
            pairs.put(lock.id(), pair.id());
            if ("ACQUIRE".equals(role)) {
                List<String> exits = graph.controlForward().getOrDefault(lock.id(), List.of());
                if (exits.isEmpty() || exits.stream().anyMatch(
                        next -> !allPathsReach(next, pair.id(), graph.controlForward(), new HashSet<>()))) {
                    issues.add(issue("FLOW_LOCK_RELEASE_BYPASS", lock.id(), "config.pairNodeId",
                            "临界区所有出口必须经过对应 RELEASE"));
                }
            }
        }
        return pairs;
    }

    private Map<String, Map<String, InputBinding>> parseAndValidateBindings(
            Map<String, FlowNode> nodes, Map<String, CompiledComponentContract> contracts,
            Graph graph, List<CompilationIssue> issues) {
        String startId = nodes.values().stream().filter(node -> type(node, contracts) == ComponentType.START)
                .map(FlowNode::id).findFirst().orElse(null);
        Map<String, Set<String>> dominators = startId == null
                ? Map.of() : dominators(startId, nodes.keySet(), graph.controlReverse());
        Map<String, Map<String, InputBinding>> result = new TreeMap<>();
        for (FlowNode target : nodes.values()) {
            CompiledComponentContract targetContract = contracts.get(target.id());
            if (targetContract == null || targetContract.componentType() == ComponentType.START) {
                continue;
            }
            Map<String, InputBinding> bindings = parseBindings(target, issues);
            result.put(target.id(), bindings);
            for (InputBinding binding : bindings.values()) {
                InputSlotContract targetInput = targetContract.inputSlots().get(binding.inputKey());
                String path = "config.inputBindings." + binding.inputKey();
                if (targetInput == null) {
                    issues.add(issue("FLOW_INPUT_KEY_NOT_FOUND", target.id(), path,
                            "输入槽位不存在: " + binding.inputKey()));
                    continue;
                }
                FlowNode source = nodes.get(binding.sourceNodeId());
                if (source == null) {
                    issues.add(issue("FLOW_INPUT_SOURCE_NOT_FOUND", target.id(), path,
                            "绑定来源节点不存在: " + binding.sourceNodeId()));
                    continue;
                }
                CompiledComponentContract sourceContract = contracts.get(source.id());
                OutputSlotContract output = sourceContract == null
                        ? null : sourceContract.outputSlots().get(binding.sourceOutputKey());
                if (output == null) {
                    issues.add(issue("FLOW_INPUT_SOURCE_OUTPUT_NOT_FOUND", target.id(), path,
                            "绑定来源输出不存在: " + source.id() + "." + binding.sourceOutputKey()));
                }
                if (source.id().equals(target.id())
                        || !dominators.getOrDefault(target.id(), Set.of()).contains(source.id())) {
                    issues.add(issue("FLOW_INPUT_SOURCE_NOT_UPSTREAM", target.id(), path,
                            "绑定来源不是目标节点每条执行路径可见的上游节点: " + source.id()));
                }
                if (output != null && !output.protocol().code().equals(targetInput.protocol().code())) {
                    issues.add(issue("FLOW_INPUT_PROTOCOL_MISMATCH", target.id(), path,
                            "输入协议 code 不匹配，期望 " + targetInput.protocol().code()
                                    + "，实际 " + output.protocol().code()));
                } else if (output != null && !schemaCompatibility.isAssignable(
                        output.protocol().schema(), targetInput.protocol().schema())) {
                    issues.add(issue("FLOW_INPUT_SCHEMA_MISMATCH", target.id(), path,
                            "同编码协议的来源 Schema 不能安全赋值给目标输入"));
                }
            }
            for (Map.Entry<String, InputSlotContract> input : targetContract.inputSlots().entrySet()) {
                if (input.getValue().required() && !bindings.containsKey(input.getKey())) {
                    issues.add(issue("FLOW_INPUT_REQUIRED", target.id(),
                            "config.inputBindings." + input.getKey(), "必传输入未绑定: " + input.getValue().name()));
                }
            }
        }
        return result;
    }

    private static Map<String, InputBinding> parseBindings(FlowNode node, List<CompilationIssue> issues) {
        Object raw = node.config().get("inputBindings");
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof List<?> values)) {
            issues.add(issue("FLOW_INPUT_BINDINGS_INVALID", node.id(), "config.inputBindings",
                    "inputBindings 必须是数组"));
            return Map.of();
        }
        Map<String, InputBinding> result = new LinkedHashMap<>();
        for (int index = 0; index < values.size(); index++) {
            Object value = values.get(index);
            if (!(value instanceof Map<?, ?> binding)) {
                issues.add(issue("FLOW_INPUT_BINDINGS_INVALID", node.id(), "config.inputBindings." + index,
                        "inputBindings 数组项必须是对象"));
                continue;
            }
            String inputKey = text(binding.get("inputKey"));
            String sourceNodeId = text(binding.get("sourceNodeId"));
            String sourceOutputKey = text(binding.get("sourceOutputKey"));
            if (inputKey == null || sourceNodeId == null || sourceOutputKey == null) {
                issues.add(issue("FLOW_INPUT_BINDINGS_INVALID", node.id(), "config.inputBindings." + index,
                        "输入绑定必须包含非空 inputKey、sourceNodeId 和 sourceOutputKey"));
                continue;
            }
            InputBinding parsed = new InputBinding(inputKey, sourceNodeId, sourceOutputKey);
            if (result.putIfAbsent(inputKey, parsed) != null) {
                issues.add(issue("FLOW_INPUT_BINDINGS_INVALID", node.id(),
                        "config.inputBindings." + inputKey, "inputKey 不能重复: " + inputKey));
            }
        }
        return result;
    }

    private Map<String, ExecutionPlan> resolveSubFlows(ExecutableFlowBundle parent,
            Map<String, FlowNode> nodes, Map<String, CompiledComponentContract> contracts,
            LinkedHashSet<FlowIdentity> ancestors, Map<FlowIdentity, ExecutionPlan> compiledPlans,
            List<CompilationIssue> issues) {
        LinkedHashSet<FlowIdentity> lineage = new LinkedHashSet<>(ancestors);
        lineage.add(parent.flowIdentity());
        Map<String, ExecutionPlan> result = new TreeMap<>();
        for (FlowNode node : nodes.values()) {
            if (type(node, contracts) != ComponentType.SUB_FLOW) {
                continue;
            }
            String targetFlowId = text(node.config().get("targetFlowId"));
            if (targetFlowId == null) {
                issues.add(issue("FLOW_SUB_FLOW_TARGET_INVALID", node.id(), "config.targetFlowId",
                        "SUB_FLOW 必须配置非空 targetFlowId"));
                continue;
            }
            FlowIdentity targetIdentity = new FlowIdentity(parent.flowIdentity().tenantId(), targetFlowId,
                    parent.flowIdentity().version());
            if (lineage.contains(targetIdentity)) {
                issues.add(issue("FLOW_SUB_FLOW_RECURSION", node.id(), "config.targetFlowId",
                        "SUB_FLOW 禁止直接或间接递归调用: " + targetFlowId));
                continue;
            }
            if (relatedFlowResolver == null) {
                issues.add(issue("FLOW_SUB_FLOW_TARGET_NOT_FOUND", node.id(), "config.targetFlowId",
                        "未配置同版本子流程解析器: " + targetFlowId));
                continue;
            }
            ExecutionPlan existing = compiledPlans.get(targetIdentity);
            if (existing != null) {
                result.put(node.id(), existing);
                continue;
            }
            try {
                ExecutableFlowBundle child = Objects.requireNonNull(
                        relatedFlowResolver.resolve(parent.flowIdentity(), targetFlowId),
                        "related flow resolver returned null");
                if (!child.flowIdentity().equals(targetIdentity)) {
                    issues.add(issue("FLOW_SUB_FLOW_IDENTITY_MISMATCH", node.id(), "config.targetFlowId",
                            "解析到的子流程必须与父流程同租户同版本且 flowId 精确匹配"));
                    continue;
                }
                result.put(node.id(), compile(child, lineage, compiledPlans));
            } catch (FlowCompilationException exception) {
                issues.addAll(exception.issues());
            } catch (RuntimeException exception) {
                issues.add(issue("FLOW_SUB_FLOW_TARGET_NOT_FOUND", node.id(), "config.targetFlowId",
                        "无法解析同版本子流程 " + targetFlowId + ": " + exception.getMessage()));
            }
        }
        return result;
    }

    private static NodePorts ports(FlowNode node, Map<String, CompiledComponentContract> contracts) {
        ComponentType type = type(node, contracts);
        if (type == null) {
            return new NodePorts(Set.of(), Set.of());
        }
        return switch (type) {
            case START -> new NodePorts(Set.of(), Set.of("NEXT"));
            case END -> new NodePorts(Set.of("IN"), Set.of());
            case IF -> new NodePorts(Set.of("IN"), Set.of("TRUE", "FALSE"));
            case PARALLEL -> new NodePorts(Set.of("IN"), Set.of("BRANCH"));
            case LOOP -> "START".equals(text(node.config().get("role")))
                    ? new NodePorts(Set.of("ENTRY", "NEXT"), Set.of("BODY", "DONE"))
                    : "END".equals(text(node.config().get("role")))
                            ? new NodePorts(Set.of("IN"), Set.of("NEXT"))
                            : new NodePorts(Set.of(), Set.of());
            default -> new NodePorts(Set.of("IN"), Set.of("NEXT"));
        };
    }

    private static ComponentType type(FlowNode node, Map<String, CompiledComponentContract> contracts) {
        CompiledComponentContract contract = node == null ? null : contracts.get(node.id());
        return contract == null ? null : contract.componentType();
    }

    private static boolean isLoopBack(FlowEdge edge, FlowNode source, FlowNode target,
            Map<String, String> pairs) {
        return target.id().equals(pairs.get(source.id()))
                && "END".equals(text(source.config().get("role")))
                && "START".equals(text(target.config().get("role")))
                && "NEXT".equals(edge.sourcePort()) && "NEXT".equals(edge.targetPort());
    }

    private static Map<String, List<String>> initializedListMap(Set<String> nodeIds) {
        Map<String, List<String>> result = new TreeMap<>();
        nodeIds.forEach(id -> result.put(id, new ArrayList<>()));
        return result;
    }

    private static Map<String, Map<String, List<String>>> initializedNestedMap(Set<String> nodeIds) {
        Map<String, Map<String, List<String>>> result = new TreeMap<>();
        nodeIds.forEach(id -> result.put(id, new TreeMap<>()));
        return result;
    }

    private static void sortIndexes(Map<String, List<String>> indexes) {
        indexes.values().forEach(values -> values.sort(String::compareTo));
    }

    private static boolean isAcyclic(Set<String> nodeIds, Map<String, List<String>> forward,
            Map<String, List<String>> reverse) {
        Map<String, Integer> indegrees = new HashMap<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        for (String nodeId : nodeIds) {
            int indegree = reverse.getOrDefault(nodeId, List.of()).size();
            indegrees.put(nodeId, indegree);
            if (indegree == 0) {
                pending.addLast(nodeId);
            }
        }
        int visited = 0;
        while (!pending.isEmpty()) {
            String nodeId = pending.removeFirst();
            visited++;
            for (String next : forward.getOrDefault(nodeId, List.of())) {
                int remaining = indegrees.compute(next, (ignored, value) -> value - 1);
                if (remaining == 0) {
                    pending.addLast(next);
                }
            }
        }
        return visited == nodeIds.size();
    }

    private static Set<String> reachable(String start, Map<String, List<String>> forward) {
        Set<String> result = new LinkedHashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        result.add(start);
        pending.add(start);
        while (!pending.isEmpty()) {
            for (String next : forward.getOrDefault(pending.removeFirst(), List.of())) {
                if (result.add(next)) {
                    pending.addLast(next);
                }
            }
        }
        return result;
    }

    private static boolean allPathsReach(String current, String expected,
            Map<String, List<String>> forward, Set<String> visiting) {
        if (current.equals(expected)) {
            return true;
        }
        if (!visiting.add(current)) {
            return false;
        }
        List<String> next = forward.getOrDefault(current, List.of());
        if (next.isEmpty()) {
            visiting.remove(current);
            return false;
        }
        for (String target : next) {
            if (!allPathsReach(target, expected, forward, visiting)) {
                visiting.remove(current);
                return false;
            }
        }
        visiting.remove(current);
        return true;
    }

    private static Set<String> nodesBefore(String start, String boundary, Map<String, List<String>> forward) {
        Set<String> result = new HashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        pending.add(start);
        while (!pending.isEmpty()) {
            String current = pending.removeFirst();
            if (current.equals(boundary) || !result.add(current)) {
                continue;
            }
            forward.getOrDefault(current, List.of()).forEach(pending::addLast);
        }
        return result;
    }

    private static Map<String, Set<String>> dominators(String start, Set<String> nodeIds,
            Map<String, List<String>> reverse) {
        Map<String, Set<String>> result = new TreeMap<>();
        for (String nodeId : nodeIds) {
            result.put(nodeId, nodeId.equals(start) ? new HashSet<>(Set.of(start)) : new HashSet<>(nodeIds));
        }
        boolean changed;
        do {
            changed = false;
            for (String nodeId : nodeIds) {
                if (nodeId.equals(start)) {
                    continue;
                }
                List<String> predecessors = reverse.getOrDefault(nodeId, List.of());
                Set<String> updated = predecessors.isEmpty()
                        ? new HashSet<>() : new HashSet<>(result.get(predecessors.getFirst()));
                for (int index = 1; index < predecessors.size(); index++) {
                    updated.retainAll(result.get(predecessors.get(index)));
                }
                updated.add(nodeId);
                if (!updated.equals(result.get(nodeId))) {
                    result.put(nodeId, updated);
                    changed = true;
                }
            }
        } while (changed);
        return result;
    }

    private static void validateNonNegative(FlowNode node, String field, List<CompilationIssue> issues) {
        Object value = node.config().get(field);
        if (value != null && (!(value instanceof Number number) || number.doubleValue() < 0)) {
            issues.add(issue("FLOW_LOCK_TIMEOUT_INVALID", node.id(), "config." + field,
                    "LOCK 等待和租约时间不能为负数"));
        }
    }

    private static int size(Map<String, List<String>> ports, String port) {
        return ports.getOrDefault(port, List.of()).size();
    }

    private static int total(Map<String, List<String>> ports) {
        return ports.values().stream().mapToInt(List::size).sum();
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private static CompilationIssue issue(String code, String nodeId, String path, String message) {
        return new CompilationIssue(code, nodeId, path, message);
    }

    private record NodePorts(Set<String> inputs, Set<String> outputs) {
    }

    private record Graph(
            Map<String, List<String>> rawForward,
            Map<String, List<String>> rawReverse,
            Map<String, List<String>> controlForward,
            Map<String, List<String>> controlReverse,
            Map<String, Map<String, List<String>>> outgoingByPort,
            Map<String, Map<String, List<String>>> incomingByPort) {
    }
}
