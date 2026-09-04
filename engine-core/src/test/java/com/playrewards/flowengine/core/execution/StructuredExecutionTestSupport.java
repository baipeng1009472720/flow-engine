package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
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
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.CompiledNode.InputBinding;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.lang.reflect.Constructor;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Shared real-engine fixtures for Task 7 structural execution tests. */
final class StructuredExecutionTestSupport {

    static final ProtocolSchema ANY = new ProtocolSchema("any", "1.0", Map.of());
    static final ProtocolSchema ARRAY = new ProtocolSchema("array", "1.0", Map.of("type", "array"));
    static final ProtocolSchema INTEGER = new ProtocolSchema("integer", "1.0", Map.of("type", "integer"));
    static final ComponentCapabilities PURE =
            new ComponentCapabilities(ComponentEffect.PURE, true, false, true, 64);

    private StructuredExecutionTestSupport() {
    }

    static CompiledNode node(String id, ComponentType type, Map<String, Object> config,
            Map<String, InputSlotContract> inputs, Map<String, OutputSlotContract> outputs,
            Map<String, InputBinding> bindings) {
        FlowNode node = new FlowNode(id, "component." + id, id, id, config);
        return new CompiledNode(node, new CompiledComponentContract(
                node.componentCode(), type, null, Map.of(), inputs, outputs, PURE), bindings);
    }

    static CompiledNode node(String id, ComponentType type, Map<String, Object> config,
            Map<String, OutputSlotContract> outputs) {
        return node(id, type, config, Map.of(), outputs, Map.of());
    }

    static InputSlotContract requiredInput(String key, ProtocolSchema protocol) {
        return new InputSlotContract(key, true, protocol);
    }

    static OutputSlotContract output(String key, ProtocolSchema protocol) {
        return new OutputSlotContract(key, protocol);
    }

    static InputBinding binding(String inputKey, String sourceNodeId, String sourceOutputKey) {
        return new InputBinding(inputKey, sourceNodeId, sourceOutputKey);
    }

    static Edge edge(String source, String target, String sourcePort, String targetPort) {
        return new Edge(source, target, sourcePort, targetPort);
    }

    static ExecutionPlan plan(String flowId, Collection<CompiledNode> nodes, Collection<Edge> edges,
            Map<String, String> loopPairs, Map<String, String> lockPairs,
            Map<String, ExecutionPlan> subFlows) {
        Map<String, CompiledNode> indexed = new TreeMap<>();
        nodes.forEach(node -> indexed.put(node.node().id(), node));
        Map<String, List<String>> forward = listIndex(indexed.keySet());
        Map<String, List<String>> reverse = listIndex(indexed.keySet());
        Map<String, Map<String, List<String>>> outgoing = nestedIndex(indexed.keySet());
        Map<String, Map<String, List<String>>> incoming = nestedIndex(indexed.keySet());
        for (Edge edge : edges) {
            forward.get(edge.source()).add(edge.target());
            reverse.get(edge.target()).add(edge.source());
            outgoing.get(edge.source()).computeIfAbsent(edge.sourcePort(), ignored -> new ArrayList<>())
                    .add(edge.target());
            incoming.get(edge.target()).computeIfAbsent(edge.targetPort(), ignored -> new ArrayList<>())
                    .add(edge.source());
        }
        forward.values().forEach(values -> values.sort(String::compareTo));
        reverse.values().forEach(values -> values.sort(String::compareTo));
        outgoing.values().forEach(ports -> ports.values().forEach(values -> values.sort(String::compareTo)));
        incoming.values().forEach(ports -> ports.values().forEach(values -> values.sort(String::compareTo)));
        return new ExecutionPlan(new FlowIdentity("tenant", flowId, "v1"), "hash-" + flowId,
                indexed, forward, reverse, outgoing, incoming, reverse, loopPairs, lockPairs, subFlows);
    }

    static ExecutionRequest request(Map<String, Object> startInputs) {
        return request(startInputs, Map.of(), Instant.now().plus(5, ChronoUnit.SECONDS));
    }

    static ExecutionRequest request(Map<String, Object> startInputs,
            Map<String, Map<String, Object>> externalScopes, Instant deadline) {
        return new ExecutionRequest("inline:test", ExecutionMode.EPHEMERAL, ExecutionPurpose.TRIAL,
                startInputs, externalScopes, null, "request-key", "tenant", deadline, Map.of());
    }

    static ExecutionResult await(ExecutionHandle handle) throws Exception {
        return handle.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    static ScriptRuntime scripts(Function<ScriptRuntime.ScriptInvocation, Object> evaluator) {
        return invocation -> CompletableFuture.completedFuture(evaluator.apply(invocation));
    }

    static NodeExecutorRegistry registry(NodeExecutor scriptExecutor, ScriptRuntime scripts,
            DistributedLockProvider locks) {
        List<NodeExecutor> structural = new ArrayList<>();
        structural.add(instantiate("com.playrewards.flowengine.core.node.structure.ParallelCoordinator",
                new Class<?>[0]));
        structural.add(instantiate("com.playrewards.flowengine.core.node.structure.LoopCoordinator",
                new Class<?>[]{ScriptRuntime.class}, scripts));
        structural.add(instantiate("com.playrewards.flowengine.core.node.structure.SubFlowNodeExecutor",
                new Class<?>[0]));
        structural.add(instantiate("com.playrewards.flowengine.core.node.structure.LockNodeExecutor",
                new Class<?>[]{ScriptRuntime.class, DistributedLockProvider.class, Clock.class},
                scripts, locks, Clock.systemUTC()));
        if (scriptExecutor != null) {
            structural.add(scriptExecutor);
        }
        return NodeExecutorRegistry.withBuiltIns(structural);
    }

    @SuppressWarnings("unchecked")
    private static <T> T instantiate(String className, Class<?>[] parameterTypes, Object... arguments) {
        try {
            Class<?> type = Class.forName(className);
            Constructor<?> constructor = type.getConstructor(parameterTypes);
            return (T) constructor.newInstance(arguments);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("Task 7 structural executor is not implemented: " + className, failure);
        }
    }

    private static Map<String, List<String>> listIndex(Collection<String> ids) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        ids.forEach(id -> result.put(id, new ArrayList<>()));
        return result;
    }

    private static Map<String, Map<String, List<String>>> nestedIndex(Collection<String> ids) {
        Map<String, Map<String, List<String>>> result = new LinkedHashMap<>();
        ids.forEach(id -> result.put(id, new LinkedHashMap<>()));
        return result;
    }

    record Edge(String source, String target, String sourcePort, String targetPort) {
    }
}
