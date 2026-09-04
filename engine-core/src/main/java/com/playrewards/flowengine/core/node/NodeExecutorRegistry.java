package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.node.builtin.EndNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.IfNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.StartNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.VariableNodeExecutor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable one-executor-per-component-type registry used by the scheduler hot path. */
public final class NodeExecutorRegistry {

    private final Map<ComponentType, NodeExecutor> executors;

    /**
     * Builds a registry and rejects ambiguous component-type ownership.
     *
     * @param executors startup-supplied executors
     */
    public NodeExecutorRegistry(Collection<? extends NodeExecutor> executors) {
        Objects.requireNonNull(executors, "node executors must not be null");
        Map<ComponentType, NodeExecutor> indexed = new EnumMap<>(ComponentType.class);
        for (NodeExecutor executor : executors) {
            Objects.requireNonNull(executor, "node executor must not be null");
            ComponentType type = Objects.requireNonNull(
                    executor.supports(), "supported component type must not be null");
            if (indexed.putIfAbsent(type, executor) != null) {
                throw new IllegalArgumentException("duplicate node executor for component type: " + type);
            }
        }
        this.executors = Map.copyOf(indexed);
    }

    /**
     * Creates the Task 5 built-in registry and appends explicitly supplied future-node executors.
     *
     * @param additional executors for component types implemented by later tasks or tests
     */
    public static NodeExecutorRegistry withBuiltIns(List<? extends NodeExecutor> additional) {
        List<NodeExecutor> all = new ArrayList<>();
        all.add(new StartNodeExecutor());
        all.add(new EndNodeExecutor());
        all.add(new IfNodeExecutor());
        all.add(new VariableNodeExecutor());
        all.addAll(additional);
        return new NodeExecutorRegistry(all);
    }

    /** @return a registry containing only START, END, IF and VARIABLE */
    public static NodeExecutorRegistry builtIns() {
        return withBuiltIns(List.of());
    }

    /**
     * Resolves one executor or fails with the exact node/type identity.
     *
     * @param type compiled component type
     * @param nodeCode flow-local node code for diagnostics
     * @return registered executor
     */
    public NodeExecutor required(ComponentType type, String nodeCode) {
        NodeExecutor executor = executors.get(type);
        if (executor == null) {
            throw new NodeExecutionException("NODE_TYPE_UNSUPPORTED",
                    "no node executor registered for " + type + " at node " + nodeCode);
        }
        return executor;
    }
}
