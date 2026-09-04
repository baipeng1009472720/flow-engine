package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.node.builtin.EndNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.BusinessNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.IfNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.ScriptNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.StartNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.VariableNodeExecutor;
import com.playrewards.flowengine.core.node.structure.LockNodeExecutor;
import com.playrewards.flowengine.core.node.structure.LoopCoordinator;
import com.playrewards.flowengine.core.node.structure.ParallelCoordinator;
import com.playrewards.flowengine.core.node.structure.SubFlowNodeExecutor;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.protocol.InputBindingResolver;
import com.playrewards.flowengine.core.protocol.OutputValidator;
import com.playrewards.flowengine.spi.ScriptRuntime;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import java.time.Clock;
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
     * Creates the complete Task 6 registry with one shared script runtime and BUSINESS registry.
     *
     * <p>Hosts must use this explicit factory for real runtime assembly. The dependency-free
     * {@link #builtIns()} remains available only for START/END and non-Groovy Task 5 embedding.</p>
     *
     * @param scriptRuntime adapter-owned bounded script runtime
     * @param businessComponents immutable startup business registry
     * @return complete START/END/IF/VARIABLE/SCRIPT/BUSINESS executor registry
     */
    public static NodeExecutorRegistry runtimeBuiltIns(ScriptRuntime scriptRuntime,
            FlowBusinessComponentRegistry businessComponents) {
        Objects.requireNonNull(scriptRuntime, "script runtime must not be null");
        Objects.requireNonNull(businessComponents, "business components must not be null");
        OutputValidator outputValidator = new OutputValidator();
        return new NodeExecutorRegistry(List.of(
                new StartNodeExecutor(),
                new EndNodeExecutor(),
                new IfNodeExecutor(scriptRuntime),
                new VariableNodeExecutor(scriptRuntime, outputValidator),
                new ScriptNodeExecutor(scriptRuntime, outputValidator),
                new ParallelCoordinator(),
                new LoopCoordinator(scriptRuntime),
                new SubFlowNodeExecutor(),
                new BusinessNodeExecutor(businessComponents, new InputBindingResolver(), outputValidator)));
    }

    /**
     * Creates the complete Task 7 runtime registry including scheduler-owned LOCK leases.
     *
     * @param scriptRuntime adapter-owned bounded script runtime
     * @param businessComponents immutable startup business registry
     * @param lockProvider tenant-scoped lease provider
     * @param clock shared deadline and lease clock
     * @return complete structural and Task 6 executor registry
     */
    public static NodeExecutorRegistry runtimeBuiltIns(ScriptRuntime scriptRuntime,
            FlowBusinessComponentRegistry businessComponents,
            DistributedLockProvider lockProvider, Clock clock) {
        Objects.requireNonNull(scriptRuntime, "script runtime must not be null");
        Objects.requireNonNull(businessComponents, "business components must not be null");
        Objects.requireNonNull(lockProvider, "lock provider must not be null");
        Objects.requireNonNull(clock, "clock must not be null");
        OutputValidator outputValidator = new OutputValidator();
        return new NodeExecutorRegistry(List.of(
                new StartNodeExecutor(),
                new EndNodeExecutor(),
                new IfNodeExecutor(scriptRuntime),
                new VariableNodeExecutor(scriptRuntime, outputValidator),
                new ScriptNodeExecutor(scriptRuntime, outputValidator),
                new ParallelCoordinator(),
                new LoopCoordinator(scriptRuntime),
                new SubFlowNodeExecutor(),
                new LockNodeExecutor(scriptRuntime, lockProvider, clock),
                new BusinessNodeExecutor(businessComponents, new InputBindingResolver(), outputValidator)));
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
