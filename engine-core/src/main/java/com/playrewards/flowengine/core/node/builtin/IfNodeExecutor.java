package com.playrewards.flowengine.core.node.builtin;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.execution.ExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.ComponentInvocation;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Selects exactly one TRUE/FALSE control port from a Boolean default binding. */
public final class IfNodeExecutor implements NodeExecutor {

    private final ScriptRuntime scriptRuntime;

    /** Creates the legacy DEFAULT-only executor used by the dependency-free registry. */
    public IfNodeExecutor() {
        this.scriptRuntime = null;
    }

    /**
     * Creates the executor with GROOVY mode enabled through the supplied sandbox runtime.
     *
     * @param scriptRuntime adapter-owned sandbox implementation
     */
    public IfNodeExecutor(ScriptRuntime scriptRuntime) {
        this.scriptRuntime = Objects.requireNonNull(scriptRuntime, "script runtime must not be null");
    }

    @Override
    public ComponentType supports() {
        return ComponentType.IF;
    }

    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        String mode = text(context.node().node().config().get("conditionMode"));
        if ("GROOVY".equals(mode)) {
            if (scriptRuntime == null) {
                throw new NodeExecutionException("NODE_MODE_UNSUPPORTED",
                        "IF GROOVY requires an explicitly configured ScriptRuntime");
            }
            return new NodeInvocation(ScriptNodeExecutor.executeScript(context, scriptRuntime)
                    .thenApply(IfNodeExecutor::groovyResult), ComponentInvocation.emptyContent());
        }
        if (!"DEFAULT".equals(mode)) {
            throw new NodeExecutionException("IF_CONFIG_INVALID", "IF conditionMode must be DEFAULT or GROOVY");
        }
        Object rawBinding = context.node().node().config().get("conditionBinding");
        if (!(rawBinding instanceof Map<?, ?> binding)) {
            throw new NodeExecutionException("IF_CONFIG_INVALID", "IF DEFAULT requires conditionBinding");
        }
        String sourceNodeId = text(binding.get("sourceNodeId"));
        String sourceOutputKey = text(binding.get("sourceOutputKey"));
        if (sourceNodeId == null || sourceOutputKey == null) {
            throw new NodeExecutionException("IF_CONFIG_INVALID",
                    "conditionBinding requires sourceNodeId and sourceOutputKey");
        }
        ExecutionContext.ContextValue condition = context.lookupSource(sourceNodeId, sourceOutputKey);
        if (!condition.present()) {
            throw new NodeExecutionException("IF_CONDITION_MISSING", "IF condition source has no value");
        }
        if (!(condition.value() instanceof Boolean selected)) {
            throw new NodeExecutionException("IF_CONDITION_NOT_BOOLEAN", "IF condition must be Boolean");
        }
        return NodeInvocation.completed(NodeResult.continueWith(Map.of(),
                Set.of(selected ? "TRUE" : "FALSE")));
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private static NodeResult groovyResult(Object value) {
        if (!(value instanceof Boolean selected)) {
            throw new NodeExecutionException("IF_CONDITION_NOT_BOOLEAN", "IF condition must be Boolean");
        }
        return NodeResult.continueWith(Map.of(), Set.of(selected ? "TRUE" : "FALSE"));
    }
}
