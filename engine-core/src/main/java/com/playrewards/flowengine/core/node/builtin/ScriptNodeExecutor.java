package com.playrewards.flowengine.core.node.builtin;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.core.protocol.OutputValidator;
import com.playrewards.flowengine.spi.ComponentInvocation;
import com.playrewards.flowengine.spi.ScriptRuntime;
import com.playrewards.flowengine.spi.ScriptRuntime.ScriptExecutionException;
import com.playrewards.flowengine.spi.ScriptRuntime.ScriptInvocation;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/** Runs a SCRIPT node through the injected sandbox and validates its single dynamic output. */
public final class ScriptNodeExecutor implements NodeExecutor {

    private final ScriptRuntime runtime;
    private final OutputValidator outputValidator;

    /**
     * Creates the SCRIPT executor from adapter-neutral dependencies.
     *
     * @param runtime adapter-owned sandbox implementation
     * @param outputValidator shared declared-output validator
     */
    public ScriptNodeExecutor(ScriptRuntime runtime, OutputValidator outputValidator) {
        this.runtime = Objects.requireNonNull(runtime, "script runtime must not be null");
        this.outputValidator = Objects.requireNonNull(outputValidator, "output validator must not be null");
    }

    @Override
    public ComponentType supports() {
        return ComponentType.SCRIPT;
    }

    /** Executes the configured Groovy source and publishes its validated {@code result} slot. */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        if (context.node().componentContract().outputSlots().size() != 1) {
            throw new NodeExecutionException("SCRIPT_OUTPUT_CONTRACT_INVALID",
                    "SCRIPT must declare exactly one output slot");
        }
        String outputKey = context.node().componentContract().outputSlots().keySet().iterator().next();
        CompletionStage<NodeResult> completion = executeScript(context, runtime).thenApply(value -> {
            Map<String, Object> outputs = new LinkedHashMap<>();
            outputs.put(outputKey, value);
            return outputValidator.validate(context.node(), outputs);
        });
        return new NodeInvocation(completion, ComponentInvocation.emptyContent());
    }

    /** Starts the shared sandbox invocation used by SCRIPT, IF GROOVY and VARIABLE GROOVY. */
    static CompletionStage<Object> executeScript(NodeExecutionContext context, ScriptRuntime runtime) {
        context.throwIfStopped();
        String script = text(context.node().node().config().get("groovyScript"));
        if (script == null) {
            throw new NodeExecutionException("SCRIPT_CONFIG_INVALID", "groovyScript must be non-blank");
        }
        CompletionStage<Object> execution;
        try {
            execution = Objects.requireNonNull(runtime.execute(new ScriptInvocation(
                    script,
                    context.externalScopes(),
                    context.ctxView(),
                    context.executionLogger(),
                    context.cancellationToken(),
                    context.request().deadline())), "script completion must not be null");
        } catch (ScriptExecutionException failure) {
            throw scriptFailure(failure);
        }
        return execution.handle((value, failure) -> {
            if (failure == null) {
                return value;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof CancellationException cancellation) {
                throw cancellation;
            }
            if (cause instanceof ScriptExecutionException scriptFailure) {
                throw scriptFailure(scriptFailure);
            }
            throw new NodeExecutionException("SCRIPT_EXECUTION_FAILED", "script execution failed");
        });
    }

    private static NodeExecutionException scriptFailure(ScriptExecutionException failure) {
        return new NodeExecutionException(failure.code(), failure.getMessage());
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text : null;
    }
}
