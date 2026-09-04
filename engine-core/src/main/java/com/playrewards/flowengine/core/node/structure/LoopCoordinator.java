package com.playrewards.flowengine.core.node.structure;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.core.node.JsonValueValidator;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.core.node.ScriptExecutionSupport;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Parses LOOP configuration and evaluates its bounded collection without owning graph workers. */
public final class LoopCoordinator implements NodeExecutor {

    private final ScriptRuntime scripts;

    /** @param scripts sandbox used only by {@code collectionMode=GROOVY} */
    public LoopCoordinator(ScriptRuntime scripts) {
        this.scripts = Objects.requireNonNull(scripts, "script runtime must not be null");
    }

    @Override
    public ComponentType supports() {
        return ComponentType.LOOP;
    }

    /** LOOP scheduling is a scheduler primitive and cannot be invoked as an isolated leaf node. */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        throw new NodeExecutionException("LOOP_COORDINATOR_REQUIRED",
                "LOOP must execute through the scoped scheduler coordinator");
    }

    /**
     * Evaluates and validates the source array before any iteration is scheduled.
     *
     * @param context LOOP START attempt
     * @return asynchronous immutable collection in source order
     */
    public CollectionEvaluation collection(NodeExecutionContext context) {
        context.throwIfStopped();
        String mode = text(context.node().node().config().get("collectionMode"));
        CompletionStage<Object> source;
        CompletionStage<Object> sourceCompletion;
        if ("UPSTREAM".equals(mode)) {
            Source binding = source(context.node().node().config().get("collectionBinding"),
                    "LOOP_COLLECTION_BINDING_INVALID");
            var value = context.lookupSource(binding.nodeId(), binding.outputKey());
            if (!value.present()) {
                throw new NodeExecutionException("LOOP_COLLECTION_MISSING", "LOOP collection source is missing");
            }
            source = CompletableFuture.completedFuture(value.value());
            sourceCompletion = source;
        } else if ("GROOVY".equals(mode)) {
            String expression = text(context.node().node().config().get("collectionExpression"));
            if (expression == null) {
                throw new NodeExecutionException("LOOP_COLLECTION_CONFIG_INVALID",
                        "LOOP GROOVY collectionExpression must be non-blank");
            }
            ScriptExecutionSupport.ScriptExecution script = ScriptExecutionSupport.start(
                    () -> scripts.execute(new ScriptRuntime.ScriptInvocation(expression,
                            context.externalScopes(), context.ctxView(), context.executionLogger(),
                            context.cancellationToken(), context.request().deadline())));
            source = script.completion();
            sourceCompletion = script.sourceCompletion();
        } else {
            throw new NodeExecutionException("LOOP_COLLECTION_CONFIG_INVALID",
                    "LOOP collectionMode must be UPSTREAM or GROOVY");
        }
        return new CollectionEvaluation(
                source.thenApply(value -> validateCollection(context, value)), sourceCompletion);
    }

    /** @return required positive configured maximum */
    public int maxIterations(NodeExecutionContext context) {
        return configuredMaximum(context.node().node().config().get("maxIterations"));
    }

    /** @return stable item alias used only inside each iteration frame */
    public String itemAlias(NodeExecutionContext context) {
        return requiredAlias(context, "itemAlias");
    }

    /** @return stable index alias used only inside each iteration frame */
    public String indexAlias(NodeExecutionContext context) {
        String index = requiredAlias(context, "indexAlias");
        if (index.equals(itemAlias(context))) {
            throw new NodeExecutionException("LOOP_CONFIG_INVALID", "LOOP aliases must be different");
        }
        return index;
    }

    /** @return optional iteration result source */
    public Source resultBinding(NodeExecutionContext context) {
        Object raw = context.node().node().config().get("resultBinding");
        return raw == null ? null : source(raw, "LOOP_RESULT_BINDING_INVALID");
    }

    /** Creates the validated parent-scope aggregate while preserving source index ordering. */
    public NodeResult completed(List<Object> results) {
        return NodeResult.continueWith(Map.of("count", results.size(), "results", results), Set.of("DONE"));
    }

    private static List<Object> validateCollection(NodeExecutionContext context, Object raw) {
        if (!(raw instanceof List<?> values)) {
            throw new NodeExecutionException("LOOP_COLLECTION_INVALID", "LOOP collection must be a JSON array");
        }
        int maximum = configuredMaximum(context.node().node().config().get("maxIterations"));
        if (values.size() > maximum) {
            throw new NodeExecutionException("LOOP_MAX_ITERATIONS_EXCEEDED",
                    "LOOP collection exceeds maxIterations");
        }
        ProtocolSchema itemProtocol = itemProtocol(context.node().node().config().get("itemProtocol"));
        List<Object> copy = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Object value = values.get(index);
            if (itemProtocol != null) {
                JsonValueValidator.validate(value, itemProtocol, "LOOP_ITEM_INVALID", "collection." + index);
            }
            copy.add(JsonValues.copyValue(value, "LOOP collection item"));
        }
        return java.util.Collections.unmodifiableList(copy);
    }

    private static ProtocolSchema itemProtocol(Object raw) {
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof Map<?, ?> value) || !(value.get("schema") instanceof Map<?, ?> schema)) {
            throw new NodeExecutionException("LOOP_ITEM_PROTOCOL_INVALID",
                    "LOOP itemProtocol must contain code, version and schema");
        }
        String code = text(value.get("code"));
        if (code == null) {
            code = text(value.get("protocolCode"));
        }
        if (code == null) {
            throw new NodeExecutionException("LOOP_ITEM_PROTOCOL_INVALID",
                    "LOOP itemProtocol code must be non-blank");
        }
        String version = text(value.get("version"));
        if (version == null) {
            version = "1.0";
        }
        Map<String, Object> schemaCopy = new java.util.LinkedHashMap<>();
        schema.forEach((key, item) -> schemaCopy.put(String.valueOf(key), item));
        return new ProtocolSchema(code, version, schemaCopy);
    }

    private static String requiredAlias(NodeExecutionContext context, String field) {
        String value = text(context.node().node().config().get(field));
        if (value == null) {
            throw new NodeExecutionException("LOOP_CONFIG_INVALID", "LOOP " + field + " must be non-blank");
        }
        return value;
    }

    private static Source source(Object raw, String code) {
        if (!(raw instanceof Map<?, ?> value)) {
            throw new NodeExecutionException(code, "LOOP source binding must be an object");
        }
        String nodeId = text(value.get("sourceNodeId"));
        String outputKey = text(value.get("sourceOutputKey"));
        if (nodeId == null || outputKey == null) {
            throw new NodeExecutionException(code, "LOOP source binding requires node and output");
        }
        return new Source(nodeId, outputKey);
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private static int configuredMaximum(Object value) {
        if (!(value instanceof Number number) || number.longValue() < 1 || number.longValue() > 10_000
                || number.doubleValue() != Math.rint(number.doubleValue())) {
            throw new NodeExecutionException("LOOP_CONFIG_INVALID", "LOOP maxIterations must be 1-10000");
        }
        return number.intValue();
    }

    /** Stable configured source binding. */
    public record Source(String nodeId, String outputKey) {
    }

    /**
     * Keeps the derived validated value separate from the adapter's actual source lifecycle.
     * Cancelling a dependent stage is not proof that a non-Future source has released its resources.
     */
    public record CollectionEvaluation(
            CompletionStage<List<Object>> completion, CompletionStage<?> sourceCompletion) {

        public CollectionEvaluation {
            Objects.requireNonNull(completion, "collection completion must not be null");
            Objects.requireNonNull(sourceCompletion, "collection source completion must not be null");
        }
    }

}
