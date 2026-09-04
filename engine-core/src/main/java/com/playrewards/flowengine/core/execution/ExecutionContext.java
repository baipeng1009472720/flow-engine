package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-execution append-only value store addressed by node code, scope path and output key.
 *
 * <p>The store accepts JSON {@code null} as a present value and uses an atomic single-write rule, so
 * concurrent branches cannot overwrite each other. No map is shared between executions.</p>
 */
public final class ExecutionContext {

    private final Map<String, Map<String, Object>> externalScopes;
    private final ConcurrentHashMap<OutputAddress, StoredValue> outputs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ChildExecution> childExecutions = new ConcurrentHashMap<>();

    /**
     * Creates an isolated execution context.
     *
     * @param externalScopes immutable-by-copy host namespaces for this execution
     */
    public ExecutionContext(Map<String, Map<String, Object>> externalScopes) {
        Objects.requireNonNull(externalScopes, "external scopes must not be null");
        Map<String, Map<String, Object>> copied = new java.util.LinkedHashMap<>();
        externalScopes.forEach((key, value) -> copied.put(
                JsonValues.requireText(key, "external scope key"),
                JsonValues.copyObject(value, "external scope " + key)));
        this.externalScopes = Map.copyOf(copied);
    }

    /** @return immutable external namespaces captured for this execution */
    public Map<String, Map<String, Object>> externalScopes() {
        return externalScopes;
    }

    /**
     * Writes one JSON output exactly once at its full logical address.
     *
     * @param nodeCode stable flow-local node code
     * @param scopePath branch/loop/child scope
     * @param outputKey stable component output slot key
     * @param value JSON-compatible value, including {@code null}
     * @throws IllegalStateException when the same address was already written
     */
    public void writeOnce(String nodeCode, ScopePath scopePath, String outputKey, Object value) {
        OutputAddress address = address(nodeCode, scopePath, outputKey);
        StoredValue stored = new StoredValue(JsonValues.copyValue(value, "node output " + nodeCode + "." + outputKey));
        if (outputs.putIfAbsent(address, stored) != null) {
            throw new IllegalStateException("node output may only be written once: " + address);
        }
    }

    /**
     * Looks up an addressed output while preserving the distinction between missing and JSON null.
     *
     * @return a present/missing value wrapper
     */
    public ContextValue lookup(String nodeCode, ScopePath scopePath, String outputKey) {
        StoredValue value = outputs.get(address(nodeCode, scopePath, outputKey));
        return value == null ? ContextValue.missing() : ContextValue.present(value.value());
    }

    /**
     * Resolves an exact value, the closest ancestor frame, or one unique descendant branch value.
     * Ambiguous descendants at a natural join fail closed instead of depending on completion order.
     */
    public ContextValue lookupVisible(String nodeCode, ScopePath scopePath, String outputKey) {
        String normalizedNode = JsonValues.requireText(nodeCode, "node code");
        String normalizedOutput = JsonValues.requireText(outputKey, "output key");
        Objects.requireNonNull(scopePath, "scope path must not be null");
        List<Map.Entry<OutputAddress, StoredValue>> ancestors = outputs.entrySet().stream()
                .filter(entry -> entry.getKey().nodeCode().equals(normalizedNode)
                        && entry.getKey().outputKey().equals(normalizedOutput)
                        && scopePath.isWithin(entry.getKey().scopePath()))
                .sorted(Comparator.comparingInt((Map.Entry<OutputAddress, StoredValue> entry) ->
                        entry.getKey().scopePath().value().length()).reversed())
                .toList();
        if (!ancestors.isEmpty()) {
            return ContextValue.present(ancestors.getFirst().getValue().value());
        }
        List<Map.Entry<OutputAddress, StoredValue>> descendants = outputs.entrySet().stream()
                .filter(entry -> entry.getKey().nodeCode().equals(normalizedNode)
                        && entry.getKey().outputKey().equals(normalizedOutput)
                        && entry.getKey().scopePath().isWithin(scopePath))
                .sorted(Comparator.comparing(entry -> entry.getKey().scopePath().value()))
                .toList();
        if (descendants.isEmpty()) {
            return ContextValue.missing();
        }
        if (descendants.size() != 1) {
            throw new IllegalArgumentException("node output is ambiguous across structured scopes: "
                    + normalizedNode + "." + normalizedOutput);
        }
        return ContextValue.present(descendants.getFirst().getValue().value());
    }

    /**
     * Returns an addressed output, including JSON null, or fails when no write exists.
     *
     * @throws IllegalArgumentException when the address has not been written
     */
    public Object required(String nodeCode, ScopePath scopePath, String outputKey) {
        ContextValue value = lookup(nodeCode, scopePath, outputKey);
        if (!value.present()) {
            throw new IllegalArgumentException("missing node output: " + nodeCode + "." + outputKey);
        }
        return value.value();
    }

    /**
     * Resolves stable {@code ctx.<nodeCode>.<outputKey>} or single-output VARIABLE paths.
     *
     * @param path external context path
     * @param scopePath scope in which to resolve it
     * @return addressed JSON value, including null
     */
    public Object requiredPath(String path, ScopePath scopePath) {
        Objects.requireNonNull(path, "context path must not be null");
        String[] segments = path.split("\\.", -1);
        if (segments.length == 3 && "ctx".equals(segments[0])) {
            return required(segments[1], scopePath, segments[2]);
        }
        if (segments.length == 2 && "ctx".equals(segments[0])) {
            List<Map.Entry<OutputAddress, StoredValue>> matches = new ArrayList<>();
            outputs.entrySet().stream()
                    .filter(entry -> entry.getKey().nodeCode().equals(segments[1])
                            && entry.getKey().scopePath().equals(scopePath))
                    .sorted(Comparator.comparing(entry -> entry.getKey().outputKey()))
                    .forEach(matches::add);
            if (matches.size() != 1) {
                throw new IllegalArgumentException("single-output context path is not uniquely resolved: " + path);
            }
            return matches.getFirst().getValue().value();
        }
        throw new IllegalArgumentException("unsupported context path: " + path);
    }

    /**
     * Returns a deeply immutable snapshot of outputs visible in one execution scope.
     *
     * <p>The snapshot is grouped by stable node code and output key. It is used only to create
     * adapter-neutral read views; callers cannot mutate the append-only store.</p>
     *
     * @param scopePath exact execution scope to project
     * @param visibleNodeCodes statically visible ancestor node codes
     * @return deeply immutable, deterministically ordered node-output projection
     */
    public Map<String, Map<String, Object>> outputsByNode(
            ScopePath scopePath, Set<String> visibleNodeCodes) {
        Objects.requireNonNull(scopePath, "scope path must not be null");
        Objects.requireNonNull(visibleNodeCodes, "visible node codes must not be null");
        Map<String, Map<String, Object>> grouped = new TreeMap<>();
        outputs.keySet().stream()
                .filter(address -> visibleNodeCodes.contains(address.nodeCode()))
                .map(address -> Map.entry(address.nodeCode(), address.outputKey()))
                .distinct()
                .sorted(Map.Entry.<String, String>comparingByKey().thenComparing(Map.Entry::getValue))
                .forEach(address -> {
                    try {
                        ContextValue visible = lookupVisible(address.getKey(), scopePath, address.getValue());
                        if (visible.present()) {
                            grouped.computeIfAbsent(address.getKey(), ignored -> new TreeMap<>())
                                    .put(address.getValue(), visible.value());
                        }
                    } catch (IllegalArgumentException ignored) {
                        // Multiple completed loop frames are intentionally not projected into a parent ctx view.
                    }
                });
        Map<String, Map<String, Object>> snapshot = new java.util.LinkedHashMap<>();
        grouped.forEach((nodeCode, values) -> snapshot.put(
                nodeCode, JsonValues.copyObject(values, "ctx." + nodeCode)));
        return java.util.Collections.unmodifiableMap(snapshot);
    }

    /** Records one independently owned child-flow context outside the business ctx namespace. */
    public void registerChildExecution(String childExecutionId, ScopePath scopePath, ExecutionContext context) {
        String normalized = JsonValues.requireText(childExecutionId, "child execution id");
        ChildExecution child = new ChildExecution(normalized,
                Objects.requireNonNull(scopePath, "child scope must not be null"),
                Objects.requireNonNull(context, "child context must not be null"));
        if (childExecutions.putIfAbsent(normalized, child) != null) {
            throw new IllegalStateException("child execution id may only be registered once");
        }
    }

    /** @return immutable stable child-flow diagnostic index */
    public Map<String, ChildExecution> childExecutions() {
        return java.util.Collections.unmodifiableMap(new TreeMap<>(childExecutions));
    }

    private static OutputAddress address(String nodeCode, ScopePath scopePath, String outputKey) {
        return new OutputAddress(JsonValues.requireText(nodeCode, "node code"),
                Objects.requireNonNull(scopePath, "scope path must not be null"),
                JsonValues.requireText(outputKey, "output key"));
    }

    /** Present/missing output wrapper which can carry a present JSON null. */
    public record ContextValue(boolean present, Object value) {

        private static ContextValue missing() {
            return new ContextValue(false, null);
        }

        private static ContextValue present(Object value) {
            return new ContextValue(true, value);
        }
    }

    private record OutputAddress(String nodeCode, ScopePath scopePath, String outputKey) {
    }

    private record StoredValue(Object value) {
    }

    /** Independently owned child-flow context and its stable scope identity. */
    public record ChildExecution(String childExecutionId, ScopePath scopePath, ExecutionContext context) {
    }
}
