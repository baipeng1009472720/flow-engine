package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.json.JsonValues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-execution append-only value store addressed by node code, scope path and output key.
 *
 * <p>The store accepts JSON {@code null} as a present value and uses an atomic single-write rule,
 * so concurrent branches cannot overwrite each other. No map is shared between executions.
 */
public final class ExecutionContext {

    private final Map<String, Map<String, Object>> externalScopes;
    private final ConcurrentHashMap<OutputAddress, StoredValue> outputs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ChildExecution> childExecutions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ScopePath, ParallelBranch> parallelBranches =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ScopePath, Set<ScopePath>> parallelConvergences =
            new ConcurrentHashMap<>();

    /**
     * Creates an isolated execution context.
     *
     * @param externalScopes immutable-by-copy host namespaces for this execution
     */
    public ExecutionContext(Map<String, Map<String, Object>> externalScopes) {
        Objects.requireNonNull(externalScopes, "external scopes must not be null");
        Map<String, Map<String, Object>> copied = new java.util.LinkedHashMap<>();
        externalScopes.forEach(
                (key, value) ->
                        copied.put(
                                JsonValues.requireText(key, "external scope key"),
                                JsonValues.copyObject(value, "external scope " + key)));
        this.externalScopes = Map.copyOf(copied);
    }

    /**
     * @return immutable external namespaces captured for this execution
     */
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
        StoredValue stored =
                new StoredValue(
                        JsonValues.copyValue(value, "node output " + nodeCode + "." + outputKey));
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
     * Resolves an exact value, the closest ancestor frame, or a value from an explicitly converged
     * parallel branch. Loop iterations and child-flow scopes are never promoted by uniqueness.
     */
    public ContextValue lookupVisible(String nodeCode, ScopePath scopePath, String outputKey) {
        String normalizedNode = JsonValues.requireText(nodeCode, "node code");
        String normalizedOutput = JsonValues.requireText(outputKey, "output key");
        Objects.requireNonNull(scopePath, "scope path must not be null");
        List<Map.Entry<OutputAddress, StoredValue>> ancestors =
                outputs.entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getKey().nodeCode().equals(normalizedNode)
                                                && entry.getKey()
                                                        .outputKey()
                                                        .equals(normalizedOutput)
                                                && scopePath.isWithin(entry.getKey().scopePath()))
                        .sorted(
                                Comparator.comparingInt(
                                                (Map.Entry<OutputAddress, StoredValue> entry) ->
                                                        entry.getKey().scopePath().value().length())
                                        .reversed())
                        .toList();
        if (!ancestors.isEmpty()) {
            return ContextValue.present(ancestors.getFirst().getValue().value());
        }
        List<Map.Entry<OutputAddress, StoredValue>> descendants =
                outputs.entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getKey().nodeCode().equals(normalizedNode)
                                                && entry.getKey()
                                                        .outputKey()
                                                        .equals(normalizedOutput)
                                                && isParallelConverged(
                                                        entry.getKey().scopePath(), scopePath))
                        .sorted(Comparator.comparing(entry -> entry.getKey().scopePath().value()))
                        .toList();
        if (descendants.isEmpty()) {
            return ContextValue.missing();
        }
        if (descendants.size() != 1) {
            throw new IllegalArgumentException(
                    "node output is ambiguous across structured scopes: "
                            + normalizedNode
                            + "."
                            + normalizedOutput);
        }
        return ContextValue.present(descendants.getFirst().getValue().value());
    }

    /**
     * Creates and records one stable PARALLEL branch scope with explicit parent provenance.
     *
     * @param parentScope scope containing the PARALLEL node
     * @param parallelNodeCode stable node code used only in the queryable address
     * @param branchIndex stable compiled-target index
     * @return registered branch scope
     */
    ScopePath parallelBranchScope(ScopePath parentScope, String parallelNodeCode, int branchIndex) {
        Objects.requireNonNull(parentScope, "parallel parent scope must not be null");
        String normalizedCode = JsonValues.requireText(parallelNodeCode, "parallel node code");
        if (branchIndex < 0) {
            throw new IllegalArgumentException("parallel branch index must be non-negative");
        }
        ScopePath scope =
                parentScope.child("parallel:" + normalizedCode).child("branch:" + branchIndex);
        ParallelBranch branch = new ParallelBranch(parentScope, normalizedCode, branchIndex);
        ParallelBranch existing = parallelBranches.putIfAbsent(scope, branch);
        if (existing != null && !existing.equals(branch)) {
            throw new IllegalStateException("parallel branch scope provenance is inconsistent");
        }
        return scope;
    }

    /**
     * Collapses only explicitly related parallel branch scopes and records their convergence.
     * Identical scopes, including LOOP iteration scopes, remain exact and create no promotion
     * grant.
     *
     * @param activeScopes scopes carried by activated incoming edges
     * @return exact scope or validated PARALLEL parent convergence scope
     */
    ScopePath convergeScopes(Collection<ScopePath> activeScopes) {
        Objects.requireNonNull(activeScopes, "active scopes must not be null");
        LinkedHashSet<ScopePath> distinct = new LinkedHashSet<>(activeScopes);
        if (distinct.isEmpty()) {
            throw new IllegalArgumentException("active scopes must not be empty");
        }
        if (distinct.size() == 1) {
            return distinct.getFirst();
        }
        ScopePath convergence = null;
        for (ScopePath scope : distinct) {
            ParallelBranch branch = parallelBranches.get(scope);
            ScopePath candidate = branch == null ? scope : branch.parentScope();
            if (convergence == null) {
                convergence = candidate;
            } else if (!convergence.equals(candidate)) {
                throw new IllegalStateException(
                        "active scopes do not share one explicit parallel convergence");
            }
        }
        Set<ScopePath> branches = new LinkedHashSet<>();
        for (ScopePath scope : distinct) {
            if (scope.equals(convergence)) {
                continue;
            }
            ParallelBranch branch = parallelBranches.get(scope);
            if (branch == null || !branch.parentScope().equals(convergence)) {
                throw new IllegalStateException(
                        "scope is not an explicit branch of the convergence");
            }
            branches.add(scope);
        }
        if (branches.isEmpty()) {
            throw new IllegalStateException(
                    "parallel convergence requires at least one branch scope");
        }
        parallelConvergences.compute(
                convergence,
                (ignored, current) -> {
                    LinkedHashSet<ScopePath> merged =
                            new LinkedHashSet<>(current == null ? Set.of() : current);
                    merged.addAll(branches);
                    return Set.copyOf(merged);
                });
        return convergence;
    }

    private boolean isParallelConverged(ScopePath outputScope, ScopePath convergenceScope) {
        Set<ScopePath> sources = parallelConvergences.get(convergenceScope);
        if (sources == null) {
            return false;
        }
        for (ScopePath source : sources) {
            if (outputScope.equals(source) || isParallelConverged(outputScope, source)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an addressed output, including JSON null, or fails when no write exists.
     *
     * @throws IllegalArgumentException when the address has not been written
     */
    public Object required(String nodeCode, ScopePath scopePath, String outputKey) {
        ContextValue value = lookup(nodeCode, scopePath, outputKey);
        if (!value.present()) {
            throw new IllegalArgumentException(
                    "missing node output: " + nodeCode + "." + outputKey);
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
                    .filter(
                            entry ->
                                    entry.getKey().nodeCode().equals(segments[1])
                                            && entry.getKey().scopePath().equals(scopePath))
                    .sorted(Comparator.comparing(entry -> entry.getKey().outputKey()))
                    .forEach(matches::add);
            if (matches.size() != 1) {
                throw new IllegalArgumentException(
                        "single-output context path is not uniquely resolved: " + path);
            }
            return matches.getFirst().getValue().value();
        }
        throw new IllegalArgumentException("unsupported context path: " + path);
    }

    /**
     * Returns a deeply immutable snapshot of outputs visible in one execution scope.
     *
     * <p>The snapshot is grouped by stable node code and output key. It is used only to create
     * adapter-neutral read views; callers cannot mutate the append-only store.
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
                .sorted(
                        Map.Entry.<String, String>comparingByKey()
                                .thenComparing(Map.Entry::getValue))
                .forEach(
                        address -> {
                            try {
                                ContextValue visible =
                                        lookupVisible(
                                                address.getKey(), scopePath, address.getValue());
                                if (visible.present()) {
                                    grouped.computeIfAbsent(
                                                    address.getKey(), ignored -> new TreeMap<>())
                                            .put(address.getValue(), visible.value());
                                }
                            } catch (IllegalArgumentException ignored) {
                                // Multiple completed loop frames are intentionally not projected
                                // into a parent ctx view.
                            }
                        });
        Map<String, Map<String, Object>> snapshot = new java.util.LinkedHashMap<>();
        grouped.forEach(
                (nodeCode, values) ->
                        snapshot.put(nodeCode, JsonValues.copyObject(values, "ctx." + nodeCode)));
        return java.util.Collections.unmodifiableMap(snapshot);
    }

    /** Records one independently owned child-flow context outside the business ctx namespace. */
    public void registerChildExecution(
            String childExecutionId, ScopePath scopePath, ExecutionContext context) {
        String normalized = JsonValues.requireText(childExecutionId, "child execution id");
        ChildExecution child =
                new ChildExecution(
                        normalized,
                        Objects.requireNonNull(scopePath, "child scope must not be null"),
                        Objects.requireNonNull(context, "child context must not be null"));
        if (childExecutions.putIfAbsent(normalized, child) != null) {
            throw new IllegalStateException("child execution id may only be registered once");
        }
    }

    /**
     * @return immutable stable child-flow diagnostic index
     */
    public Map<String, ChildExecution> childExecutions() {
        return java.util.Collections.unmodifiableMap(new TreeMap<>(childExecutions));
    }

    /**
     * Full addressed snapshot, unlike outputsByNode's scope projection; null values remain present.
     */
    public Map<String, Object> snapshot() {
        List<Object> values = new ArrayList<>();
        outputs.forEach(
                (address, value) -> {
                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    row.put("node", address.nodeCode());
                    row.put("scope", address.scopePath().value());
                    row.put("key", address.outputKey());
                    row.put("value", value.value());
                    values.add(row);
                });
        List<Object> branches = new ArrayList<>();
        parallelBranches.forEach(
                (scope, branch) ->
                        branches.add(
                                Map.of(
                                        "scope",
                                        scope.value(),
                                        "parent",
                                        branch.parentScope().value(),
                                        "node",
                                        branch.nodeCode(),
                                        "index",
                                        branch.branchIndex())));
        Map<String, Object> convergences = new TreeMap<>();
        parallelConvergences.forEach(
                (scope, sources) ->
                        convergences.put(
                                scope.value(), sources.stream().map(ScopePath::value).toList()));
        Map<String, Object> children = new TreeMap<>();
        childExecutions.forEach(
                (id, child) ->
                        children.put(
                                id,
                                Map.of(
                                        "scope",
                                        child.scopePath().value(),
                                        "context",
                                        child.context().snapshot())));
        return JsonValues.copyObject(
                Map.of(
                        "values",
                        values,
                        "branches",
                        branches,
                        "convergences",
                        convergences,
                        "children",
                        children),
                "checkpoint ctx");
    }

    /** Rehydrates only engine-owned JSON state before scheduling; never promotes loop scopes. */
    @SuppressWarnings("unchecked")
    public void restore(Map<String, Object> snapshot) {
        for (Object value : (List<?>) snapshot.getOrDefault("values", List.of())) {
            Map<String, Object> row = (Map<String, Object>) value;
            outputs.put(
                    address(
                            (String) row.get("node"),
                            new ScopePath((String) row.get("scope")),
                            (String) row.get("key")),
                    new StoredValue(row.get("value")));
        }
        for (Object value : (List<?>) snapshot.getOrDefault("branches", List.of())) {
            Map<String, Object> row = (Map<String, Object>) value;
            parallelBranches.put(
                    new ScopePath((String) row.get("scope")),
                    new ParallelBranch(
                            new ScopePath((String) row.get("parent")),
                            (String) row.get("node"),
                            ((Number) row.get("index")).intValue()));
        }
        ((Map<String, Object>) snapshot.getOrDefault("convergences", Map.of()))
                .forEach(
                        (scope, values) ->
                                parallelConvergences.put(
                                        new ScopePath(scope),
                                        ((List<String>) values)
                                                .stream()
                                                        .map(ScopePath::new)
                                                        .collect(
                                                                java.util.stream.Collectors
                                                                        .toSet())));
        ((Map<String, Object>) snapshot.getOrDefault("children", Map.of()))
                .forEach(
                        (id, value) -> {
                            Map<String, Object> row = (Map<String, Object>) value;
                            var child = new ExecutionContext(externalScopes);
                            child.restore((Map<String, Object>) row.get("context"));
                            childExecutions.put(
                                    id,
                                    new ChildExecution(
                                            id, new ScopePath((String) row.get("scope")), child));
                        });
    }

    private static OutputAddress address(String nodeCode, ScopePath scopePath, String outputKey) {
        return new OutputAddress(
                JsonValues.requireText(nodeCode, "node code"),
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

    private record OutputAddress(String nodeCode, ScopePath scopePath, String outputKey) {}

    private record StoredValue(Object value) {}

    /** Explicit origin of one PARALLEL branch path; address text is never parsed for semantics. */
    private record ParallelBranch(ScopePath parentScope, String nodeCode, int branchIndex) {}

    /** Independently owned child-flow context and its stable scope identity. */
    public record ChildExecution(
            String childExecutionId, ScopePath scopePath, ExecutionContext context) {}
}
