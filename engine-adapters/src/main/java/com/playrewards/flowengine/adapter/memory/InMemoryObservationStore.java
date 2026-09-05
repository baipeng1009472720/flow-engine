package com.playrewards.flowengine.adapter.memory;

import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import java.time.*;
import java.util.*;

/**
 * Finite, synchronized EPHEMERAL tables. Completed executions expire together; active ones never
 * do.
 */
public final class InMemoryObservationStore implements ExecutionRecordStore, PayloadStore {
    private final Limits limits;
    private final Clock clock;
    private final Map<String, ExecutionRecord> executions = new LinkedHashMap<>();
    private final Map<String, NodeExecutionRecord> nodes = new LinkedHashMap<>();
    private final Map<String, List<ExecutionEvent>> events = new HashMap<>();
    private final Map<String, List<ExecutionLogRecord>> logs = new HashMap<>();
    private final Map<String, List<ContentChunk>> contents = new HashMap<>();
    private final Map<String, StoredPayload> payloads = new HashMap<>();

    /** Uses finite local-demo defaults with one-hour completed-execution retention. */
    public InMemoryObservationStore() {
        this(Limits.defaults(), Clock.systemUTC());
    }

    /**
     * Creates bounded tables; exhausting active capacity rejects rather than evicts an execution.
     */
    public InMemoryObservationStore(
            int maxExecutions, int maxRows, Duration retention, Clock clock) {
        this(
                new Limits(
                        maxExecutions,
                        maxRows,
                        maxRows,
                        Math.min(maxRows, 1024),
                        Math.min(maxRows, 4096),
                        maxRows,
                        65536,
                        65536,
                        16384,
                        retention,
                        retention,
                        retention,
                        retention),
                clock);
    }

    /**
     * Applies independent per-kind retention and size budgets while sharing owner identity and
     * expiry protection.
     */
    public InMemoryObservationStore(Limits limits, Clock clock) {
        this.limits = Objects.requireNonNull(limits);
        this.clock = clock;
    }

    @Override
    public synchronized void saveExecution(ExecutionRecord value) {
        expire();
        if (!executions.containsKey(value.executionId())
                && executions.size() >= limits.maxExecutions())
            throw new IllegalStateException("OBSERVATION_CAPACITY_EXCEEDED");
        executions.put(value.executionId(), value);
    }

    /** 所有拒绝条件均在修改前校验，同一监视器保护查询及终态的原子可见性。 */
    @Override
    public synchronized void commitTerminal(ExecutionRecord record, ExecutionEvent event) {
        required(record.executionId());
        if (record.finishedAt() == null
                || !record.executionId().equals(event.executionId())
                || record.status() != event.status())
            throw new IllegalArgumentException("TERMINAL_RECORD_INVALID");
        bounded(event.attributes(), limits.maxEventBytes());
        var rows = events.getOrDefault(record.executionId(), List.of());
        if (!rows.isEmpty() && rows.getLast().sequence() >= event.sequence())
            throw new IllegalStateException("EVENT_ORDER_INVALID");
        append(event);
        executions.put(record.executionId(), record);
    }

    @Override
    public synchronized Optional<ExecutionRecord> execution(String id) {
        expire();
        return Optional.ofNullable(executions.get(id));
    }

    @Override
    public synchronized void saveNode(NodeExecutionRecord value) {
        required(value.executionId());
        if (!nodes.containsKey(value.nodeExecutionId())
                && nodes(value.executionId()).size() >= limits.maxNodes())
            throw new IllegalStateException("NODE_RECORD_CAPACITY_EXCEEDED");
        nodes.put(value.nodeExecutionId(), value);
    }

    @Override
    public synchronized Optional<NodeExecutionRecord> node(String id) {
        expire();
        return Optional.ofNullable(nodes.get(id));
    }

    @Override
    public synchronized List<NodeExecutionRecord> nodes(String id) {
        expire();
        return nodes.values().stream().filter(n -> n.executionId().equals(id)).toList();
    }

    @Override
    public synchronized void append(ExecutionEvent value) {
        required(value.executionId());
        bounded(value.attributes(), limits.maxEventBytes());
        var rows = events.computeIfAbsent(value.executionId(), k -> new ArrayList<>());
        if (!rows.isEmpty() && rows.getLast().sequence() >= value.sequence())
            throw new IllegalStateException("EVENT_ORDER_INVALID");
        if (rows.size() >= limits.maxEvents()) rows.removeFirst();
        rows.add(value);
    }

    @Override
    public synchronized List<ExecutionEvent> readAfter(String id, long cursor) {
        ExecutionObservationStore.requireCursor(cursor);
        expire();
        var rows = events.getOrDefault(id, List.of());
        if (rows.isEmpty()
                && executions.containsKey(id)
                && expired(executions.get(id), limits.eventRetention()))
            throw new IllegalArgumentException("EVENT_CURSOR_EXPIRED");
        if (!rows.isEmpty() && cursor < rows.getFirst().sequence() - 1)
            throw new IllegalArgumentException("EVENT_CURSOR_EXPIRED");
        return rows.stream().filter(e -> e.sequence() > cursor).toList();
    }

    @Override
    public synchronized void appendLog(ExecutionLogRecord value) {
        required(value.executionId());
        var node = nodes.get(value.nodeExecutionId());
        if (node == null || !node.executionId().equals(value.executionId()))
            throw new IllegalArgumentException("NODE_NOT_FOUND");
        bounded(
                Map.of("message", value.message(), "attributes", value.maskedAttributes()),
                limits.maxLogBytes());
        var rows = logs.computeIfAbsent(value.nodeExecutionId(), k -> new ArrayList<>());
        if (rows.size() >= limits.maxLogs()) rows.removeFirst();
        rows.add(value);
    }

    @Override
    public synchronized List<ExecutionLogRecord> logs(String id) {
        expire();
        return List.copyOf(logs.getOrDefault(id, List.of()));
    }

    @Override
    public synchronized void appendContent(String id, ContentChunk chunk) {
        chunk.validateResourceLimits();
        required(chunk.executionId());
        var node = nodes.get(id);
        if (node == null || !node.executionId().equals(chunk.executionId()))
            throw new IllegalArgumentException("NODE_NOT_FOUND");
        var rows = contents.computeIfAbsent(id, k -> new ArrayList<>());
        if (rows.size() >= limits.maxContentChunks())
            throw new IllegalStateException("CONTENT_CAPACITY_EXCEEDED");
        rows.add(chunk);
    }

    @Override
    public synchronized List<ContentChunk> content(String id) {
        expire();
        return List.copyOf(contents.getOrDefault(id, List.of()));
    }

    @Override
    public void flush(String id) {
        /* Writes are synchronous and snapshots immutable. */
    }

    @Override
    public synchronized PayloadReference store(String id, Object payload) {
        required(id);
        if (payloads.values().stream().filter(p -> p.executionId().equals(id)).count()
                >= limits.maxPayloads())
            throw new IllegalStateException("PAYLOAD_CAPACITY_EXCEEDED");
        bounded(payload, limits.maxPayloadBytes());
        String key = UUID.randomUUID().toString();
        payloads.put(
                key,
                new StoredPayload(
                        id,
                        com.playrewards.flowengine.contract.json.JsonValues.copyValue(
                                payload, "payload")));
        return new PayloadReference(key);
    }

    @Override
    public synchronized Optional<Object> find(PayloadReference reference) {
        expire();
        var value = payloads.get(reference.value());
        return value == null ? Optional.empty() : Optional.ofNullable(value.value());
    }

    /**
     * Returns the owner for independent payload authorization; never accepts a tenant from the
     * client.
     */
    public synchronized Optional<String> payloadExecution(String id) {
        expire();
        return Optional.ofNullable(payloads.get(id)).map(StoredPayload::executionId);
    }

    @Override
    public Optional<String> executionId(PayloadReference reference) {
        return payloadExecution(reference.value());
    }

    private void required(String id) {
        expire();
        if (!executions.containsKey(id)) throw new IllegalArgumentException("EXECUTION_NOT_FOUND");
    }

    private void expire() {
        Set<String> removed = new HashSet<>();
        executions
                .values()
                .removeIf(
                        e -> {
                            boolean expired =
                                    e.finishedAt() != null
                                            && e.finishedAt()
                                                    .plus(limits.executionRetention())
                                                    .isBefore(clock.instant());
                            if (expired) removed.add(e.executionId());
                            return expired;
                        });
        // Each diagnostic kind may expire earlier than its execution summary, but never while
        // active.
        for (var execution : executions.values()) {
            if (expired(execution, limits.eventRetention())) events.remove(execution.executionId());
            if (expired(execution, limits.logRetention()))
                nodes.values().stream()
                        .filter(n -> n.executionId().equals(execution.executionId()))
                        .forEach(n -> logs.remove(n.nodeExecutionId()));
            if (expired(execution, limits.payloadRetention())) {
                payloads.values().removeIf(p -> p.executionId().equals(execution.executionId()));
                nodes.values().stream()
                        .filter(n -> n.executionId().equals(execution.executionId()))
                        .forEach(n -> contents.remove(n.nodeExecutionId()));
            }
        }
        if (removed.isEmpty()) return;
        events.keySet().removeAll(removed);
        nodes.values()
                .removeIf(
                        n -> {
                            if (!removed.contains(n.executionId())) return false;
                            logs.remove(n.nodeExecutionId());
                            contents.remove(n.nodeExecutionId());
                            return true;
                        });
        payloads.values().removeIf(p -> removed.contains(p.executionId()));
    }

    private record StoredPayload(String executionId, Object value) {}

    private boolean expired(ExecutionRecord execution, Duration retention) {
        return execution.finishedAt() != null
                && execution.finishedAt().plus(retention).isBefore(clock.instant());
    }

    private static void bounded(Object value, int bytes) {
        com.playrewards.flowengine.contract.json.JsonValues.measureJsonResources(
                Collections.singletonList(value),
                new com.playrewards.flowengine.contract.json.JsonValues.JsonResourceLimits(
                        bytes, bytes, 24, 4096));
    }

    /** Independent finite budgets; retention clocks begin only after an execution has stopped. */
    public record Limits(
            int maxExecutions,
            int maxNodes,
            int maxEvents,
            int maxLogs,
            int maxContentChunks,
            int maxPayloads,
            int maxPayloadBytes,
            int maxEventBytes,
            int maxLogBytes,
            Duration executionRetention,
            Duration eventRetention,
            Duration logRetention,
            Duration payloadRetention) {
        public Limits {
            for (int value :
                    new int[] {
                        maxExecutions,
                        maxNodes,
                        maxEvents,
                        maxLogs,
                        maxContentChunks,
                        maxPayloads,
                        maxPayloadBytes,
                        maxEventBytes,
                        maxLogBytes
                    })
                if (value < 1)
                    throw new IllegalArgumentException("observation limits must be positive");
            for (Duration retention :
                    List.of(executionRetention, eventRetention, logRetention, payloadRetention))
                if (retention.isZero() || retention.isNegative())
                    throw new IllegalArgumentException("retention must be positive");
        }

        /**
         * Conservative standalone defaults; hosts can override every count, byte and retention
         * budget.
         */
        public static Limits defaults() {
            return new Limits(
                    1024,
                    16384,
                    16384,
                    1024,
                    4096,
                    16384,
                    65536,
                    65536,
                    16384,
                    Duration.ofHours(1),
                    Duration.ofHours(1),
                    Duration.ofHours(1),
                    Duration.ofHours(1));
        }
    }
}
