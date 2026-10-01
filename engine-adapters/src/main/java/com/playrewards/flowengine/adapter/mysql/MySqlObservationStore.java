package com.playrewards.flowengine.adapter.mysql;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import java.util.*;

/**
 * Tenant-scoped observation tables. Terminal state and event commit atomically before publication.
 */
public final class MySqlObservationStore implements ExecutionRecordStore {
    private final MySqlRuntimeDatabase db;
    private final String tenant;
    private final int capacity;
    private final java.time.Duration retention;
    private Map<String, Integer> capacities = Map.of();
    private Map<String, java.time.Duration> retentions = Map.of();

    public MySqlObservationStore(MySqlRuntimeDatabase db, String tenant) {
        this(db, tenant, 10000, java.time.Duration.ofHours(1));
    }

    public MySqlObservationStore(
            MySqlRuntimeDatabase db, String tenant, int capacity, java.time.Duration retention) {
        if (capacity < 1 || retention.isZero() || retention.isNegative())
            throw new IllegalArgumentException("observation bounds invalid");
        this.db = db;
        this.tenant = tenant;
        this.capacity = capacity;
        this.retention = retention;
    }

    /** Independently configurable event/log/content retention and per-table row bounds. */
    public MySqlObservationStore(
            MySqlRuntimeDatabase db,
            String tenant,
            Map<String, Integer> capacities,
            Map<String, java.time.Duration> retentions) {
        this(db, tenant);
        if (capacities.values().stream().anyMatch(v -> v < 1)
                || retentions.values().stream().anyMatch(v -> v.isZero() || v.isNegative()))
            throw new IllegalArgumentException("observation bounds invalid");
        this.capacities = Map.copyOf(capacities);
        this.retentions = Map.copyOf(retentions);
    }

    private int capacity(String table) {
        return capacities.getOrDefault(table, capacity);
    }

    public void saveExecution(ExecutionRecord record) {
        owned(record);
        db.capacityTransaction(
                tenant,
                tx -> {
                    save(tx, record);
                    return null;
                });
    }

    private void owned(ExecutionRecord record) {
        if (!tenant.equals(record.flowIdentity().tenantId()))
            throw new SecurityException("EXECUTION_TENANT_MISMATCH");
    }

    private void save(MySqlRuntimeDatabase.Tx tx, ExecutionRecord r) {
        MySqlCapacity.lock(tx, tenant);
        if (one(tx, "engine_execution", "execution_id", r.executionId(), ExecutionRecord.class)
                .isEmpty())
            MySqlCapacity.reserve(tx, tenant, "engine_execution", capacity("engine_execution"));
        tx.update(
                "INSERT INTO engine_execution(tenant_id,execution_id,status,event_cursor,body)"
                    + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]},#{args[4]}) ON DUPLICATE"
                    + " KEY UPDATE"
                    + " status=VALUES(status),body=VALUES(body),updated_at=CURRENT_TIMESTAMP(6)",
                tenant,
                r.executionId(),
                r.status().name(),
                r.eventCursor(),
                json(r));
    }

    public Optional<ExecutionRecord> execution(String id) {
        return db.transaction(
                tx -> one(tx, "engine_execution", "execution_id", id, ExecutionRecord.class));
    }

    public void commitTerminal(ExecutionRecord record, ExecutionEvent event) {
        owned(record);
        if (!record.executionId().equals(event.executionId()) || record.status() != event.status())
            throw new IllegalStateException("TERMINAL_EVENT_MISMATCH");
        db.capacityTransaction(
                tenant,
                tx -> {
                    save(tx, record);
                    append(tx, event);
                    return null;
                });
    }

    public void append(ExecutionEvent event) {
        db.capacityTransaction(
                tenant,
                tx -> {
                    append(tx, event);
                    return null;
                });
    }

    public void commitTerminal(ExecutionRecord record, ExecutionEvent event, ExecutionLease lease) {
        owned(record);
        if (!record.executionId().equals(event.executionId())
                || record.status() != event.status()
                || !record.rootExecutionId().equals(lease.executionId()))
            throw new IllegalStateException("TERMINAL_EVENT_MISMATCH");
        db.capacityTransaction(
                tenant,
                tx -> {
                    MySqlLeaseRepository.validate(tx, tenant, lease.executionId(), lease);
                    save(tx, record);
                    append(tx, event);
                    return null;
                });
    }

    private void append(MySqlRuntimeDatabase.Tx tx, ExecutionEvent e) {
        prune(tx);
        MySqlCapacity.reserve(tx, tenant, "engine_event", capacity("engine_event"));
        var current =
                tx.row(
                                "SELECT event_cursor FROM engine_execution WHERE"
                                        + " tenant_id=#{args[0]} AND execution_id=#{args[1]} FOR"
                                        + " UPDATE",
                                tenant,
                                e.executionId())
                        .orElseThrow(() -> new IllegalStateException("EXECUTION_NOT_FOUND"));
        if (e.sequence() <= MySqlRuntimeDatabase.number(current, "event_cursor"))
            throw new IllegalStateException("EVENT_SEQUENCE_CONFLICT");
        tx.update(
                "INSERT INTO engine_event(tenant_id,execution_id,sequence,body)"
                        + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                tenant,
                e.executionId(),
                e.sequence(),
                json(e));
        tx.update(
                "UPDATE engine_execution SET event_cursor=#{args[2]} WHERE tenant_id=#{args[0]} AND"
                        + " execution_id=#{args[1]}",
                tenant,
                e.executionId(),
                e.sequence());
    }

    public List<ExecutionEvent> readAfter(String id, long cursor) {
        ExecutionObservationStore.requireCursor(cursor);
        return db.transaction(
                tx ->
                        tx
                                .rows(
                                        "SELECT body FROM engine_event WHERE tenant_id=#{args[0]}"
                                            + " AND execution_id=#{args[1]} AND sequence>#{args[2]}"
                                            + " ORDER BY sequence LIMIT 10000",
                                        tenant,
                                        id,
                                        cursor)
                                .stream()
                                .map(r -> parse(r, ExecutionEvent.class))
                                .toList());
    }

    public void saveNode(NodeExecutionRecord r) {
        db.capacityTransaction(
                tenant,
                tx -> {
                    prune(tx);
                    if (one(
                                    tx,
                                    "engine_execution",
                                    "execution_id",
                                    r.executionId(),
                                    ExecutionRecord.class)
                            .isEmpty()) throw new IllegalStateException("EXECUTION_NOT_FOUND");
                    var existing =
                            tx.row(
                                    "SELECT node_execution_id FROM engine_node_attempt WHERE"
                                        + " tenant_id=#{args[0]} AND execution_id=#{args[1]} AND"
                                        + " node_id=#{args[2]} AND scope_digest=#{args[3]} AND"
                                        + " attempt=#{args[4]}",
                                    tenant,
                                    r.executionId(),
                                    r.nodeId(),
                                    MySqlIdempotencyStore.digest(r.scopePath()),
                                    r.attempt());
                    if (existing.isPresent()
                            && !r.nodeExecutionId()
                                    .equals(
                                            MySqlRuntimeDatabase.text(
                                                    existing.get(), "node_execution_id")))
                        throw new IllegalStateException("NODE_ATTEMPT_IDENTITY_CONFLICT");
                    if (existing.isEmpty())
                        MySqlCapacity.reserve(
                                tx, tenant, "engine_node_attempt", capacity("engine_node_attempt"));
                    tx.update(
                            "INSERT INTO"
                                + " engine_node_attempt(tenant_id,node_execution_id,execution_id,node_id,scope_digest,attempt,body)"
                                + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]},#{args[4]},#{args[5]},#{args[6]})"
                                + " ON DUPLICATE KEY UPDATE"
                                + " body=IF(node_execution_id=VALUES(node_execution_id),VALUES(body),body)",
                            tenant,
                            r.nodeExecutionId(),
                            r.executionId(),
                            r.nodeId(),
                            MySqlIdempotencyStore.digest(r.scopePath()),
                            r.attempt(),
                            json(r));
                    return null;
                });
    }

    public Optional<NodeExecutionRecord> node(String id) {
        return db.transaction(
                tx ->
                        one(
                                tx,
                                "engine_node_attempt",
                                "node_execution_id",
                                id,
                                NodeExecutionRecord.class));
    }

    public List<NodeExecutionRecord> nodes(String id) {
        return db.transaction(
                tx ->
                        many(
                                tx,
                                "engine_node_attempt",
                                "execution_id",
                                id,
                                "attempt",
                                NodeExecutionRecord.class));
    }

    public void appendLog(ExecutionLogRecord r) {
        db.capacityTransaction(
                tenant,
                tx -> {
                    prune(tx);
                    MySqlCapacity.reserve(tx, tenant, "engine_log", capacity("engine_log"));
                    if (one(
                                    tx,
                                    "engine_node_attempt",
                                    "node_execution_id",
                                    r.nodeExecutionId(),
                                    NodeExecutionRecord.class)
                            .isEmpty()) throw new IllegalStateException("NODE_NOT_FOUND");
                    tx.update(
                            "INSERT INTO engine_log(tenant_id,node_execution_id,sequence,body)"
                                    + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                            tenant,
                            r.nodeExecutionId(),
                            r.sequence(),
                            json(r));
                    return null;
                });
    }

    public List<ExecutionLogRecord> logs(String id) {
        return db.transaction(
                tx ->
                        many(
                                tx,
                                "engine_log",
                                "node_execution_id",
                                id,
                                "sequence",
                                ExecutionLogRecord.class));
    }

    public void appendContent(String id, ContentChunk chunk) {
        db.capacityTransaction(
                tenant,
                tx -> {
                    prune(tx);
                    MySqlCapacity.reserve(tx, tenant, "engine_content", capacity("engine_content"));
                    if (one(
                                    tx,
                                    "engine_node_attempt",
                                    "node_execution_id",
                                    id,
                                    NodeExecutionRecord.class)
                            .isEmpty()) throw new IllegalStateException("NODE_NOT_FOUND");
                    tx.update(
                            "INSERT INTO engine_content(tenant_id,node_execution_id,sequence,body)"
                                    + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                            tenant,
                            id,
                            chunk.sequence(),
                            json(chunk));
                    return null;
                });
    }

    public List<ContentChunk> content(String id) {
        return db.transaction(
                tx ->
                        many(
                                tx,
                                "engine_content",
                                "node_execution_id",
                                id,
                                "sequence",
                                ContentChunk.class));
    }

    public void flush(String id) {
        /* Each write has already committed synchronously. */
    }

    /** Diagnostic retention never deletes strong checkpoints or their pinned values. */
    private void prune(MySqlRuntimeDatabase.Tx tx) {
        MySqlCapacity.lock(tx, tenant);
        for (String table : List.of("engine_event", "engine_log", "engine_content"))
            tx.update(
                    "DELETE FROM "
                            + table
                            + " WHERE tenant_id=#{args[0]} AND"
                            + " created_at<TIMESTAMPADD(MICROSECOND,-#{args[1]},CURRENT_TIMESTAMP(6))",
                    tenant,
                    retentions.getOrDefault(table, retention).toMillis() * 1000);
    }

    private <T> Optional<T> one(
            MySqlRuntimeDatabase.Tx tx, String table, String key, String id, Class<T> type) {
        return tx.row(
                        "SELECT body FROM "
                                + table
                                + " WHERE tenant_id=#{args[0]} AND "
                                + key
                                + "=#{args[1]}",
                        tenant,
                        id)
                .map(r -> parse(r, type));
    }

    private <T> List<T> many(
            MySqlRuntimeDatabase.Tx tx,
            String table,
            String key,
            String id,
            String order,
            Class<T> type) {
        return tx
                .rows(
                        "SELECT body FROM "
                                + table
                                + " WHERE tenant_id=#{args[0]} AND "
                                + key
                                + "=#{args[1]} ORDER BY "
                                + order
                                + " LIMIT 10000",
                        tenant,
                        id)
                .stream()
                .map(r -> parse(r, type))
                .toList();
    }

    private static <T> T parse(Map<String, Object> row, Class<T> type) {
        return JSON.parseObject(MySqlRuntimeDatabase.text(row, "body"), type);
    }

    private static String json(Object value) {
        String text = JSON.toJSONString(value);
        if (text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 262144)
            throw new IllegalArgumentException("OBSERVATION_TOO_LARGE");
        return text;
    }
}
