package com.playrewards.flowengine.adapter.mysql;

import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import java.time.Duration;
import java.util.*;

/**
 * Internal owner routing for a multi-tenant host. API authorization still runs in ExecutionService;
 * opaque IDs resolve to exactly one tenant or fail closed. No caller-selected default tenant
 * exists.
 */
public final class MySqlTenantStoreRouter implements ExecutionRecordStore, PayloadStore {
    private final MySqlRuntimeDatabase db;
    private final Map<String, Integer> capacities;
    private final MySqlRetentionPolicy policy;
    private final int payloadMaxBytes;
    private final Duration payloadRetention;

    public MySqlTenantStoreRouter(MySqlRuntimeDatabase db) {
        this(db, Map.of(), Map.of(), 1048576, Duration.ofHours(1));
    }

    /**
     * Host bounds apply identically to every authenticated tenant; routing never changes limits.
     */
    public MySqlTenantStoreRouter(
            MySqlRuntimeDatabase db,
            Map<String, Integer> capacities,
            Map<String, Duration> retentions,
            int payloadMaxBytes,
            Duration payloadRetention) {
        this(
                db,
                capacities,
                MySqlRetentionPolicy.defaults()
                        .withRowRetention(
                                retentions.getOrDefault("engine_event", Duration.ofHours(1)),
                                retentions.getOrDefault("engine_log", Duration.ofHours(1)),
                                retentions.getOrDefault("engine_content", Duration.ofHours(1))),
                payloadMaxBytes,
                payloadRetention);
    }

    /**
     * 使用完整保留策略构造路由；所有租户共享相同上限与保留期。
     *
     * @param capacities 表名到租户行数上限
     * @param policy 执行与行级保留策略，同时决定容量满时的过期清理范围
     * @param payloadMaxBytes 单个 Payload 字节上限
     * @param payloadRetention 非固定 Payload 保留期
     */
    public MySqlTenantStoreRouter(
            MySqlRuntimeDatabase db,
            Map<String, Integer> capacities,
            MySqlRetentionPolicy policy,
            int payloadMaxBytes,
            Duration payloadRetention) {
        this.db = db;
        this.capacities = Map.copyOf(capacities);
        this.policy = Objects.requireNonNull(policy);
        this.payloadMaxBytes = payloadMaxBytes;
        this.payloadRetention = payloadRetention;
    }

    public DurableStores durableStores(String tenant) {
        return new DurableStores(
                new MySqlExecutionStateStore(db, tenant),
                new MySqlLeaseRepository(db, tenant),
                payloads(tenant));
    }

    /**
     * Bounded startup/takeover scan reads only strong state, including previously served tenants.
     * Live leases are excluded before limiting so long-running owners cannot starve recovery.
     * Candidates still acquire a fenced lease after scanning; this query does not grant ownership.
     */
    public List<RecoveryCandidate> recoverable(int limit) {
        if (limit < 1 || limit > 1000)
            throw new IllegalArgumentException("recovery batch out of bounds");
        return db.transaction(
                tx ->
                        tx
                                .rows(
                                        "SELECT s.tenant_id,s.execution_id FROM engine_execution_state s"
                                                + " WHERE s.status IN"
                                                + " ('CREATED','RUNNING','WAITING_RETRY')"
                                                + " AND NOT EXISTS (SELECT 1 FROM engine_lease l"
                                                + " WHERE l.tenant_id=s.tenant_id"
                                                + " AND l.execution_id=s.execution_id"
                                                + " AND l.expires_at>CURRENT_TIMESTAMP(6)) ORDER BY"
                                                + " s.updated_at,s.tenant_id,s.execution_id LIMIT #{args[0]}",
                                        limit)
                                .stream()
                                .map(
                                        r ->
                                                new RecoveryCandidate(
                                                        MySqlRuntimeDatabase.text(r, "tenant_id"),
                                                        MySqlRuntimeDatabase.text(
                                                                r, "execution_id")))
                                .toList());
    }

    public record RecoveryCandidate(String tenantId, String executionId) {}

    private MySqlObservationStore records(String tenant) {
        return new MySqlObservationStore(db, tenant, capacities, policy);
    }

    private MySqlPayloadStore payloads(String tenant) {
        return new MySqlPayloadStore(db, tenant, payloadMaxBytes, payloadRetention);
    }

    private Optional<String> tenant(String table, String key, String id) {
        return db.transaction(
                tx -> {
                    var rows =
                            tx.rows(
                                    "SELECT tenant_id FROM "
                                            + table
                                            + " WHERE "
                                            + key
                                            + "=#{args[0]} LIMIT 2",
                                    id);
                    if (rows.size() > 1) throw new SecurityException("AMBIGUOUS_RUNTIME_IDENTITY");
                    return rows.stream()
                            .findFirst()
                            .map(r -> MySqlRuntimeDatabase.text(r, "tenant_id"));
                });
    }

    private String executionTenant(String id) {
        return tenant("engine_execution", "execution_id", id)
                .orElseThrow(() -> new IllegalStateException("EXECUTION_NOT_FOUND"));
    }

    private String nodeTenant(String id) {
        return tenant("engine_node_attempt", "node_execution_id", id)
                .orElseThrow(() -> new IllegalStateException("NODE_NOT_FOUND"));
    }

    public void saveExecution(ExecutionRecord r) {
        records(r.flowIdentity().tenantId()).saveExecution(r);
    }

    public Optional<ExecutionRecord> execution(String id) {
        return tenant("engine_execution", "execution_id", id)
                .flatMap(t -> records(t).execution(id));
    }

    public void commitTerminal(ExecutionRecord r, ExecutionEvent e) {
        records(r.flowIdentity().tenantId()).commitTerminal(r, e);
    }

    public void commitTerminal(ExecutionRecord r, ExecutionEvent e, ExecutionLease l) {
        records(r.flowIdentity().tenantId()).commitTerminal(r, e, l);
    }

    public void append(ExecutionEvent e) {
        records(executionTenant(e.executionId())).append(e);
    }

    public List<ExecutionEvent> readAfter(String id, long cursor) {
        return tenant("engine_execution", "execution_id", id)
                .map(t -> records(t).readAfter(id, cursor))
                .orElse(List.of());
    }

    public void saveNode(NodeExecutionRecord r) {
        records(executionTenant(r.executionId())).saveNode(r);
    }

    public Optional<NodeExecutionRecord> node(String id) {
        return tenant("engine_node_attempt", "node_execution_id", id)
                .flatMap(t -> records(t).node(id));
    }

    public List<NodeExecutionRecord> nodes(String id) {
        return tenant("engine_execution", "execution_id", id)
                .map(t -> records(t).nodes(id))
                .orElse(List.of());
    }

    public void appendLog(ExecutionLogRecord r) {
        records(nodeTenant(r.nodeExecutionId())).appendLog(r);
    }

    public List<ExecutionLogRecord> logs(String id) {
        return tenant("engine_node_attempt", "node_execution_id", id)
                .map(t -> records(t).logs(id))
                .orElse(List.of());
    }

    public void appendContent(String id, ContentChunk c) {
        records(nodeTenant(id)).appendContent(id, c);
    }

    public List<ContentChunk> content(String id) {
        return tenant("engine_node_attempt", "node_execution_id", id)
                .map(t -> records(t).content(id))
                .orElse(List.of());
    }

    public void flush(String id) {
        records(executionTenant(id)).flush(id);
    }

    public PayloadReference store(String id, Object value) {
        return payloads(executionTenant(id)).store(id, value);
    }

    public Optional<Object> find(PayloadReference ref) {
        return tenant("engine_payload", "payload_id", ref.value())
                .flatMap(t -> payloads(t).find(ref));
    }

    public Optional<String> executionId(PayloadReference ref) {
        return tenant("engine_payload", "payload_id", ref.value())
                .flatMap(t -> payloads(t).executionId(ref));
    }
}
