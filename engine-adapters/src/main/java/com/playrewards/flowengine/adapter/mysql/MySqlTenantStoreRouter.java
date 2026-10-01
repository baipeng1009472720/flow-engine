package com.playrewards.flowengine.adapter.mysql;

import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import java.time.Duration;
import java.util.*;

/**
 * Internal owner routing for a multi-tenant host. API authorization still runs in ExecutionService;
 * opaque IDs resolve to exactly one tenant or fail closed. No caller-selected default tenant
 * exists.
 *
 * <p>启用写后缓冲时：执行首次落库（容量预留需先于副作用）、带 finishedAt 的记录与终态事件同步提交；
 * 其余诊断写入进入按执行划分的有序通道异步批量提交。所有读取先排空相关通道，保证读己之写；
 * {@link #flush} 与终态提交前排空并报告失败；{@link #close} 排空全部通道。
 */
public final class MySqlTenantStoreRouter
        implements ExecutionRecordStore, PayloadStore, AutoCloseable {
    private static final int IDENTITY_CACHE = 65536;
    private static final Set<String> TERMINAL_EVENTS =
            Set.of("COMPLETED", "FAILED", "CANCELLED", "WAITING");
    private final MySqlRuntimeDatabase db;
    private final Map<String, Integer> capacities;
    private final MySqlRetentionPolicy policy;
    private final int payloadMaxBytes;
    private final Duration payloadRetention;
    private final ObservationWriteBehind buffer;
    /** executionId → tenant；只缓存已确认存在的执行，用于跳过路由查询并判断是否已首次落库。 */
    private final Map<String, String> tenants = lru();
    /** nodeExecutionId / payloadId → executionId，用于按不透明 ID 读取前排空所属通道。 */
    private final Map<String, String> owners = lru();

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
        this(db, capacities, policy, payloadMaxBytes, payloadRetention, null);
    }

    /**
     * 启用观察写后缓冲的构造。
     *
     * @param writeBehind 写线程数、全局待写上限与单批上限；为 null 时全部同步写入
     */
    public MySqlTenantStoreRouter(
            MySqlRuntimeDatabase db,
            Map<String, Integer> capacities,
            MySqlRetentionPolicy policy,
            int payloadMaxBytes,
            Duration payloadRetention,
            WriteBehind writeBehind) {
        this.db = db;
        this.capacities = Map.copyOf(capacities);
        this.policy = Objects.requireNonNull(policy);
        this.payloadMaxBytes = payloadMaxBytes;
        this.payloadRetention = payloadRetention;
        this.buffer =
                writeBehind == null
                        ? null
                        : new ObservationWriteBehind(
                                (tenant, batch) ->
                                        records(tenant).applyBatch(batch, payloadRetention),
                                writeBehind.writers(),
                                writeBehind.maxPending(),
                                writeBehind.maxBatch());
    }

    /**
     * 写后缓冲参数。
     *
     * @param writers 后台写线程（即后台最多占用的连接数）
     * @param maxPending 全局待写条数上限，超过后由提交者同步排空
     * @param maxBatch 单事务最多合并条数
     */
    public record WriteBehind(int writers, int maxPending, int maxBatch) {
        public WriteBehind {
            if (writers < 1 || maxPending < 1 || maxBatch < 1)
                throw new IllegalArgumentException("write-behind bounds invalid");
        }
    }

    private static Map<String, String> lru() {
        return Collections.synchronizedMap(
                new LinkedHashMap<>(1024, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                        return size() > IDENTITY_CACHE;
                    }
                });
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
        String cached = tenants.get(id);
        if (cached != null) return cached;
        String tenant =
                tenant("engine_execution", "execution_id", id)
                        .orElseThrow(() -> new IllegalStateException("EXECUTION_NOT_FOUND"));
        tenants.put(id, tenant);
        return tenant;
    }

    private String nodeTenant(String id) {
        return tenant("engine_node_attempt", "node_execution_id", id)
                .orElseThrow(() -> new IllegalStateException("NODE_NOT_FOUND"));
    }

    /** 读取或同步写入前排空一个执行的通道；失败保留给 flush 报告。 */
    private void drain(String executionId) {
        if (buffer != null && executionId != null) buffer.drain(executionId);
    }

    private void drainOwner(String opaqueId) {
        if (buffer != null) drain(owners.get(opaqueId));
    }

    public void saveExecution(ExecutionRecord r) {
        String tenant = r.flowIdentity().tenantId();
        if (buffer == null || r.finishedAt() != null || !tenant.equals(tenants.get(r.executionId()))) {
            // 首次落库同步完成容量预留与租户绑定；带 finishedAt 的补偿记录同样不得滞留在缓冲中。
            drain(r.executionId());
            records(tenant).saveExecution(r);
            tenants.put(r.executionId(), tenant);
            return;
        }
        buffer.submit(tenant, new ObservationWrite.SaveExecution(r));
    }

    public Optional<ExecutionRecord> execution(String id) {
        drain(id);
        return tenant("engine_execution", "execution_id", id)
                .flatMap(t -> records(t).execution(id));
    }

    public void commitTerminal(ExecutionRecord r, ExecutionEvent e) {
        drain(r.executionId());
        records(r.flowIdentity().tenantId()).commitTerminal(r, e);
        completed(r.executionId());
    }

    public void commitTerminal(ExecutionRecord r, ExecutionEvent e, ExecutionLease l) {
        drain(r.executionId());
        records(r.flowIdentity().tenantId()).commitTerminal(r, e, l);
        completed(r.executionId());
    }

    private void completed(String executionId) {
        if (buffer != null) buffer.complete(executionId);
    }

    public void append(ExecutionEvent e) {
        String tenant = executionTenant(e.executionId());
        if (buffer == null || (e.nodeId() == null && TERMINAL_EVENTS.contains(e.eventType()))) {
            drain(e.executionId());
            records(tenant).append(e);
            return;
        }
        buffer.submit(tenant, new ObservationWrite.AppendEvent(e));
    }

    public List<ExecutionEvent> readAfter(String id, long cursor) {
        drain(id);
        return tenant("engine_execution", "execution_id", id)
                .map(t -> records(t).readAfter(id, cursor))
                .orElse(List.of());
    }

    public void saveNode(NodeExecutionRecord r) {
        String tenant = executionTenant(r.executionId());
        if (buffer == null) {
            records(tenant).saveNode(r);
            return;
        }
        owners.put(r.nodeExecutionId(), r.executionId());
        buffer.submit(tenant, new ObservationWrite.SaveNode(r));
    }

    public Optional<NodeExecutionRecord> node(String id) {
        drainOwner(id);
        return tenant("engine_node_attempt", "node_execution_id", id)
                .flatMap(t -> records(t).node(id));
    }

    public List<NodeExecutionRecord> nodes(String id) {
        drain(id);
        return tenant("engine_execution", "execution_id", id)
                .map(t -> records(t).nodes(id))
                .orElse(List.of());
    }

    public void appendLog(ExecutionLogRecord r) {
        if (buffer == null) {
            records(nodeTenant(r.nodeExecutionId())).appendLog(r);
            return;
        }
        buffer.submit(executionTenant(r.executionId()), new ObservationWrite.AppendLog(r));
    }

    public List<ExecutionLogRecord> logs(String id) {
        drainOwner(id);
        return tenant("engine_node_attempt", "node_execution_id", id)
                .map(t -> records(t).logs(id))
                .orElse(List.of());
    }

    public void appendContent(String id, ContentChunk c) {
        if (buffer == null) {
            records(nodeTenant(id)).appendContent(id, c);
            return;
        }
        buffer.submit(executionTenant(c.executionId()), new ObservationWrite.AppendContent(id, c));
    }

    public List<ContentChunk> content(String id) {
        drainOwner(id);
        return tenant("engine_node_attempt", "node_execution_id", id)
                .map(t -> records(t).content(id))
                .orElse(List.of());
    }

    public void flush(String id) {
        if (buffer != null) buffer.flush(id);
    }

    public PayloadReference store(String id, Object value) {
        String tenant = executionTenant(id);
        if (buffer == null) return payloads(tenant).store(id, value);
        // 序列化与大小校验在调用线程完成；引用立即可用，读取前会排空所属通道。
        var prepared = MySqlPayloadStore.prepare(id, value, payloadMaxBytes);
        var write =
                new ObservationWrite.StorePayload(
                        UUID.randomUUID().toString(), id, prepared.body(), prepared.size());
        owners.put(write.reference(), id);
        buffer.submit(tenant, write);
        return new PayloadReference(write.reference());
    }

    public Optional<Object> find(PayloadReference ref) {
        drainOwner(ref.value());
        return tenant("engine_payload", "payload_id", ref.value())
                .flatMap(t -> payloads(t).find(ref));
    }

    public Optional<String> executionId(PayloadReference ref) {
        drainOwner(ref.value());
        return tenant("engine_payload", "payload_id", ref.value())
                .flatMap(t -> payloads(t).executionId(ref));
    }

    /** 排空全部写后通道并停止后台写线程；之后的写入在调用线程同步提交。 */
    @Override
    public void close() {
        if (buffer != null) buffer.close();
    }
}
