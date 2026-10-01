package com.playrewards.flowengine.adapter.mysql;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.spi.*;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

/**
 * 单租户观察表适配器：终态记录与终态事件在同一事务提交后才会被发布。
 *
 * <p>写入热路径只做行级读写与增量容量预留，不再在每次写入时执行过期删除或 COUNT(*)，也不再获取租户
 * 互斥行锁；同一执行的事件顺序由 {@code engine_execution} 行上的 {@code FOR UPDATE} 游标校验保证。
 * 容量预留统一放在事务末尾并按表名排序，缩短计数行锁持有时间并固定加锁顺序。容量已满时先清理该租户
 * 的过期记录，仍不足再淘汰最早的终态执行；只有活动执行占满容量时才返回
 * {@code RUNTIME_STORAGE_CAPACITY_EXCEEDED}。
 *
 * <p>不负责：周期性保留清理（见 {@link MySqlObservationRetention}）、跨租户路由与授权、强一致检查点。
 */
public final class MySqlObservationStore implements ExecutionRecordStore {
    private static final int DEFAULT_CAPACITY = 10000;
    private static final Duration DEFAULT_ROW_RETENTION = Duration.ofHours(1);
    /** 容量压力下单轮淘汰的终态执行数与最大轮数，限制写路径上的额外延迟。 */
    private static final int EVICTION_BATCH = 50, EVICTION_ROUNDS = 3;

    private final MySqlRuntimeDatabase db;
    private final String tenant;
    private final int capacity;
    private final Map<String, Integer> capacities;
    private final MySqlRetentionPolicy policy;

    public MySqlObservationStore(MySqlRuntimeDatabase db, String tenant) {
        this(db, tenant, DEFAULT_CAPACITY, DEFAULT_ROW_RETENTION);
    }

    /** 兼容旧参数：统一行数上限，事件/日志/内容共用同一保留期。 */
    public MySqlObservationStore(
            MySqlRuntimeDatabase db, String tenant, int capacity, Duration retention) {
        this(
                db,
                tenant,
                capacity,
                Map.of(),
                MySqlRetentionPolicy.defaults().withRowRetention(retention, retention, retention));
    }

    /** 兼容旧参数：按表配置行数上限与事件/日志/内容保留期，执行记录使用默认保留策略。 */
    public MySqlObservationStore(
            MySqlRuntimeDatabase db,
            String tenant,
            Map<String, Integer> capacities,
            Map<String, Duration> retentions) {
        this(
                db,
                tenant,
                DEFAULT_CAPACITY,
                capacities,
                MySqlRetentionPolicy.defaults()
                        .withRowRetention(
                                retentions.getOrDefault("engine_event", DEFAULT_ROW_RETENTION),
                                retentions.getOrDefault("engine_log", DEFAULT_ROW_RETENTION),
                                retentions.getOrDefault("engine_content", DEFAULT_ROW_RETENTION)));
    }

    /**
     * 按表配置行数上限，并使用完整保留策略。
     *
     * @param capacities 表名到租户行数上限；缺省表使用 10000
     * @param policy 执行与行级保留策略
     */
    public MySqlObservationStore(
            MySqlRuntimeDatabase db,
            String tenant,
            Map<String, Integer> capacities,
            MySqlRetentionPolicy policy) {
        this(db, tenant, DEFAULT_CAPACITY, capacities, policy);
    }

    private MySqlObservationStore(
            MySqlRuntimeDatabase db,
            String tenant,
            int capacity,
            Map<String, Integer> capacities,
            MySqlRetentionPolicy policy) {
        if (capacity < 1 || capacities.values().stream().anyMatch(v -> v < 1))
            throw new IllegalArgumentException("observation bounds invalid");
        this.db = Objects.requireNonNull(db);
        this.tenant = Objects.requireNonNull(tenant);
        this.capacity = capacity;
        this.capacities = Map.copyOf(capacities);
        this.policy = Objects.requireNonNull(policy);
    }

    private int capacity(String table) {
        return capacities.getOrDefault(table, capacity);
    }

    @Override
    public void saveExecution(ExecutionRecord record) {
        owned(record);
        write(
                tx -> {
                    var ledger = new Ledger();
                    save(tx, record, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    private void owned(ExecutionRecord record) {
        if (!tenant.equals(record.flowIdentity().tenantId()))
            throw new SecurityException("EXECUTION_TENANT_MISMATCH");
    }

    private void save(MySqlRuntimeDatabase.Tx tx, ExecutionRecord r, Ledger ledger) {
        boolean exists =
                tx.row(
                                "SELECT 1 AS x FROM engine_execution WHERE tenant_id=#{args[0]} AND"
                                        + " execution_id=#{args[1]}",
                                tenant,
                                r.executionId())
                        .isPresent();
        tx.update(
                "INSERT INTO engine_execution(tenant_id,execution_id,status,event_cursor,body,expires_at)"
                        + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]},#{args[4]},"
                        + "IF(#{args[5]}<0,NULL,TIMESTAMPADD(MICROSECOND,#{args[5]},CURRENT_TIMESTAMP(6))))"
                        + " ON DUPLICATE KEY UPDATE status=VALUES(status),body=VALUES(body),"
                        + "expires_at=VALUES(expires_at),updated_at=CURRENT_TIMESTAMP(6)",
                tenant,
                r.executionId(),
                r.status().name(),
                r.eventCursor(),
                json(r),
                expiryMicros(r));
        if (!exists) ledger.add("engine_execution", 1);
    }

    /** 计算执行记录的保留期（微秒）；返回 -1 表示活动 DURABLE 或待人工处置，不得按保留期删除。 */
    private long expiryMicros(ExecutionRecord r) {
        boolean ephemeral = r.mode() != ExecutionMode.DURABLE;
        Duration retention =
                switch (r.status()) {
                    case SUCCEEDED, FAILED, CANCELLED ->
                            ephemeral ? policy.ephemeralExecutions() : policy.durableExecutions();
                    case WAITING_MANUAL -> null;
                    default -> ephemeral ? policy.staleExecutions() : null;
                };
        return retention == null ? -1 : retention.toMillis() * 1000;
    }

    @Override
    public Optional<ExecutionRecord> execution(String id) {
        return db.transaction(
                tx -> one(tx, "engine_execution", "execution_id", id, ExecutionRecord.class));
    }

    @Override
    public void commitTerminal(ExecutionRecord record, ExecutionEvent event) {
        owned(record);
        if (!record.executionId().equals(event.executionId()) || record.status() != event.status())
            throw new IllegalStateException("TERMINAL_EVENT_MISMATCH");
        write(
                tx -> {
                    var ledger = new Ledger();
                    save(tx, record, ledger);
                    append(tx, event, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    @Override
    public void append(ExecutionEvent event) {
        write(
                tx -> {
                    var ledger = new Ledger();
                    append(tx, event, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    @Override
    public void commitTerminal(ExecutionRecord record, ExecutionEvent event, ExecutionLease lease) {
        owned(record);
        if (!record.executionId().equals(event.executionId())
                || record.status() != event.status()
                || !record.rootExecutionId().equals(lease.executionId()))
            throw new IllegalStateException("TERMINAL_EVENT_MISMATCH");
        write(
                tx -> {
                    MySqlLeaseRepository.validate(tx, tenant, lease.executionId(), lease);
                    var ledger = new Ledger();
                    save(tx, record, ledger);
                    append(tx, event, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    private void append(MySqlRuntimeDatabase.Tx tx, ExecutionEvent e, Ledger ledger) {
        // 执行行锁串行化同一执行的事件游标；不同执行之间不再共享租户级互斥。
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
        ledger.add("engine_event", 1);
    }

    @Override
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

    @Override
    public void saveNode(NodeExecutionRecord r) {
        write(
                tx -> {
                    var ledger = new Ledger();
                    saveNode(tx, r, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    private void saveNode(MySqlRuntimeDatabase.Tx tx, NodeExecutionRecord r, Ledger ledger) {
        if (tx.row(
                        "SELECT 1 AS x FROM engine_execution WHERE tenant_id=#{args[0]} AND"
                                + " execution_id=#{args[1]}",
                        tenant,
                        r.executionId())
                .isEmpty()) throw new IllegalStateException("EXECUTION_NOT_FOUND");
        String scope = MySqlIdempotencyStore.digest(r.scopePath());
        var existing =
                tx.row(
                        "SELECT node_execution_id FROM engine_node_attempt WHERE"
                                + " tenant_id=#{args[0]} AND execution_id=#{args[1]} AND"
                                + " node_id=#{args[2]} AND scope_digest=#{args[3]} AND"
                                + " attempt=#{args[4]}",
                        tenant,
                        r.executionId(),
                        r.nodeId(),
                        scope,
                        r.attempt());
        if (existing.isPresent()
                && !r.nodeExecutionId()
                        .equals(MySqlRuntimeDatabase.text(existing.get(), "node_execution_id")))
            throw new IllegalStateException("NODE_ATTEMPT_IDENTITY_CONFLICT");
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
                scope,
                r.attempt(),
                json(r));
        if (existing.isEmpty()) ledger.add("engine_node_attempt", 1);
    }

    @Override
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

    @Override
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

    @Override
    public void appendLog(ExecutionLogRecord r) {
        write(
                tx -> {
                    var ledger = new Ledger();
                    appendLog(tx, r, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    private void appendLog(MySqlRuntimeDatabase.Tx tx, ExecutionLogRecord r, Ledger ledger) {
        requireNode(tx, r.nodeExecutionId());
        insertLog(tx, r, ledger);
    }

    private void insertLog(MySqlRuntimeDatabase.Tx tx, ExecutionLogRecord r, Ledger ledger) {
        tx.update(
                "INSERT INTO engine_log(tenant_id,node_execution_id,sequence,body)"
                        + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                tenant,
                r.nodeExecutionId(),
                r.sequence(),
                json(r));
        ledger.add("engine_log", 1);
    }

    @Override
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

    @Override
    public void appendContent(String id, ContentChunk chunk) {
        write(
                tx -> {
                    var ledger = new Ledger();
                    appendContent(tx, id, chunk, ledger);
                    ledger.reserve(tx);
                    return null;
                });
    }

    private void appendContent(
            MySqlRuntimeDatabase.Tx tx, String id, ContentChunk chunk, Ledger ledger) {
        requireNode(tx, id);
        insertContent(tx, id, chunk, ledger);
    }

    private void insertContent(
            MySqlRuntimeDatabase.Tx tx, String id, ContentChunk chunk, Ledger ledger) {
        tx.update(
                "INSERT INTO engine_content(tenant_id,node_execution_id,sequence,body)"
                        + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                tenant,
                id,
                chunk.sequence(),
                json(chunk));
        ledger.add("engine_content", 1);
    }

    private void requireNode(MySqlRuntimeDatabase.Tx tx, String nodeExecutionId) {
        if (tx.row(
                        "SELECT 1 AS x FROM engine_node_attempt WHERE tenant_id=#{args[0]} AND"
                                + " node_execution_id=#{args[1]}",
                        tenant,
                        nodeExecutionId)
                .isEmpty()) throw new IllegalStateException("NODE_NOT_FOUND");
    }

    @Override
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

    /**
     * 在一个事务内按顺序应用一批写后缓冲的观察写入。
     *
     * <p>同一批内对同一执行/节点尝试的多次快照只写最后一版，位置保持在首次出现处（后续写入只依赖其存在）；
     * 同一执行的事件只锁定并读取一次游标，逐条校验严格递增后在事务末尾回写。所有新增行在末尾一次性
     * 按表排序预留容量。任一写入失败则整批回滚，由调用方逐条重放定位失败项。
     *
     * @param writes 同一执行通道内按提交顺序排列的写入
     * @param payloadRetention 非固定观察 Payload 的保留期
     * @throws IllegalStateException 顺序冲突、引用缺失或容量已满
     */
    void applyBatch(List<ObservationWrite> writes, Duration payloadRetention) {
        Map<String, ExecutionRecord> lastExecution = new HashMap<>();
        Map<String, NodeExecutionRecord> lastNode = new HashMap<>();
        for (var write : writes) {
            if (write instanceof ObservationWrite.SaveExecution save) {
                owned(save.record());
                lastExecution.put(save.record().executionId(), save.record());
            } else if (write instanceof ObservationWrite.SaveNode node)
                lastNode.put(node.record().nodeExecutionId(), node.record());
        }
        write(
                tx -> {
                    var ledger = new Ledger();
                    Set<String> savedExecutions = new HashSet<>(), savedNodes = new HashSet<>();
                    Map<String, Long> cursors = new LinkedHashMap<>();
                    for (var write : writes) {
                        switch (write) {
                            case ObservationWrite.SaveExecution save -> {
                                String id = save.record().executionId();
                                if (savedExecutions.add(id))
                                    save(tx, lastExecution.get(id), ledger);
                            }
                            case ObservationWrite.SaveNode node -> {
                                String id = node.record().nodeExecutionId();
                                if (savedNodes.add(id)) saveNode(tx, lastNode.get(id), ledger);
                            }
                            case ObservationWrite.AppendEvent append ->
                                    appendBuffered(tx, append.event(), cursors, ledger);
                            case ObservationWrite.AppendLog log -> {
                                if (!savedNodes.contains(log.record().nodeExecutionId()))
                                    requireNode(tx, log.record().nodeExecutionId());
                                insertLog(tx, log.record(), ledger);
                            }
                            case ObservationWrite.AppendContent content -> {
                                if (!savedNodes.contains(content.nodeExecutionId()))
                                    requireNode(tx, content.nodeExecutionId());
                                insertContent(tx, content.nodeExecutionId(), content.chunk(), ledger);
                            }
                            case ObservationWrite.StorePayload payload -> {
                                MySqlPayloadStore.insert(tx, tenant, payload, false, payloadRetention);
                                ledger.add("engine_payload", 1);
                            }
                        }
                    }
                    for (var cursor : cursors.entrySet())
                        tx.update(
                                "UPDATE engine_execution SET event_cursor=#{args[2]} WHERE"
                                        + " tenant_id=#{args[0]} AND execution_id=#{args[1]}",
                                tenant,
                                cursor.getKey(),
                                cursor.getValue());
                    ledger.reserve(tx);
                    return null;
                });
    }

    private void appendBuffered(
            MySqlRuntimeDatabase.Tx tx, ExecutionEvent e, Map<String, Long> cursors, Ledger ledger) {
        Long cursor = cursors.get(e.executionId());
        if (cursor == null)
            cursor =
                    MySqlRuntimeDatabase.number(
                            tx.row(
                                            "SELECT event_cursor FROM engine_execution WHERE"
                                                    + " tenant_id=#{args[0]} AND"
                                                    + " execution_id=#{args[1]} FOR UPDATE",
                                            tenant,
                                            e.executionId())
                                    .orElseThrow(
                                            () -> new IllegalStateException("EXECUTION_NOT_FOUND")),
                            "event_cursor");
        if (e.sequence() <= cursor) throw new IllegalStateException("EVENT_SEQUENCE_CONFLICT");
        tx.update(
                "INSERT INTO engine_event(tenant_id,execution_id,sequence,body)"
                        + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                tenant,
                e.executionId(),
                e.sequence(),
                json(e));
        cursors.put(e.executionId(), e.sequence());
        ledger.add("engine_event", 1);
    }

    @Override
    public void flush(String id) {
        /* Each write has already committed synchronously. */
    }

    /**
     * 执行一次观察写事务；容量已满时先清理本租户过期记录，仍不足再淘汰最早的终态执行后完整重试。
     *
     * <p>重试是整个事务重放而非局部补写，因此不会产生部分成功；活动执行占满容量、没有可回收记录时
     * 返回原始容量错误，保持硬上限语义。
     */
    private <T> T write(Function<MySqlRuntimeDatabase.Tx, T> action) {
        try {
            return db.transaction(action);
        } catch (IllegalStateException failure) {
            if (!MySqlCapacity.EXCEEDED.equals(failure.getMessage())) throw failure;
        }
        var retention = new MySqlObservationRetention(db, policy);
        // 先清理过期记录；仍无可回收时再按最早过期淘汰一小批终态执行，最多三轮后保持硬上限。
        boolean freed = retention.purgeTenant(tenant, 1).total() > 0;
        for (int round = 0; ; round++) {
            if (!freed) {
                if (round >= EVICTION_ROUNDS
                        || retention.evictOldestTerminal(tenant, EVICTION_BATCH).total() == 0)
                    throw new IllegalStateException(MySqlCapacity.EXCEEDED);
            }
            try {
                return db.transaction(action);
            } catch (IllegalStateException failure) {
                if (!MySqlCapacity.EXCEEDED.equals(failure.getMessage())) throw failure;
                freed = false;
            }
        }
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

    /**
     * 事务内容量账本：先完成所有行写入，最后按表名排序一次性预留，保证所有写事务以相同顺序获取计数行锁。
     */
    final class Ledger {
        private final TreeMap<String, Integer> rows = new TreeMap<>();

        void add(String table, int count) {
            rows.merge(table, count, Integer::sum);
        }

        void reserve(MySqlRuntimeDatabase.Tx tx) {
            for (var entry : rows.entrySet())
                MySqlCapacity.reserve(
                        tx,
                        tenant,
                        entry.getKey(),
                        entry.getValue(),
                        entry.getKey().equals("engine_payload")
                                ? MySqlPayloadStore.CAPACITY
                                : capacity(entry.getKey()));
        }
    }
}
