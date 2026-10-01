package com.playrewards.flowengine.adapter.mysql;

import java.util.*;

/**
 * MySQL 观察记录分批保留清理器。
 *
 * <p>两类清理：1）{@code expires_at} 已过期的执行记录，级联删除其节点尝试、日志、内容流、事件和非固定
 * Payload；2）事件、日志、内容流按创建时间独立过期。每批在独立的 READ COMMITTED 短事务中执行，
 * 单批最多 {@code batchSize} 条执行或行，删除与容量计数递减同事务提交，计数释放按表名排序放在事务
 * 末尾，与写路径保持相同的计数行加锁顺序。多实例并发清理是安全的：删除幂等，计数按实际删除行数递减。
 *
 * <p>不负责：DURABLE 强状态（{@code engine_execution_state}、检查点及固定 Payload），活动执行
 * （{@code expires_at IS NULL}）永不删除。
 */
public final class MySqlObservationRetention {
    private static final int ID_CHUNK = 500;

    private final MySqlRuntimeDatabase db;
    private final MySqlRetentionPolicy policy;

    /**
     * @param db 引擎运行库
     * @param policy 保留期与单批大小
     */
    public MySqlObservationRetention(MySqlRuntimeDatabase db, MySqlRetentionPolicy policy) {
        this.db = Objects.requireNonNull(db);
        this.policy = Objects.requireNonNull(policy);
    }

    /**
     * 对所有已出现过的租户各执行至多 {@code maxBatches} 批清理。
     *
     * @param maxBatches 每租户每类清理的最大批数，限制单次调度的总耗时
     * @return 删除的执行数与其它行数
     */
    public PurgeResult purgeExpired(int maxBatches) {
        if (maxBatches < 1) throw new IllegalArgumentException("maxBatches must be positive");
        List<String> tenants =
                db.transaction(
                        tx ->
                                tx
                                        .rows(
                                                "SELECT DISTINCT tenant_id FROM"
                                                        + " engine_capacity_usage")
                                        .stream()
                                        .map(r -> MySqlRuntimeDatabase.text(r, "tenant_id"))
                                        .toList());
        var total = new PurgeResult(0, 0);
        for (String tenant : tenants) total = total.plus(purgeTenant(tenant, maxBatches));
        return total;
    }

    /** 以默认 20 批上限执行一次全租户清理。 */
    public PurgeResult purgeExpired() {
        return purgeExpired(20);
    }

    /**
     * 清理一个租户的过期记录；写路径在容量满时以 {@code maxBatches=1} 调用。
     *
     * @param tenant 已认证租户
     * @param maxBatches 每类清理的最大批数
     * @return 删除统计
     */
    PurgeResult purgeTenant(String tenant, int maxBatches) {
        int executions = 0, rows = 0;
        for (int batch = 0; batch < maxBatches; batch++) {
            var deleted = db.readCommittedTransaction(tx -> purgeExecutionBatch(tx, tenant));
            executions += deleted.executions();
            rows += deleted.rows();
            if (deleted.executions() < policy.batchSize()) break;
        }
        for (String table : List.of("engine_content", "engine_event", "engine_log")) {
            for (int batch = 0; batch < maxBatches; batch++) {
                int deleted = db.readCommittedTransaction(tx -> purgeRows(tx, tenant, table));
                rows += deleted;
                if (deleted < policy.batchSize()) break;
            }
        }
        rows += db.readCommittedTransaction(tx -> purgePayloads(tx, tenant));
        return new PurgeResult(executions, rows);
    }

    /**
     * 容量压力淘汰：在没有过期记录可清理时，按 {@code expires_at} 从早到晚淘汰一批已结束的执行。
     *
     * <p>只选择终态（SUCCEEDED/FAILED/CANCELLED）且带过期时间的执行，EPHEMERAL 终态的过期时间更早
     * 因而优先被淘汰；活动执行、DURABLE 非终态与 WAITING_MANUAL 永不淘汰，全部容量被它们占用时
     * 写入仍按硬上限拒绝。
     *
     * @param tenant 已认证租户
     * @param limit 本批最多淘汰的执行数
     * @return 删除统计
     */
    PurgeResult evictOldestTerminal(String tenant, int limit) {
        return db.readCommittedTransaction(
                tx ->
                        deleteExecutions(
                                tx,
                                tenant,
                                ids(
                                        tx,
                                        "SELECT execution_id FROM engine_execution WHERE"
                                                + " tenant_id=#{args[0]} AND expires_at IS NOT NULL"
                                                + " AND status IN ('SUCCEEDED','FAILED','CANCELLED')"
                                                + " ORDER BY expires_at LIMIT #{args[1]}",
                                        tenant,
                                        limit),
                                "status IN ('SUCCEEDED','FAILED','CANCELLED')"));
    }

    private static List<String> ids(
            MySqlRuntimeDatabase.Tx tx, String sql, String tenant, int limit) {
        return tx.rows(sql, tenant, limit).stream()
                .map(r -> MySqlRuntimeDatabase.text(r, "execution_id"))
                .toList();
    }

    private PurgeResult purgeExecutionBatch(MySqlRuntimeDatabase.Tx tx, String tenant) {
        return deleteExecutions(
                tx,
                tenant,
                ids(
                        tx,
                        "SELECT execution_id FROM engine_execution WHERE tenant_id=#{args[0]} AND"
                                + " expires_at<CURRENT_TIMESTAMP(6) ORDER BY expires_at LIMIT"
                                + " #{args[1]}",
                        tenant,
                        policy.batchSize()),
                "expires_at<CURRENT_TIMESTAMP(6)");
    }

    /**
     * 级联删除一批执行及其观察数据。
     *
     * @param guard 删除执行行时再次校验的条件（适配器内部常量），两步之间状态变化的执行不删除执行行
     */
    private static PurgeResult deleteExecutions(
            MySqlRuntimeDatabase.Tx tx, String tenant, List<String> ids, String guard) {
        if (ids.isEmpty()) return new PurgeResult(0, 0);
        var released = new TreeMap<String, Integer>();
        List<String> nodes = new ArrayList<>();
        for (var chunk : chunks(ids))
            tx.rows(
                            "SELECT node_execution_id FROM engine_node_attempt WHERE"
                                    + " tenant_id=#{args[0]} AND execution_id IN ("
                                    + placeholders(chunk.size(), 1)
                                    + ")",
                            args(tenant, chunk))
                    .forEach(r -> nodes.add(MySqlRuntimeDatabase.text(r, "node_execution_id")));
        for (var chunk : chunks(nodes)) {
            for (String table : List.of("engine_log", "engine_content", "engine_node_attempt"))
                released.merge(
                        table,
                        tx.update(
                                "DELETE FROM "
                                        + table
                                        + " WHERE tenant_id=#{args[0]} AND node_execution_id IN ("
                                        + placeholders(chunk.size(), 1)
                                        + ")",
                                args(tenant, chunk)),
                        Integer::sum);
        }
        int executions = 0;
        for (var chunk : chunks(ids)) {
            released.merge(
                    "engine_event",
                    tx.update(
                            "DELETE FROM engine_event WHERE tenant_id=#{args[0]} AND execution_id"
                                    + " IN ("
                                    + placeholders(chunk.size(), 1)
                                    + ")",
                            args(tenant, chunk)),
                    Integer::sum);
            // 固定（检查点）Payload 属于强状态，只删除观察侧的非固定值。
            released.merge(
                    "engine_payload",
                    tx.update(
                            "DELETE FROM engine_payload WHERE tenant_id=#{args[0]} AND pinned=FALSE"
                                    + " AND execution_id IN ("
                                    + placeholders(chunk.size(), 1)
                                    + ")",
                            args(tenant, chunk)),
                    Integer::sum);
            // 再次校验选择条件：两步之间被续期或改变状态的记录不删除执行行本身。
            int deleted =
                    tx.update(
                            "DELETE FROM engine_execution WHERE tenant_id=#{args[0]} AND "
                                    + guard
                                    + " AND execution_id IN ("
                                    + placeholders(chunk.size(), 1)
                                    + ")",
                            args(tenant, chunk));
            released.merge("engine_execution", deleted, Integer::sum);
            executions += deleted;
        }
        int rows = 0;
        for (var entry : released.entrySet()) {
            MySqlCapacity.release(tx, tenant, entry.getKey(), entry.getValue());
            if (!entry.getKey().equals("engine_execution")) rows += entry.getValue();
        }
        return new PurgeResult(executions, rows);
    }

    private int purgeRows(MySqlRuntimeDatabase.Tx tx, String tenant, String table) {
        int deleted =
                tx.update(
                        "DELETE FROM "
                                + table
                                + " WHERE tenant_id=#{args[0]} AND"
                                + " created_at<TIMESTAMPADD(MICROSECOND,-#{args[1]},CURRENT_TIMESTAMP(6))"
                                + " ORDER BY created_at LIMIT #{args[2]}",
                        tenant,
                        policy.rows(table).toMillis() * 1000,
                        policy.batchSize());
        MySqlCapacity.release(tx, tenant, table, deleted);
        return deleted;
    }

    /**
     * 回收一个租户的一批已过期非固定 Payload；供 Payload 写入在容量满时调用。
     *
     * @return 删除行数
     */
    static int purgeExpiredPayloads(MySqlRuntimeDatabase db, String tenant) {
        return db.readCommittedTransaction(tx -> purgePayloads(tx, tenant, 500));
    }

    private int purgePayloads(MySqlRuntimeDatabase.Tx tx, String tenant) {
        return purgePayloads(tx, tenant, policy.batchSize());
    }

    private static int purgePayloads(MySqlRuntimeDatabase.Tx tx, String tenant, int batchSize) {
        int deleted =
                tx.update(
                        "DELETE FROM engine_payload WHERE tenant_id=#{args[0]} AND pinned=FALSE AND"
                                + " expires_at<CURRENT_TIMESTAMP(6) ORDER BY expires_at LIMIT"
                                + " #{args[1]}",
                        tenant,
                        batchSize);
        MySqlCapacity.release(tx, tenant, "engine_payload", deleted);
        return deleted;
    }

    private static List<List<String>> chunks(List<String> values) {
        List<List<String>> chunks = new ArrayList<>();
        for (int start = 0; start < values.size(); start += ID_CHUNK)
            chunks.add(values.subList(start, Math.min(values.size(), start + ID_CHUNK)));
        return chunks;
    }

    private static String placeholders(int count, int offset) {
        var text = new StringJoiner(",");
        for (int index = 0; index < count; index++)
            text.add("#{args[" + (offset + index) + "]}");
        return text.toString();
    }

    private static Object[] args(String tenant, List<String> ids) {
        Object[] values = new Object[ids.size() + 1];
        values[0] = tenant;
        for (int index = 0; index < ids.size(); index++) values[index + 1] = ids.get(index);
        return values;
    }

    /**
     * 一次清理的删除统计，不含任何租户或执行身份。
     *
     * @param executions 删除的执行记录数
     * @param rows 删除的节点、事件、日志、内容和 Payload 行数
     */
    public record PurgeResult(int executions, int rows) {
        PurgeResult plus(PurgeResult other) {
            return new PurgeResult(executions + other.executions, rows + other.rows);
        }

        /** @return 删除的总行数 */
        public int total() {
            return executions + rows;
        }
    }
}
