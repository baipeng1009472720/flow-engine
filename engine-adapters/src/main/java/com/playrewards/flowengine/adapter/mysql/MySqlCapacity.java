package com.playrewards.flowengine.adapter.mysql;

/**
 * 租户级存储容量边界。
 *
 * <p>容量用 {@code engine_capacity_usage(tenant_id, table_name)} 增量计数：预留是一条带上限条件的
 * {@code UPDATE used=used+k WHERE used+k<=cap}，与业务行插入处于同一事务；删除按实际删除行数递减。
 * InnoDB 对计数行加排他锁直到提交，因此任意并发下提交点上的 used 等于真实行数，且永不超过上限，
 * 不存在“先 COUNT 再判断”的竞态，也无需扫描整张表。
 *
 * <p>不负责：选择删除哪些过期记录（由调用方或保留清理器决定），也不负责跨表的级联删除。
 */
final class MySqlCapacity {
    static final String EXCEEDED = "RUNTIME_STORAGE_CAPACITY_EXCEEDED";

    private MySqlCapacity() {}

    /**
     * 获取租户互斥行锁。仅 Payload/检查点清理路径仍需要它来固定加锁顺序，观察写入不再依赖它。
     *
     * @param tx 当前事务
     * @param tenant 已认证租户
     */
    static void lock(MySqlRuntimeDatabase.Tx tx, String tenant) {
        // A duplicate-key UPDATE takes the tenant row's exclusive lock directly. INSERT IGNORE
        // followed by FOR UPDATE can deadlock when many first-time writers all upgrade their locks.
        tx.update(
                "INSERT INTO engine_capacity VALUES(#{args[0]})"
                        + " ON DUPLICATE KEY UPDATE tenant_id=VALUES(tenant_id)",
                tenant);
        tx.row(
                        "SELECT tenant_id FROM engine_capacity WHERE tenant_id=#{args[0]} FOR"
                                + " UPDATE",
                        tenant)
                .orElseThrow();
    }

    /** 预留一行容量；语义见 {@link #tryReserve}。 */
    static void reserve(MySqlRuntimeDatabase.Tx tx, String tenant, String table, int capacity) {
        if (!tryReserve(tx, tenant, table, 1, capacity))
            throw new IllegalStateException(EXCEEDED);
    }

    /**
     * 在当前事务内原子预留 {@code rows} 行容量。
     *
     * @param tx 与业务行插入相同的事务；回滚时预留一并撤销
     * @param tenant 已认证租户
     * @param table 受容量约束的引擎表名（适配器内部常量）
     * @param rows 本事务将新增的行数，0 时直接成功
     * @param capacity 该表的租户行数上限
     * @return 预留成功返回 true；超过上限返回 false 且不改变计数
     */
    static boolean tryReserve(
            MySqlRuntimeDatabase.Tx tx, String tenant, String table, int rows, int capacity) {
        if (rows <= 0) return true;
        if (rows > capacity) return false;
        if (increment(tx, tenant, table, rows, capacity)) return true;
        // 计数行不存在时（新租户或升级后首次写入）以真实行数播种一次；INSERT IGNORE 保证并发只有一个生效。
        if (tx.row(
                        "SELECT used FROM engine_capacity_usage WHERE tenant_id=#{args[0]} AND"
                                + " table_name=#{args[1]}",
                        tenant,
                        table)
                .isEmpty()) {
            tx.update(
                    "INSERT IGNORE INTO engine_capacity_usage(tenant_id,table_name,used) SELECT"
                            + " #{args[0]},#{args[1]},COUNT(*) FROM "
                            + table
                            + " WHERE tenant_id=#{args[0]}",
                    tenant,
                    table);
            return increment(tx, tenant, table, rows, capacity);
        }
        return false;
    }

    /**
     * 按实际删除行数释放容量；必须与删除语句处于同一事务。
     *
     * @param deleted 删除语句返回的影响行数
     */
    static void release(MySqlRuntimeDatabase.Tx tx, String tenant, String table, int deleted) {
        if (deleted <= 0) return;
        tx.update(
                "UPDATE engine_capacity_usage SET used=GREATEST(used-#{args[2]},0) WHERE"
                        + " tenant_id=#{args[0]} AND table_name=#{args[1]}",
                tenant,
                table,
                deleted);
    }

    private static boolean increment(
            MySqlRuntimeDatabase.Tx tx, String tenant, String table, int rows, int capacity) {
        return tx.update(
                        "UPDATE engine_capacity_usage SET used=used+#{args[2]} WHERE"
                                + " tenant_id=#{args[0]} AND table_name=#{args[1]} AND"
                                + " used+#{args[2]}<=#{args[3]}",
                        tenant,
                        table,
                        rows,
                        capacity)
                == 1;
    }
}
