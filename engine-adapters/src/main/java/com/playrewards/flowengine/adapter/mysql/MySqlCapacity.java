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
        reserve(tx, tenant, table, 1, capacity);
    }

    /** 预留多行容量，超过上限时抛出 {@code RUNTIME_STORAGE_CAPACITY_EXCEEDED} 使整个事务回滚。 */
    static void reserve(
            MySqlRuntimeDatabase.Tx tx, String tenant, String table, int rows, int capacity) {
        if (!tryReserve(tx, tenant, table, rows, capacity))
            throw new IllegalStateException(EXCEEDED);
    }

    /**
     * 在当前事务内原子预留 {@code rows} 行容量。
     *
     * @param tx 与业务行插入相同的事务；回滚时预留一并撤销
     * @param tenant 已认证租户
     * @param table 受容量约束的引擎表名（适配器内部常量）
     * @param rows 本事务已插入（或将插入）的新行数，0 时直接成功
     * @param capacity 该表的租户行数上限
     * @return 预留成功返回 true；超过上限返回 false 且不改变计数
     */
    static boolean tryReserve(
            MySqlRuntimeDatabase.Tx tx, String tenant, String table, int rows, int capacity) {
        if (rows <= 0) return true;
        if (rows > capacity) return false;
        // 先用不加锁的一致性读确认计数行存在：对不存在的主键执行 UPDATE 会在 RR 下持有间隙锁，
        // 多个首次写入者随后插入同一间隙会互相死锁。
        if (tx.row(
                        "SELECT used FROM engine_capacity_usage WHERE tenant_id=#{args[0]} AND"
                                + " table_name=#{args[1]}",
                        tenant,
                        table)
                .isEmpty()) seed(tx.database(), tenant, table);
        return increment(tx, tenant, table, rows, capacity);
    }

    /**
     * 在独立短事务中以已提交行数播种计数行（新租户或升级后首次写入）。
     *
     * <p>独立事务看不到调用方尚未提交的插入，而调用方随后会对这些行自行递增，因此不会重复计数；
     * 并发播种用 ON DUPLICATE KEY UPDATE 保持幂等。使用非池化控制连接，避免持有工作连接的调用方
     * 再次向已满的工作池借连接而互相等待。
     */
    private static void seed(MySqlRuntimeDatabase db, String tenant, String table) {
        db.controlTransaction(
                seed -> {
                    long existing =
                            MySqlRuntimeDatabase.number(
                                    seed.row(
                                                    "SELECT COUNT(*) AS n FROM "
                                                            + table
                                                            + " WHERE tenant_id=#{args[0]}",
                                                    tenant)
                                            .orElseThrow(),
                                    "n");
                    return seed.update(
                            "INSERT INTO engine_capacity_usage(tenant_id,table_name,used)"
                                    + " VALUES(#{args[0]},#{args[1]},#{args[2]})"
                                    + " ON DUPLICATE KEY UPDATE used=used",
                            tenant,
                            table,
                            existing);
                });
    }

    /**
     * 按实际删除行数释放容量；必须与删除语句处于同一事务。
     *
     * @param deleted 删除语句返回的影响行数
     */
    static void release(MySqlRuntimeDatabase.Tx tx, String tenant, String table, int deleted) {
        if (deleted <= 0) return;
        // 计数行缺失时无需释放：首次预留会按真实行数播种；对缺失主键 UPDATE 还会引入间隙锁。
        if (tx.row(
                        "SELECT used FROM engine_capacity_usage WHERE tenant_id=#{args[0]} AND"
                                + " table_name=#{args[1]}",
                        tenant,
                        table)
                .isEmpty()) return;
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
