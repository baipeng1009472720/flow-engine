package com.playrewards.flowengine.adapter.mysql;

/** One tenant mutex serializes row reservations, preventing concurrent count/check overshoot. */
final class MySqlCapacity {
    private MySqlCapacity() {}

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

    static void reserve(MySqlRuntimeDatabase.Tx tx, String tenant, String table, int capacity) {
        long count =
                MySqlRuntimeDatabase.number(
                        tx.row(
                                        "SELECT COUNT(*) AS n FROM "
                                                + table
                                                + " WHERE tenant_id=#{args[0]}",
                                        tenant)
                                .orElseThrow(),
                        "n");
        if (count >= capacity) throw new IllegalStateException("RUNTIME_STORAGE_CAPACITY_EXCEEDED");
    }
}
