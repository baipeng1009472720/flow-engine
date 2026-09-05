package com.playrewards.flowengine.adapter.mysql;

import com.playrewards.flowengine.spi.*;

import java.time.*;
import java.util.*;

/** Database-clock leases; expired owners cannot renew themselves or write through an old fence. */
public final class MySqlLeaseRepository implements ExecutionLeaseRepository {
    private final MySqlRuntimeDatabase db;
    private final String tenant;

    public MySqlLeaseRepository(MySqlRuntimeDatabase db, String tenant) {
        this.db = db;
        this.tenant = tenant;
    }

    public Optional<ExecutionLease> acquire(String id, String owner, Duration duration) {
        long micros = micros(duration);
        return db.transaction(
                tx -> {
                    tx.update(
                            "INSERT IGNORE INTO engine_lease VALUES"
                                    + " (#{args[0]},#{args[1]},#{args[2]},0,CURRENT_TIMESTAMP(6))",
                            tenant,
                            id,
                            owner);
                    int changed =
                            tx.update(
                                    "UPDATE engine_lease SET"
                                        + " owner_id=#{args[2]},fencing_token=fencing_token+1,expires_at=TIMESTAMPADD(MICROSECOND,#{args[3]},CURRENT_TIMESTAMP(6))"
                                        + " WHERE tenant_id=#{args[0]} AND execution_id=#{args[1]}"
                                        + " AND expires_at<=CURRENT_TIMESTAMP(6)",
                                    tenant,
                                    id,
                                    owner,
                                    micros);
                    if (changed != 1) return Optional.empty();
                    var row =
                            tx.row(
                                            "SELECT fencing_token,UNIX_TIMESTAMP(expires_at)*1000"
                                                    + " AS expiry FROM engine_lease WHERE"
                                                    + " tenant_id=#{args[0]} AND"
                                                    + " execution_id=#{args[1]}",
                                            tenant,
                                            id)
                                    .orElseThrow();
                    return Optional.of(
                            new ExecutionLease(
                                    tenant,
                                    id,
                                    owner,
                                    MySqlRuntimeDatabase.number(row, "fencing_token"),
                                    Instant.ofEpochMilli(
                                            MySqlRuntimeDatabase.number(row, "expiry"))));
                });
    }

    public boolean heartbeat(ExecutionLease lease, Duration duration) {
        if (!tenant.equals(lease.tenantId())) return false;
        return db.transaction(
                tx ->
                        tx.update(
                                        "UPDATE engine_lease SET"
                                            + " expires_at=TIMESTAMPADD(MICROSECOND,#{args[4]},CURRENT_TIMESTAMP(6))"
                                            + " WHERE tenant_id=#{args[0]} AND"
                                            + " execution_id=#{args[1]} AND owner_id=#{args[2]} AND"
                                            + " fencing_token=#{args[3]} AND"
                                            + " expires_at>CURRENT_TIMESTAMP(6)",
                                        tenant,
                                        lease.executionId(),
                                        lease.owner(),
                                        lease.fencingToken(),
                                        micros(duration))
                                == 1);
    }

    public boolean release(ExecutionLease lease) {
        if (!tenant.equals(lease.tenantId())) return false;
        return db.transaction(
                tx ->
                        tx.update(
                                        "UPDATE engine_lease SET expires_at=CURRENT_TIMESTAMP(6)"
                                            + " WHERE tenant_id=#{args[0]} AND"
                                            + " execution_id=#{args[1]} AND owner_id=#{args[2]} AND"
                                            + " fencing_token=#{args[3]}",
                                        tenant,
                                        lease.executionId(),
                                        lease.owner(),
                                        lease.fencingToken())
                                == 1);
    }

    static void validate(
            MySqlRuntimeDatabase.Tx tx, String tenant, String id, ExecutionLease lease) {
        if (!tenant.equals(lease.tenantId())
                || !id.equals(lease.executionId())
                || tx.row(
                                "SELECT fencing_token FROM engine_lease WHERE tenant_id=#{args[0]}"
                                        + " AND execution_id=#{args[1]} AND owner_id=#{args[2]} AND"
                                        + " fencing_token=#{args[3]} AND"
                                        + " expires_at>CURRENT_TIMESTAMP(6) FOR UPDATE",
                                tenant,
                                id,
                                lease.owner(),
                                lease.fencingToken())
                        .isEmpty()) throw new IllegalStateException("EXECUTION_LEASE_LOST");
    }

    private static long micros(Duration d) {
        if (d.isNegative() || d.isZero() || d.compareTo(Duration.ofMinutes(5)) > 0)
            throw new IllegalArgumentException("lease duration out of bounds");
        return Math.max(1, d.toNanos() / 1000);
    }
}
