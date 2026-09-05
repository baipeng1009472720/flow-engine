package com.playrewards.flowengine.adapter.mysql;

import com.playrewards.flowengine.spi.*;

import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

/** Shared business-resource lease storage, independent from execution scheduler ownership. */
public final class MySqlDistributedLockProvider implements DistributedLockProvider {
    private final MySqlRuntimeDatabase db;

    public MySqlDistributedLockProvider(MySqlRuntimeDatabase db) {
        this.db = db;
    }

    public Optional<LockLease> tryAcquire(LockRequest request) {
        String tenant = tenant(request.lockName()),
                resource = MySqlIdempotencyStore.digest(request.lockName());
        String owner = MySqlIdempotencyStore.digest(request.ownerId()),
                leaseId = UUID.randomUUID().toString();
        if (request.leaseDuration().compareTo(Duration.ofMinutes(5)) > 0)
            throw new IllegalArgumentException("LOCK_DURATION_OUT_OF_BOUNDS");
        return db.transaction(
                tx -> {
                    tx.update(
                            "INSERT IGNORE INTO engine_business_lock"
                                + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]},0,CURRENT_TIMESTAMP(6))",
                            tenant,
                            resource,
                            owner,
                            leaseId);
                    int changed =
                            tx.update(
                                    "UPDATE engine_business_lock SET"
                                        + " owner_digest=#{args[2]},lease_id=#{args[3]},fencing_token=fencing_token+1,expires_at=TIMESTAMPADD(MICROSECOND,#{args[4]},CURRENT_TIMESTAMP(6))"
                                        + " WHERE tenant_id=#{args[0]} AND"
                                        + " resource_digest=#{args[1]} AND"
                                        + " expires_at<=CURRENT_TIMESTAMP(6)",
                                    tenant,
                                    resource,
                                    owner,
                                    leaseId,
                                    Math.max(1, request.leaseDuration().toNanos() / 1000));
                    if (changed != 1) return Optional.empty();
                    var row =
                            tx.row(
                                            "SELECT fencing_token,UNIX_TIMESTAMP(expires_at)*1000"
                                                    + " AS expiry FROM engine_business_lock WHERE"
                                                    + " tenant_id=#{args[0]} AND"
                                                    + " resource_digest=#{args[1]}",
                                            tenant,
                                            resource)
                                    .orElseThrow();
                    return Optional.of(
                            new LockLease(
                                    request.lockName(),
                                    request.ownerId(),
                                    leaseId,
                                    Instant.ofEpochMilli(
                                            MySqlRuntimeDatabase.number(row, "expiry")),
                                    MySqlRuntimeDatabase.number(row, "fencing_token")));
                });
    }

    public boolean release(LockLease lease) {
        return db.transaction(
                tx ->
                        tx.update(
                                        "UPDATE engine_business_lock SET"
                                            + " expires_at=CURRENT_TIMESTAMP(6) WHERE"
                                            + " tenant_id=#{args[0]} AND resource_digest=#{args[1]}"
                                            + " AND owner_digest=#{args[2]} AND lease_id=#{args[3]}"
                                            + " AND fencing_token=#{args[4]} AND"
                                            + " expires_at>CURRENT_TIMESTAMP(6)",
                                        tenant(lease.lockName()),
                                        MySqlIdempotencyStore.digest(lease.lockName()),
                                        MySqlIdempotencyStore.digest(lease.ownerId()),
                                        lease.leaseId(),
                                        lease.fencingToken())
                                == 1);
    }

    /**
     * The core's scoped lock-name contract base64-encodes the tenant; malformed host names fail
     * closed.
     */
    private static String tenant(String name) {
        if (!name.startsWith("tenant:") || !name.contains(":flow:"))
            throw new IllegalArgumentException("LOCK_TENANT_REQUIRED");
        String value =
                new String(
                        Base64.getUrlDecoder().decode(name.substring(7, name.indexOf(":flow:"))),
                        StandardCharsets.UTF_8);
        if (value.isBlank() || value.length() > 128)
            throw new IllegalArgumentException("LOCK_TENANT_INVALID");
        return value;
    }
}
