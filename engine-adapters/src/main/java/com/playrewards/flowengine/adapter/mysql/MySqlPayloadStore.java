package com.playrewards.flowengine.adapter.mysql;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.spi.*;

import java.time.Duration;
import java.util.*;

/**
 * Opaque tenant-owned bounded values; pinned recovery payloads do not expire with observation data.
 */
public final class MySqlPayloadStore implements PayloadStore {
    private final MySqlRuntimeDatabase db;
    private final String tenant;
    private final int maxBytes;
    private final Duration retention;

    public MySqlPayloadStore(
            MySqlRuntimeDatabase db, String tenant, int maxBytes, Duration retention) {
        if (maxBytes < 1 || maxBytes > 16777215 || retention.isNegative() || retention.isZero())
            throw new IllegalArgumentException("payload bounds invalid");
        this.db = db;
        this.tenant = tenant;
        this.maxBytes = maxBytes;
        this.retention = retention;
    }

    public PayloadReference store(String executionId, Object payload) {
        return store(executionId, payload, false);
    }

    /** Strong-checkpoint values remain retained until explicit execution lifecycle cleanup. */
    public PayloadReference storePinned(String executionId, Object payload) {
        return store(executionId, payload, true);
    }

    @Override
    public PayloadReference storeCheckpoint(String executionId, Object payload) {
        return storePinned(executionId, payload);
    }

    private PayloadReference store(String id, Object payload, boolean pinned) {
        String body = JSON.toJSONString(JsonValues.copyValue(payload, "payload"));
        int size = body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (size > maxBytes) throw new IllegalArgumentException("PAYLOAD_TOO_LARGE");
        String ref = UUID.randomUUID().toString();
        db.capacityTransaction(
                tenant,
                tx -> {
                    MySqlCapacity.lock(tx, tenant);
                    tx.update(
                            "DELETE FROM engine_payload WHERE tenant_id=#{args[0]} AND pinned=FALSE"
                                    + " AND expires_at<CURRENT_TIMESTAMP(6)",
                            tenant);
                    MySqlCapacity.reserve(tx, tenant, "engine_payload", 10000);
                    return tx.update(
                            "INSERT INTO engine_payload"
                                + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]},#{args[4]},#{args[5]},TIMESTAMPADD(MICROSECOND,#{args[6]},CURRENT_TIMESTAMP(6)),#{args[7]})",
                            tenant,
                            ref,
                            id,
                            MySqlIdempotencyStore.digest(body),
                            size,
                            body,
                            retention.toMillis() * 1000,
                            pinned);
                });
        return new PayloadReference(ref);
    }

    public Optional<Object> find(PayloadReference ref) {
        return db.transaction(
                tx ->
                        row(tx, ref)
                                .map(
                                        r -> {
                                            String body = MySqlRuntimeDatabase.text(r, "body");
                                            if (!MySqlIdempotencyStore.digest(body)
                                                    .equals(MySqlRuntimeDatabase.text(r, "digest")))
                                                throw new IllegalStateException(
                                                        "PAYLOAD_DIGEST_MISMATCH");
                                            return JsonValues.copyValue(
                                                    JSON.parse(body), "payload");
                                        }));
    }

    public Optional<String> executionId(PayloadReference ref) {
        return db.transaction(
                tx -> row(tx, ref).map(r -> MySqlRuntimeDatabase.text(r, "execution_id")));
    }

    private Optional<Map<String, Object>> row(MySqlRuntimeDatabase.Tx tx, PayloadReference ref) {
        return tx.row(
                "SELECT execution_id,body,digest FROM engine_payload WHERE tenant_id=#{args[0]} AND"
                    + " payload_id=#{args[1]} AND (pinned=TRUE OR expires_at>CURRENT_TIMESTAMP(6))",
                tenant,
                ref.value());
    }
}
