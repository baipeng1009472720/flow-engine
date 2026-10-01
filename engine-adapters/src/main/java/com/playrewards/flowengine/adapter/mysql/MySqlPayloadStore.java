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
    /** 每租户 Payload 行数上限。 */
    static final int CAPACITY = 10000;

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
        var prepared = prepare(id, payload, maxBytes);
        var write =
                new ObservationWrite.StorePayload(
                        UUID.randomUUID().toString(), id, prepared.body(), prepared.size());
        try {
            insertWithTenantLock(write, pinned);
        } catch (IllegalStateException full) {
            // 容量满时先回收本租户已过期的非固定 Payload，再完整重试一次；仍满则保持硬上限。
            if (!MySqlCapacity.EXCEEDED.equals(full.getMessage())
                    || MySqlObservationRetention.purgeExpiredPayloads(db, tenant) == 0) throw full;
            insertWithTenantLock(write, pinned);
        }
        return new PayloadReference(write.reference());
    }

    private void insertWithTenantLock(ObservationWrite.StorePayload write, boolean pinned) {
        db.capacityTransaction(
                tenant,
                tx -> {
                    // 固定 Payload 与检查点清理共享租户互斥，保持既有加锁顺序；过期清理已移出热路径。
                    MySqlCapacity.lock(tx, tenant);
                    insert(tx, tenant, write, pinned, retention);
                    MySqlCapacity.reserve(tx, tenant, "engine_payload", CAPACITY);
                    return null;
                });
    }

    /**
     * 在调用线程完成深拷贝、序列化和大小校验，使超限立即以 {@code PAYLOAD_TOO_LARGE} 拒绝。
     *
     * @return 序列化文本及其 UTF-8 字节数
     */
    static Prepared prepare(String executionId, Object payload, int maxBytes) {
        Objects.requireNonNull(executionId, "executionId");
        String body = JSON.toJSONString(JsonValues.copyValue(payload, "payload"));
        int size = body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        if (size > maxBytes) throw new IllegalArgumentException("PAYLOAD_TOO_LARGE");
        return new Prepared(body, size);
    }

    /** 已校验的 Payload 文本。 */
    record Prepared(String body, int size) {}

    /** 插入一行 Payload；调用方负责在同一事务内预留 {@code engine_payload} 容量。 */
    static void insert(
            MySqlRuntimeDatabase.Tx tx,
            String tenant,
            ObservationWrite.StorePayload write,
            boolean pinned,
            Duration retention) {
        tx.update(
                "INSERT INTO engine_payload"
                    + " VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]},#{args[4]},#{args[5]},TIMESTAMPADD(MICROSECOND,#{args[6]},CURRENT_TIMESTAMP(6)),#{args[7]})",
                tenant,
                write.reference(),
                write.executionId(),
                MySqlIdempotencyStore.digest(write.body()),
                write.size(),
                write.body(),
                retention.toMillis() * 1000,
                pinned);
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
