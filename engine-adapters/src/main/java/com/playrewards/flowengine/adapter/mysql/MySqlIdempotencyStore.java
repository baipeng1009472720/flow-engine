package com.playrewards.flowengine.adapter.mysql;

import com.playrewards.flowengine.spi.*;

import java.util.*;

/** Unique tenant/key digest atomically retains the first execution association across processes. */
public final class MySqlIdempotencyStore implements IdempotencyStore {
    private final MySqlRuntimeDatabase db;

    public MySqlIdempotencyStore(MySqlRuntimeDatabase db) {
        this.db = db;
    }

    public Optional<IdempotencyRecord> find(IdempotencyKey key) {
        return db.transaction(tx -> find(tx, key));
    }

    private Optional<IdempotencyRecord> find(MySqlRuntimeDatabase.Tx tx, IdempotencyKey key) {
        return tx.row(
                        "SELECT execution_id FROM engine_idempotency WHERE tenant_id=#{args[0]} AND"
                                + " key_digest=#{args[1]}",
                        key.tenantId(),
                        digest(key.value()))
                .map(r -> new IdempotencyRecord(key, MySqlRuntimeDatabase.text(r, "execution_id")));
    }

    public IdempotencyRecord claim(IdempotencyRecord value) {
        return db.transaction(
                tx -> {
                    tx.update(
                            "INSERT IGNORE INTO engine_idempotency"
                                    + " VALUES(#{args[0]},#{args[1]},#{args[2]})",
                            value.key().tenantId(),
                            digest(value.key().value()),
                            value.executionId());
                    return find(tx, value.key()).orElseThrow();
                });
    }

    static String digest(String value) {
        try {
            return HexFormat.of()
                    .formatHex(
                            java.security.MessageDigest.getInstance("SHA-256")
                                    .digest(
                                            value.getBytes(
                                                    java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA256_UNAVAILABLE");
        }
    }
}
