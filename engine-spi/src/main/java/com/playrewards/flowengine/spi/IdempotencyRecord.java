package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Objects;

/** Immutable winner record associating one idempotency identity with one execution. */
public record IdempotencyRecord(IdempotencyKey key, String executionId) {

    public IdempotencyRecord {
        Objects.requireNonNull(key, "idempotency key must not be null");
        executionId = JsonValues.requireText(executionId, "idempotency executionId");
    }
}
