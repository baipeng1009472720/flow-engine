package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;

/** Stable tenant-scoped identity used to prevent duplicate execution starts. */
public record IdempotencyKey(String tenantId, String value) {

    public IdempotencyKey {
        tenantId = JsonValues.requireText(tenantId, "idempotency tenantId");
        value = JsonValues.requireText(value, "idempotency key");
    }

    @Override
    public String toString() {
        return "IdempotencyKey[tenantId=" + tenantId + ", value=<redacted>]";
    }
}
