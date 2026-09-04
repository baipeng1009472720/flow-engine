package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Duration;
import java.util.Objects;

/** Immutable request to acquire a tenant- or workflow-scoped distributed lock. */
public record LockRequest(String lockName, String ownerId, Duration leaseDuration) {

    public LockRequest {
        lockName = JsonValues.requireText(lockName, "lock name");
        ownerId = JsonValues.requireText(ownerId, "lock ownerId");
        Objects.requireNonNull(leaseDuration, "lock leaseDuration must not be null");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("lock leaseDuration must be positive");
        }
    }
}
