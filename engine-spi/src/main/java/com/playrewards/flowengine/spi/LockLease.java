package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Instant;
import java.util.Objects;

/**
 * Opaque ownership proof returned by a successful lock acquisition.
 *
 * <p>A lease must be treated as secret-bearing capability data: callers may release it but must not
 * place it in logs, events, or user-visible payloads.</p>
 */
public record LockLease(String lockName, String ownerId, String leaseId, Instant expiresAt,
                        long fencingToken) {

    public LockLease {
        lockName = JsonValues.requireText(lockName, "lock lease name");
        ownerId = JsonValues.requireText(ownerId, "lock lease ownerId");
        leaseId = JsonValues.requireText(leaseId, "lock leaseId");
        Objects.requireNonNull(expiresAt, "lock expiry must not be null");
        if (fencingToken < 1) {
            throw new IllegalArgumentException("lock fencingToken must be positive");
        }
    }

    @Override
    public String toString() {
        return "LockLease[lockName=<redacted>, ownerId=" + ownerId
                + ", leaseId=<redacted>, expiresAt=" + expiresAt + ", fencingToken=<redacted>]";
    }
}
