package com.playrewards.flowengine.spi;

import java.time.Duration;
import java.util.Optional;

/** Execution ownership independent of business LOCK leases; transfer always advances the fence. */
public interface ExecutionLeaseRepository {
    /** Acquires only an absent/expired lease; every transfer advances its fencing token. */
    Optional<ExecutionLease> acquire(String executionId, String owner, Duration duration);

    /** Renews a still-live exact owner/token; a lost or expired token returns false. */
    boolean heartbeat(ExecutionLease lease, Duration duration);

    /** Releases only this capability; an old owner cannot shorten a newer owner's lease. */
    boolean release(ExecutionLease lease);
}
