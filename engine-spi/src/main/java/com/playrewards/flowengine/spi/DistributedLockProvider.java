package com.playrewards.flowengine.spi;

import java.util.Optional;

/**
 * Technology-neutral provider for leases used by LOCK node semantics.
 *
 * <p>Implementations must only release a lease held by the supplied lease identity. This port defines
 * no database, Redis, or host transaction API.</p>
 */
public interface DistributedLockProvider {

    /**
     * Attempts to acquire one lease without waiting indefinitely.
     *
     * @param request immutable lock identity and lease request
     * @return ownership lease when acquired, otherwise empty
     */
    Optional<LockLease> tryAcquire(LockRequest request);

    /**
     * Releases a lease when it is still owned by the supplied identity.
     *
     * @param lease opaque lease returned by this provider
     * @return {@code true} when this call released an active lease
     */
    boolean release(LockLease lease);
}
