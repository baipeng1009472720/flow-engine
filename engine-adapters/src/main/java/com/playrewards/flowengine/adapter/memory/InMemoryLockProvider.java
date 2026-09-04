package com.playrewards.flowengine.adapter.memory;

import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.LockLease;
import com.playrewards.flowengine.spi.LockRequest;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Semaphore;

/**
 * Concurrent local lease provider for tests and single-process embedding.
 *
 * <p>Only active keys are retained. A process-wide monotonic sequence preserves fencing ordering
 * after inactive key records are removed; this adapter does not provide cross-process exclusion.</p>
 */
public final class InMemoryLockProvider implements DistributedLockProvider {

    private static final int DEFAULT_MAX_ACTIVE_LOCKS = 65_536;

    private final ConcurrentHashMap<String, LockLease> active = new ConcurrentHashMap<>();
    private final AtomicLong fencingSequence = new AtomicLong();
    private final Semaphore capacity;
    private final Clock clock;

    /** Creates a provider using the system UTC clock. */
    public InMemoryLockProvider() {
        this(Clock.systemUTC());
    }

    /**
     * Creates a provider with a deterministic lease clock.
     *
     * @param clock expiry clock shared with the embedding engine when deterministic behavior matters
     */
    public InMemoryLockProvider(Clock clock) {
        this(clock, DEFAULT_MAX_ACTIVE_LOCKS);
    }

    /** Creates a provider with an explicit strict active-key capacity. */
    public InMemoryLockProvider(Clock clock, int maxActiveLocks) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        if (maxActiveLocks < 1) {
            throw new IllegalArgumentException("maxActiveLocks must be at least one");
        }
        this.capacity = new Semaphore(maxActiveLocks, true);
    }

    /**
     * Acquires an absent or expired key atomically and assigns a strictly increasing fence.
     *
     * @param request requested lock identity and finite lease duration
     * @return the new ownership capability, or empty while an unexpired owner remains
     */
    @Override
    public Optional<LockLease> tryAcquire(LockRequest request) {
        Objects.requireNonNull(request, "lock request must not be null");
        purgeExpired();
        Instant now = clock.instant();
        AtomicReference<LockLease> acquired = new AtomicReference<>();
        active.compute(request.lockName(), (ignored, current) -> {
            if (current != null && current.expiresAt().isAfter(now)) {
                return current;
            }
            boolean reserved = false;
            if (current == null) {
                if (!capacity.tryAcquire()) {
                    return null;
                }
                reserved = true;
            }
            try {
                LockLease lease = new LockLease(request.lockName(), request.ownerId(),
                        UUID.randomUUID().toString(), now.plus(request.leaseDuration()),
                        fencingSequence.incrementAndGet());
                acquired.set(lease);
                return lease;
            } catch (RuntimeException | Error failure) {
                if (reserved) {
                    capacity.release();
                }
                throw failure;
            }
        });
        return Optional.ofNullable(acquired.get());
    }

    /**
     * Releases only the exact active owner/lease/fence tuple and rejects expired capabilities.
     *
     * @param lease opaque ownership capability returned by this provider
     * @return true only when the supplied capability removed the currently active lease
     */
    @Override
    public boolean release(LockLease lease) {
        Objects.requireNonNull(lease, "lock lease must not be null");
        Instant now = clock.instant();
        AtomicBoolean released = new AtomicBoolean();
        AtomicBoolean removed = new AtomicBoolean();
        active.computeIfPresent(lease.lockName(), (ignored, current) -> {
            if (!sameCapability(current, lease) || !current.expiresAt().isAfter(now)) {
                if (!current.expiresAt().isAfter(now)) {
                    removed.set(true);
                    return null;
                }
                return current;
            }
            released.set(true);
            removed.set(true);
            return null;
        });
        if (removed.get()) {
            capacity.release();
        }
        return released.get();
    }

    /** @return current active-key count, exposed for bounded-resource diagnostics and tests */
    public int activeLockCount() {
        purgeExpired();
        return active.size();
    }

    private void purgeExpired() {
        Instant now = clock.instant();
        active.forEach((name, lease) -> {
            if (!lease.expiresAt().isAfter(now) && active.remove(name, lease)) {
                capacity.release();
            }
        });
    }

    private static boolean sameCapability(LockLease current, LockLease supplied) {
        return current.ownerId().equals(supplied.ownerId())
                && current.leaseId().equals(supplied.leaseId())
                && current.fencingToken() == supplied.fencingToken();
    }
}
