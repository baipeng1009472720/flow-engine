package com.playrewards.flowengine.adapter.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.playrewards.flowengine.spi.LockLease;
import com.playrewards.flowengine.spi.LockRequest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Verifies fencing, stale-owner rejection, tenant-name isolation and active-key cleanup. */
class InMemoryLockProviderTest {

    @Test
    void stale_or_wrong_capability_cannot_release_a_newer_lease() {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-04T00:00:00Z"));
        InMemoryLockProvider provider = new InMemoryLockProvider(clock);
        LockLease first = provider.tryAcquire(request("tenant:a:key", "owner-a", 10)).orElseThrow();
        LockLease wrongOwner = new LockLease(first.lockName(), "owner-b", first.leaseId(),
                first.expiresAt(), first.fencingToken());

        assertFalse(provider.release(wrongOwner));
        clock.advance(Duration.ofMillis(11));
        LockLease second = provider.tryAcquire(request("tenant:a:key", "owner-b", 10)).orElseThrow();

        assertTrue(second.fencingToken() > first.fencingToken());
        assertFalse(provider.release(first));
        assertTrue(provider.release(second));
        assertEquals(0, provider.activeLockCount());
    }

    @Test
    void distinct_tenant_scoped_names_do_not_conflict_and_released_keys_are_removed() {
        InMemoryLockProvider provider = new InMemoryLockProvider();
        LockLease first = provider.tryAcquire(request("tenant:a:key", "owner-a", 1_000)).orElseThrow();
        LockLease second = provider.tryAcquire(request("tenant:b:key", "owner-b", 1_000)).orElseThrow();

        assertEquals(2, provider.activeLockCount());
        assertTrue(provider.release(first));
        assertTrue(provider.release(second));
        assertEquals(0, provider.activeLockCount());
    }

    @Test
    void expired_unique_keys_are_reclaimed_on_the_normal_acquire_path() {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-04T00:00:00Z"));
        InMemoryLockProvider provider = new InMemoryLockProvider(clock, 1);
        provider.tryAcquire(request("tenant:a:first", "owner-a", 10)).orElseThrow();
        clock.advance(Duration.ofMillis(11));

        LockLease replacement = provider.tryAcquire(
                request("tenant:a:second", "owner-b", 10)).orElseThrow();

        assertEquals("tenant:a:second", replacement.lockName());
        assertEquals(1, provider.activeLockCount());
    }

    @Test
    void failed_lease_construction_rolls_back_the_reserved_capacity() {
        MutableClock clock = new MutableClock(Instant.MAX);
        InMemoryLockProvider provider = new InMemoryLockProvider(clock, 1);

        assertThrows(java.time.DateTimeException.class,
                () -> provider.tryAcquire(request("overflow", "owner-a", 1_000)));
        clock.set(Instant.parse("2026-09-04T00:00:00Z"));

        assertTrue(provider.tryAcquire(request("healthy", "owner-b", 1_000)).isPresent());
    }

    private static LockRequest request(String name, String owner, long leaseMillis) {
        return new LockRequest(name, owner, Duration.ofMillis(leaseMillis));
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        private MutableClock(Instant now) {
            this.now = new AtomicReference<>(now);
        }

        private void advance(Duration duration) {
            now.updateAndGet(value -> value.plus(duration));
        }

        private void set(Instant instant) {
            now.set(instant);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
