package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.spi.CancellationToken;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Bounded hierarchical concurrency gate with one deadlock-safe acquisition order.
 *
 * <p>Every invocation acquires global, tenant, flow, component and resource permits in that exact
 * order. A failed, interrupted, timed-out or cancelled acquisition releases all earlier levels in
 * reverse order.</p>
 */
public final class ConcurrencyGovernor {

    private static final long CANCELLATION_POLL_MILLIS = 25;
    private static final ConcurrencyGovernor PROCESS_DEFAULT =
            new ConcurrencyGovernor(1024, 256, 256, 128, 64, Clock.systemUTC());

    private final Semaphore global;
    private final Clock clock;
    private final KeyedPermitPool tenants;
    private final KeyedPermitPool flows;
    private final KeyedPermitPool components;
    private final KeyedPermitPool resources;

    /**
     * Returns the process-shared finite governor used by default engine and scheduler wiring.
     *
     * @return JVM-wide default governor; callers needing intentional isolation must use the public constructor
     */
    public static ConcurrencyGovernor defaults() {
        return PROCESS_DEFAULT;
    }

    /**
     * Creates a five-level concurrency gate.
     *
     * @param globalLimit process-wide limit
     * @param tenantLimit limit for each tenant
     * @param flowLimit limit for each tenant-flow identity
     * @param componentLimit limit for each tenant-component identity
     * @param resourceLimit limit for each external resource identity
     * @param clock deadline time source
     */
    public ConcurrencyGovernor(int globalLimit, int tenantLimit, int flowLimit, int componentLimit,
            int resourceLimit, Clock clock) {
        this.global = new Semaphore(positive(globalLimit, "globalLimit"), true);
        this.tenants = new KeyedPermitPool(positive(tenantLimit, "tenantLimit"));
        this.flows = new KeyedPermitPool(positive(flowLimit, "flowLimit"));
        this.components = new KeyedPermitPool(positive(componentLimit, "componentLimit"));
        this.resources = new KeyedPermitPool(positive(resourceLimit, "resourceLimit"));
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Acquires every hierarchy level or atomically cleans up the partial acquisition.
     *
     * @param request stable scheduling identities
     * @param cancellationToken execution cancellation view
     * @param deadline absolute execution deadline
     * @return closeable permit owning all five acquired levels
     * @throws CancellationException when cancellation wins while waiting
     * @throws ExecutionDeadlineExceededException when the deadline wins while waiting
     */
    public Permit acquire(PermitRequest request, CancellationToken cancellationToken, Instant deadline) {
        Objects.requireNonNull(request, "permit request must not be null");
        Objects.requireNonNull(cancellationToken, "cancellation token must not be null");
        Objects.requireNonNull(deadline, "deadline must not be null");
        List<LevelLease> levels = new ArrayList<>(5);
        List<LevelLease> acquired = new ArrayList<>(5);
        try {
            levels.add(new LevelLease(global, () -> { }));
            levels.add(tenants.retain(new TenantKey(request.tenantId())));
            levels.add(flows.retain(new FlowKey(request.tenantId(), request.flowId())));
            levels.add(components.retain(new ComponentKey(request.tenantId(), request.componentCode()),
                    request.componentMaxConcurrency()));
            levels.add(resources.retain(new ResourceKey(request.resourceKey())));
            for (LevelLease level : levels) {
                acquireOne(level.semaphore(), cancellationToken, deadline);
                acquired.add(level);
            }
            return new Permit(levels);
        } catch (RuntimeException exception) {
            releasePermitsReverse(acquired);
            releaseReferencesReverse(levels);
            throw exception;
        }
    }

    private void acquireOne(Semaphore semaphore, CancellationToken cancellationToken, Instant deadline) {
        while (true) {
            cancellationToken.throwIfCancellationRequested();
            Instant now = clock.instant();
            if (!now.isBefore(deadline)) {
                throw new ExecutionDeadlineExceededException("deadline exceeded while waiting for concurrency permit");
            }
            long remainingMillis = Math.max(1, now.until(deadline, ChronoUnit.MILLIS));
            try {
                if (semaphore.tryAcquire(Math.min(CANCELLATION_POLL_MILLIS, remainingMillis),
                        TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                if (cancellationToken.isCancellationRequested()) {
                    throw new CancellationException("permit acquisition cancelled");
                }
                throw new CancellationException("permit acquisition interrupted");
            }
        }
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least one");
        }
        return value;
    }

    private static void releasePermitsReverse(List<LevelLease> acquired) {
        for (int index = acquired.size() - 1; index >= 0; index--) {
            acquired.get(index).semaphore().release();
        }
    }

    private static void releaseReferencesReverse(List<LevelLease> levels) {
        for (int index = levels.size() - 1; index >= 0; index--) {
            levels.get(index).releaseReference().run();
        }
    }

    /** Stable keys required to select one semaphore at each hierarchy level. */
    public record PermitRequest(String tenantId, String flowId, String componentCode, String resourceKey,
                                int componentMaxConcurrency) {

        public PermitRequest {
            tenantId = JsonValues.requireText(tenantId, "permit tenantId");
            flowId = JsonValues.requireText(flowId, "permit flowId");
            componentCode = JsonValues.requireText(componentCode, "permit componentCode");
            resourceKey = JsonValues.requireText(resourceKey, "permit resourceKey");
            positive(componentMaxConcurrency, "componentMaxConcurrency");
        }

        /** Uses only the governor-wide component ceiling when no narrower contract limit is supplied. */
        public PermitRequest(String tenantId, String flowId, String componentCode, String resourceKey) {
            this(tenantId, flowId, componentCode, resourceKey, Integer.MAX_VALUE);
        }
    }

    /** Idempotently releases a complete hierarchy in reverse acquisition order. */
    public static final class Permit implements AutoCloseable {

        private final List<LevelLease> levels;
        private final AtomicBoolean open = new AtomicBoolean(true);

        private Permit(List<LevelLease> levels) {
            this.levels = List.copyOf(levels);
        }

        /** @return true until this permit has been closed */
        public boolean acquired() {
            return open.get();
        }

        @Override
        public void close() {
            if (open.compareAndSet(true, false)) {
                releasePermitsReverse(levels);
                releaseReferencesReverse(levels);
            }
        }
    }

    private record LevelLease(Semaphore semaphore, Runnable releaseReference) {
    }

    private record TenantKey(String tenantId) {
    }

    private record FlowKey(String tenantId, String flowId) {
    }

    private record ComponentKey(String tenantId, String componentCode) {
    }

    private record ResourceKey(String resourceKey) {
    }

    /** Removes idle keyed semaphores without racing a concurrent retain of the same identity. */
    private static final class KeyedPermitPool {

        private final int limit;
        private final ConcurrentHashMap<Object, PermitCell> cells = new ConcurrentHashMap<>();

        private KeyedPermitPool(int limit) {
            this.limit = limit;
        }

        private LevelLease retain(Object key) {
            return retain(key, limit);
        }

        private LevelLease retain(Object key, int requestedLimit) {
            int effectiveLimit = Math.min(limit, positive(requestedLimit, "requestedLimit"));
            AtomicReference<PermitCell> retained = new AtomicReference<>();
            cells.compute(key, (ignored, current) -> {
                PermitCell cell = current == null
                        ? new PermitCell(effectiveLimit) : current;
                if (current != null) {
                    cell.addLimit(effectiveLimit);
                }
                cell.references++;
                retained.set(cell);
                return cell;
            });
            PermitCell cell = retained.get();
            return new LevelLease(cell.semaphore, () -> release(key, cell, effectiveLimit));
        }

        private void release(Object key, PermitCell expected, int retainedLimit) {
            cells.compute(key, (ignored, current) -> {
                if (current != expected) {
                    return current;
                }
                current.references--;
                if (current.references < 0) {
                    throw new IllegalStateException("keyed concurrency permit reference underflow");
                }
                if (current.references == 0) {
                    return null;
                }
                current.removeLimit(retainedLimit);
                return current;
            });
        }
    }

    /** Mutable only under ConcurrentHashMap.compute for the corresponding key. */
    private static final class PermitCell {

        private final AdjustableSemaphore semaphore;
        private final TreeMap<Integer, Integer> retainedLimits = new TreeMap<>();
        private int references;

        private PermitCell(int limit) {
            this.semaphore = new AdjustableSemaphore(limit, true);
            retainedLimits.put(limit, 1);
        }

        private void addLimit(int limit) {
            int previous = retainedLimits.firstKey();
            retainedLimits.merge(limit, 1, Integer::sum);
            int current = retainedLimits.firstKey();
            if (current < previous) {
                semaphore.reduce(previous - current);
            }
        }

        private void removeLimit(int limit) {
            int previous = retainedLimits.firstKey();
            retainedLimits.compute(limit, (ignored, count) -> {
                if (count == null) {
                    throw new IllegalStateException("component concurrency limit reference underflow");
                }
                return count == 1 ? null : count - 1;
            });
            int current = retainedLimits.firstKey();
            if (current > previous) {
                semaphore.release(current - previous);
            }
        }
    }

    /** Exposes protected permit reduction for a safely lowered overlapping component contract. */
    private static final class AdjustableSemaphore extends Semaphore {

        private AdjustableSemaphore(int permits, boolean fair) {
            super(permits, fair);
        }

        private void reduce(int reductions) {
            reducePermits(reductions);
        }
    }
}
