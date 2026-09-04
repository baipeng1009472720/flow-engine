package com.playrewards.flowengine.core.compile;

import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Thread-safe bounded LRU cache for plans addressed by identity and immutable bundle hash. */
public final class ExecutionPlanCache {

    private final int maxEntries;
    private final Duration expiry;
    private final Clock clock;
    private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);

    /**
     * Creates a deterministic cache with expire-after-write semantics.
     *
     * @param maxEntries positive hard capacity
     * @param expiry positive entry lifetime
     * @param clock time source used for deterministic expiry
     */
    public ExecutionPlanCache(int maxEntries, Duration expiry, Clock clock) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be greater than zero");
        }
        Objects.requireNonNull(expiry, "expiry must not be null");
        if (expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("expiry must be greater than zero");
        }
        this.maxEntries = maxEntries;
        this.expiry = expiry;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Returns a cached exact-hash plan or compiles and stores a new one.
     *
     * @param bundle immutable bundle to address
     * @param compiler compiler used on misses
     * @return compiled plan for the exact identity and hash
     */
    public synchronized ExecutionPlan getOrCompile(ExecutableFlowBundle bundle, FlowCompiler compiler) {
        Objects.requireNonNull(bundle, "bundle must not be null");
        Objects.requireNonNull(compiler, "compiler must not be null");
        Instant now = clock.instant();
        evictExpired(now);
        Key key = new Key(bundle.flowIdentity(), bundle.bundleHash());
        Entry current = entries.get(key);
        if (current != null) {
            return current.plan();
        }
        ExecutionPlan plan = compiler.compile(bundle);
        entries.put(key, new Entry(plan, now));
        while (entries.size() > maxEntries) {
            Iterator<Key> iterator = entries.keySet().iterator();
            iterator.next();
            iterator.remove();
        }
        return plan;
    }

    /** @return current entry count after eagerly removing expired plans */
    public synchronized int size() {
        evictExpired(clock.instant());
        return entries.size();
    }

    private void evictExpired(Instant now) {
        entries.entrySet().removeIf(entry -> !now.isBefore(entry.getValue().createdAt().plus(expiry)));
    }

    private record Key(FlowIdentity identity, String bundleHash) {
    }

    private record Entry(ExecutionPlan plan, Instant createdAt) {
    }
}
