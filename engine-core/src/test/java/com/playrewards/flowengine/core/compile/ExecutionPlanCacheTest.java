package com.playrewards.flowengine.core.compile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowNode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Verifies deterministic expiry, capacity and content-hash cache boundaries. */
class ExecutionPlanCacheTest {

    @Test
    void validates_limits_expires_entries_and_never_reuses_a_changed_hash() {
        MutableClock clock = new MutableClock();
        assertThrows(IllegalArgumentException.class, () -> new ExecutionPlanCache(0, Duration.ofMinutes(1), clock));
        assertThrows(IllegalArgumentException.class, () -> new ExecutionPlanCache(1, Duration.ZERO, clock));
        ExecutionPlanCache cache = new ExecutionPlanCache(2, Duration.ofMinutes(1), clock);
        FlowCompiler compiler = new FlowCompiler();
        ExecutableFlowBundle first = FlowCompilerTest.simpleBundle();

        ExecutionPlan firstPlan = cache.getOrCompile(first, compiler);
        assertSame(firstPlan, cache.getOrCompile(first, compiler));

        clock.advance(Duration.ofMinutes(2));
        assertNotSame(firstPlan, cache.getOrCompile(first, compiler));

        ExecutableFlowBundle changedBody = FlowCompilerTest.bundle(
                first.flowDefinition().nodes(), first.flowDefinition().edges(), first.componentContracts(),
                java.util.Map.of("start", java.util.Map.of("changed", true)));
        assertNotSame(cache.getOrCompile(first, compiler), cache.getOrCompile(changedBody, compiler));
    }

    @Test
    void evicts_least_recently_used_entry_at_capacity() {
        MutableClock clock = new MutableClock();
        ExecutionPlanCache cache = new ExecutionPlanCache(2, Duration.ofHours(1), clock);
        FlowCompiler compiler = new FlowCompiler();
        ExecutableFlowBundle a = StructureCompilationTestAccess.withFlowId(FlowCompilerTest.simpleBundle(), "a");
        ExecutableFlowBundle b = StructureCompilationTestAccess.withFlowId(FlowCompilerTest.simpleBundle(), "b");
        ExecutableFlowBundle c = StructureCompilationTestAccess.withFlowId(FlowCompilerTest.simpleBundle(), "c");
        ExecutionPlan a1 = cache.getOrCompile(a, compiler);
        ExecutionPlan b1 = cache.getOrCompile(b, compiler);
        assertSame(a1, cache.getOrCompile(a, compiler));
        cache.getOrCompile(c, compiler);

        ExecutionPlan b2 = cache.getOrCompile(b, compiler);
        assertNotSame(b1, b2);
        assertSame(b2, cache.getOrCompile(b, compiler));
        assertEquals(2, cache.size());
    }

    @Test
    void validates_bundle_hash_before_a_cache_hit_can_return_an_old_plan() {
        MutableClock clock = new MutableClock();
        ExecutionPlanCache cache = new ExecutionPlanCache(2, Duration.ofHours(1), clock);
        FlowCompiler compiler = new FlowCompiler();
        ExecutableFlowBundle valid = FlowCompilerTest.simpleBundle();
        cache.getOrCompile(valid, compiler);
        FlowNode changedStart = new FlowNode("start", "builtin.start", "start", "start",
                Map.of("changed", true));
        ExecutableFlowBundle staleHash = new ExecutableFlowBundle(
                valid.contractVersion(), valid.flowIdentity(),
                new FlowDefinition(valid.flowDefinition().schemaVersion(),
                        List.of(changedStart, valid.flowDefinition().nodes().getLast()), valid.flowDefinition().edges()),
                valid.componentContracts(), valid.protocolSchemas(), valid.externalScopeSchemas(),
                valid.bundleHash(), valid.trust());

        FlowCompilationException failure = assertThrows(FlowCompilationException.class,
                () -> cache.getOrCompile(staleHash, compiler));

        assertTrue(failure.issues().stream()
                .anyMatch(issue -> "FLOW_BUNDLE_HASH_MISMATCH".equals(issue.code())));
    }

    @Test
    void starts_the_full_ttl_after_slow_compilation_finishes() {
        MutableClock clock = new MutableClock();
        AtomicInteger resolutions = new AtomicInteger();
        ExecutableFlowBundle child = StructureCompilationTest.withIdentity(
                FlowCompilerTest.simpleBundle(), "child");
        ExecutableFlowBundle parent = StructureCompilationTest.subFlowBundle("parent", "child");
        FlowCompiler slowCompiler = new FlowCompiler((identity, targetFlowId) -> {
            resolutions.incrementAndGet();
            clock.advance(Duration.ofMinutes(10));
            return child;
        });
        ExecutionPlanCache cache = new ExecutionPlanCache(2, Duration.ofMinutes(1), clock);

        ExecutionPlan first = cache.getOrCompile(parent, slowCompiler);
        ExecutionPlan second = cache.getOrCompile(parent, slowCompiler);

        assertSame(first, second);
        assertEquals(1, resolutions.get());
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-09-04T00:00:00Z");

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return instant; }
    }

    private static final class StructureCompilationTestAccess {
        static ExecutableFlowBundle withFlowId(ExecutableFlowBundle bundle, String flowId) {
            ExecutableFlowBundle raw = new ExecutableFlowBundle(bundle.contractVersion(),
                    new com.playrewards.flowengine.contract.bundle.FlowIdentity(
                            bundle.flowIdentity().tenantId(), flowId, bundle.flowIdentity().version()),
                    bundle.flowDefinition(), bundle.componentContracts(), bundle.protocolSchemas(),
                    bundle.externalScopeSchemas(), null, bundle.trust());
            return raw.withCalculatedHash();
        }
    }
}
