package com.playrewards.flowengine.server.demo;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.contract.execution.ContentChunk;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.core.compile.FlowCompiler;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

/** Verifies the demo is executable trusted data and that stream completion follows real demand. */
class DemoFlowsTest {
    @Test
    void slow_demo_cooperatively_stops_when_cancelled() throws Exception {
        var cancellation = new com.playrewards.flowengine.core.execution.CancellationHandle();
        var started = new java.util.concurrent.CountDownLatch(1);
        var completion = new java.util.concurrent.CompletableFuture<ComponentInvocation>();
        Thread worker =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    try {
                                        completion.complete(
                                                new DemoSlowComponent()
                                                        .execute(
                                                                new ComponentExecutionContext(
                                                                        new ComponentConfig(
                                                                                Map.of()),
                                                                        new ComponentInputs(
                                                                                Map.of(), Map.of()),
                                                                        new ReadOnlyExecutionContext(
                                                                                Map.of()),
                                                                        entry ->
                                                                                started.countDown(),
                                                                        cancellation,
                                                                        ExecutionPurpose.TRIAL)));
                                    } catch (Throwable failure) {
                                        completion.completeExceptionally(failure);
                                    }
                                });
        try {
            assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(completion.isDone());
            cancellation.cancel();
            assertThrows(java.util.concurrent.CancellationException.class, completion::join);
            worker.join(2_000);
            assertFalse(worker.isAlive());
        } finally {
            cancellation.cancel();
            worker.interrupt();
            worker.join(2_000);
        }
    }

    @Test
    void demo_bundles_compile_and_keep_subflow_in_the_demo_tenant() {
        DemoBundleLoader loader = new DemoBundleLoader();
        FlowCompiler compiler = new FlowCompiler(loader::resolve);
        for (String name : List.of("main", "child", "slow", "failure")) {
            var bundle = loader.load("demo:" + name);
            assertEquals("demo", bundle.flowIdentity().tenantId());
            assertEquals(bundle.calculateBundleHash(), bundle.bundleHash());
            assertFalse(compiler.compile(bundle).nodesById().isEmpty());
        }
        var plan = compiler.compile(loader.load("demo:main"));
        assertEquals("child", plan.subFlowPlans().get("sub").flowIdentity().flowId());
        assertEquals("loopEnd", plan.loopPairs().get("loop"));
        assertEquals(2, plan.outgoingByPort().get("parallel").get("BRANCH").size());
        assertThrows(IllegalArgumentException.class, () -> loader.load("demo:unknown"));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        loader.resolve(
                                new com.playrewards.flowengine.contract.bundle.FlowIdentity(
                                        "other", "main", "v1"),
                                "child"));
    }

    @Test
    void stream_emits_only_on_demand_then_completes_named_output_and_logs() {
        List<ExecutionLogEntry> logs = new ArrayList<>();
        var component = new DemoStreamComponent();
        var invocation =
                component.execute(
                        new ComponentExecutionContext(
                                new ComponentConfig(Map.of()),
                                new ComponentInputs(Map.of(), Map.of()),
                                new ReadOnlyExecutionContext(Map.of()),
                                logs::add,
                                () -> false,
                                ExecutionPurpose.TRIAL));
        var subscription = new AtomicReference<Flow.Subscription>();
        List<ContentChunk> chunks = new ArrayList<>();
        invocation
                .content()
                .subscribe(
                        new Flow.Subscriber<ContentChunk>() {
                            public void onSubscribe(Flow.Subscription value) {
                                subscription.set(value);
                            }

                            public void onNext(ContentChunk chunk) {
                                chunks.add(chunk);
                            }

                            public void onError(Throwable failure) {
                                fail(failure);
                            }

                            public void onComplete() {}
                        });
        assertFalse(invocation.completion().toCompletableFuture().isDone());
        subscription.get().request(1);
        assertEquals(1, chunks.size());
        assertFalse(invocation.completion().toCompletableFuture().isDone());
        subscription.get().request(1);
        assertEquals(List.of(0L, 1L), chunks.stream().map(ContentChunk::sequence).toList());
        assertEquals(
                "stream-complete",
                invocation.completion().toCompletableFuture().join().values().get("result"));
        assertFalse(logs.isEmpty());
    }
}
