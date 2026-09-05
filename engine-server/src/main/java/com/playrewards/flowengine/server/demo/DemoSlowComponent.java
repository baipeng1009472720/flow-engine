package com.playrewards.flowengine.server.demo;

import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.spi.*;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Pure local cancellation example; waits on the scheduler's bounded virtual-thread capacity. */
public final class DemoSlowComponent implements FlowBusinessComponent {
    @Override
    public String executorRef() {
        return "demo.slow";
    }

    @Override
    public ComponentCapabilities capabilities() {
        return new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 64);
    }

    /**
     * Waits at most thirty seconds, cooperatively stopping on token cancellation or interruption.
     */
    @Override
    public ComponentInvocation execute(ComponentExecutionContext context) {
        context.logger().info("演示慢流程开始等待", null, Map.of());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            context.cancellationToken().throwIfCancellationRequested();
            try {
                TimeUnit.MILLISECONDS.sleep(25);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new CancellationException("demo wait interrupted");
            }
        }
        context.cancellationToken().throwIfCancellationRequested();
        return new ComponentInvocation(
                CompletableFuture.completedFuture(
                        new ComponentOutputs(Map.of("result", "wait-complete"))),
                ComponentInvocation.emptyContent());
    }
}
