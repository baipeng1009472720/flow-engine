package com.playrewards.flowengine.server.demo;

import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.execution.ContentChunk;
import com.playrewards.flowengine.spi.*;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Local-only pure example: two demanded chunks followed by one protocol-conformant final result.
 */
public final class DemoStreamComponent implements FlowBusinessComponent {
    public static final ComponentCapabilities CAPABILITIES =
            new ComponentCapabilities(ComponentEffect.PURE, true, true, false, 64);

    /** Stable example reference registered once by the explicit demo configuration. */
    @Override
    public String executorRef() {
        return "demo.stream";
    }

    @Override
    public ComponentCapabilities capabilities() {
        return CAPABILITIES;
    }

    /** No background worker or unbounded buffering; only requested chunks are emitted. */
    @Override
    public ComponentInvocation execute(ComponentExecutionContext context) {
        CompletableFuture<ComponentOutputs> result = new CompletableFuture<>();
        AtomicBoolean subscribed = new AtomicBoolean();
        return new ComponentInvocation(
                result,
                subscriber -> {
                    Objects.requireNonNull(subscriber, "subscriber");
                    if (!subscribed.compareAndSet(false, true)) {
                        ComponentInvocation.emptyContent().subscribe(subscriber);
                        return;
                    }
                    subscriber.onSubscribe(
                            new Flow.Subscription() {
                                private int next;
                                private boolean stopped;

                                @Override
                                public synchronized void request(long count) {
                                    if (stopped) return;
                                    if (count <= 0) {
                                        stopped = true;
                                        var failure =
                                                new IllegalArgumentException(
                                                        "positive stream demand required");
                                        result.completeExceptionally(failure);
                                        subscriber.onError(failure);
                                        return;
                                    }
                                    try {
                                        for (long emitted = 0;
                                                emitted < count && next < 2 && !stopped;
                                                emitted++) {
                                            context.cancellationToken()
                                                    .throwIfCancellationRequested();
                                            int sequence = next++;
                                            context.logger()
                                                    .info("演示内容分片", null, Map.of("part", sequence));
                                            // The runtime subscriber binds trusted
                                            // execution/node/scope identity.
                                            subscriber.onNext(
                                                    new ContentChunk(
                                                            "demo",
                                                            "stream",
                                                            "root",
                                                            sequence,
                                                            "text/plain",
                                                            sequence == 0 ? "第一段" : "第二段"));
                                        }
                                        if (next == 2 && !stopped) {
                                            stopped = true;
                                            subscriber.onComplete();
                                            result.complete(
                                                    new ComponentOutputs(
                                                            Map.of("result", "stream-complete")));
                                        }
                                    } catch (RuntimeException failure) {
                                        stopped = true;
                                        result.completeExceptionally(failure);
                                        subscriber.onError(failure);
                                    }
                                }

                                @Override
                                public synchronized void cancel() {
                                    if (!stopped) {
                                        stopped = true;
                                        result.cancel(false);
                                    }
                                }
                            });
                });
    }
}
