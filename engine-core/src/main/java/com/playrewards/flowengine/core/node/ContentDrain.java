package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.execution.ContentChunk;
import com.playrewards.flowengine.contract.json.JsonValues;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Owns one bounded publisher subscription; completion and cancellation never leave it attached. */
public final class ContentDrain implements Flow.Subscriber<ContentChunk>, AutoCloseable {
    private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final Consumer<ContentChunk> sink;
    private int chunks;

    /** Creates an in-process bounded sink; it must never send directly to a network client. */
    public ContentDrain(Consumer<ContentChunk> sink) {
        this.sink = sink;
    }

    @Override
    public void onSubscribe(Flow.Subscription value) {
        if (!subscription.compareAndSet(null, value) || done.isDone()) {
            value.cancel();
            return;
        }
        value.request(Long.MAX_VALUE);
    }

    @Override
    public synchronized void onNext(ContentChunk chunk) {
        if (done.isDone()) return;
        try {
            if (++chunks > 4096)
                throw new NodeExecutionException(
                        "CONTENT_LIMIT_EXCEEDED", "content limit exceeded");
            JsonValues.measureJsonResources(
                    Collections.singletonList(chunk.payload()),
                    new JsonValues.JsonResourceLimits(16384, 8192, 16, 1024));
            sink.accept(chunk);
        } catch (RuntimeException failure) {
            onError(failure);
            closeSubscription();
        }
    }

    @Override
    public void onError(Throwable failure) {
        done.completeExceptionally(failure);
    }

    @Override
    public void onComplete() {
        done.complete(null);
    }

    /** Waits cooperatively for content completion, bounded by the same node deadline. */
    public void await(NodeExecutionContext context) throws Exception {
        while (!done.isDone()) {
            context.throwIfStopped();
            try {
                done.get(20, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
            }
        }
        done.get();
    }

    private void closeSubscription() {
        var value = subscription.get();
        if (value != null) value.cancel();
    }

    @Override
    public void close() {
        done.cancel(false);
        closeSubscription();
    }
}
