package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ContentChunk;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Asynchronous result and optional content stream started by a component invocation.
 *
 * <p>The completion stage contains named outputs exactly once. The publisher may emit user-visible
 * chunks before completion; non-streaming components must use {@link #emptyContent()} rather than
 * {@code null}. Subscription callbacks follow the {@link Flow.Publisher} threading contract.</p>
 */
public record ComponentInvocation(CompletionStage<ComponentOutputs> completion,
                                  Flow.Publisher<ContentChunk> content) {

    public ComponentInvocation {
        Objects.requireNonNull(completion, "component completion must not be null");
        Objects.requireNonNull(content, "component content publisher must not be null");
    }

    /**
     * Returns a publisher that completes immediately without emitting content.
     *
     * @return reusable non-null publisher for non-streaming component invocations
     */
    public static Flow.Publisher<ContentChunk> emptyContent() {
        return EmptyContentPublisher.INSTANCE;
    }

    private enum EmptyContentPublisher implements Flow.Publisher<ContentChunk> {
        INSTANCE;

        @Override
        public void subscribe(Flow.Subscriber<? super ContentChunk> subscriber) {
            Objects.requireNonNull(subscriber, "content subscriber must not be null");
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long count) {
                    // This publisher has no values and therefore no demand to satisfy.
                }

                @Override
                public void cancel() {
                    // There is no producer resource to release.
                }
            });
            subscriber.onComplete();
        }
    }
}
