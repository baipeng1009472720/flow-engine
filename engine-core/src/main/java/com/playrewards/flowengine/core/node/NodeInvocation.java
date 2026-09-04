package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.execution.ContentChunk;
import com.playrewards.flowengine.spi.ComponentInvocation;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Asynchronous final node result plus an optional content stream. */
public record NodeInvocation(CompletionStage<NodeResult> completion,
                             Flow.Publisher<ContentChunk> contentStream) {

    public NodeInvocation {
        Objects.requireNonNull(completion, "node completion must not be null");
        Objects.requireNonNull(contentStream, "node content stream must not be null");
    }

    /** @return a completed non-streaming invocation for a built-in node */
    public static NodeInvocation completed(NodeResult result) {
        return new NodeInvocation(CompletableFuture.completedFuture(
                Objects.requireNonNull(result, "node result must not be null")),
                ComponentInvocation.emptyContent());
    }
}
