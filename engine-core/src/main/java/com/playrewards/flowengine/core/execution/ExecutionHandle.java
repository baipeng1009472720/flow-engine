package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.contract.execution.ExecutionResult;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Caller handle for one in-memory execution, including explicit cancellation and isolated values. */
public final class ExecutionHandle {

    private final String executionId;
    private final CompletionStage<ExecutionResult> completion;
    private final ExecutionContext context;
    private final CancellationHandle cancellation;

    /** Creates the caller view without exposing scheduler-owned mutable readiness state. */
    ExecutionHandle(String executionId, CompletionStage<ExecutionResult> completion,
            ExecutionContext context, CancellationHandle cancellation) {
        this.executionId = Objects.requireNonNull(executionId, "executionId must not be null");
        this.completion = Objects.requireNonNull(completion, "completion must not be null");
        this.context = Objects.requireNonNull(context, "context must not be null");
        this.cancellation = Objects.requireNonNull(cancellation, "cancellation must not be null");
    }

    /** @return unique execution identity */
    public String executionId() {
        return executionId;
    }

    /** @return terminal result stage, completed exactly once */
    public CompletionStage<ExecutionResult> completion() {
        return completion;
    }

    /** @return isolated append-only execution values for embedding and diagnostics */
    public ExecutionContext context() {
        return context;
    }

    /** @return true only when this call won the cancellation race */
    public boolean cancel() {
        return cancellation.cancel();
    }
}
