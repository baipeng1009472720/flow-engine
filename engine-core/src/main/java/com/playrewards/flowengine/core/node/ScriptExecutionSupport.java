package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.spi.ScriptRuntime.ScriptExecutionException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/** Central safe-failure boundary shared by every core caller of {@code ScriptRuntime}. */
public final class ScriptExecutionSupport {

    private ScriptExecutionSupport() {
    }

    /**
     * Starts one script call and maps both synchronous and asynchronous adapter failures.
     *
     * <p>The returned source stage remains separate so structural nodes can wait for the adapter's
     * real resource lifecycle instead of only the mapped dependent stage.</p>
     *
     * @param invocation deferred adapter call
     * @return safe completion plus the actual adapter source lifecycle
     * @throws NodeExecutionException for synchronous safe script or generic adapter failures
     */
    public static ScriptExecution start(Supplier<CompletionStage<Object>> invocation) {
        Objects.requireNonNull(invocation, "script invocation must not be null");
        CompletionStage<Object> source;
        try {
            source = Objects.requireNonNull(invocation.get(), "script completion must not be null");
        } catch (RuntimeException failure) {
            throw safeFailure(failure);
        }
        CompletableFuture<Object> completion = new CompletableFuture<>();
        try {
            source.whenComplete((value, failure) -> {
                if (failure == null) {
                    completion.complete(value);
                } else {
                    completion.completeExceptionally(safeFailure(failure));
                }
            });
        } catch (RuntimeException failure) {
            throw safeFailure(failure);
        }
        return new ScriptExecution(completion, source);
    }

    /**
     * Converts one adapter failure without leaking script source or input values.
     *
     * @param failure synchronous or wrapped asynchronous failure
     * @return cancellation unchanged, declared safe script failure, or generic safe failure
     */
    public static RuntimeException safeFailure(Throwable failure) {
        Throwable cause = unwrap(Objects.requireNonNull(failure, "script failure must not be null"));
        if (cause instanceof CancellationException cancellation) {
            return cancellation;
        }
        if (cause instanceof NodeExecutionException nodeFailure) {
            return nodeFailure;
        }
        if (cause instanceof ScriptExecutionException scriptFailure) {
            return new NodeExecutionException(scriptFailure.code(), scriptFailure.getMessage());
        }
        return new NodeExecutionException("SCRIPT_EXECUTION_FAILED", "script execution failed");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /** Mapped public completion paired with the exact adapter-owned source stage. */
    public record ScriptExecution(
            CompletionStage<Object> completion, CompletionStage<Object> sourceCompletion) {

        public ScriptExecution {
            Objects.requireNonNull(completion, "safe script completion must not be null");
            Objects.requireNonNull(sourceCompletion, "script source completion must not be null");
        }
    }
}
