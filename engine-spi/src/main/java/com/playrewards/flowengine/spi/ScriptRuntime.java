package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Executes one untrusted script behind an adapter-owned sandbox boundary.
 *
 * <p>The SPI carries only copied JSON values and cooperative runtime services. Implementations must
 * enforce resource limits and must never expose compiled classes, script source, or host framework
 * objects to Core.</p>
 */
@FunctionalInterface
public interface ScriptRuntime {

    /**
     * Starts a bounded script invocation.
     *
     * @param invocation immutable source, context and execution controls
     * @return asynchronous JSON-compatible script result
     */
    CompletionStage<Object> execute(ScriptInvocation invocation);

    /** Immutable, redaction-safe values supplied to one script execution. */
    record ScriptInvocation(String script, Map<String, Map<String, Object>> externalScopes,
                            Map<String, Object> ctx, ExecutionLogger logger,
                            CancellationToken cancellationToken, Instant deadline) {

        public ScriptInvocation {
            Objects.requireNonNull(script, "script must not be null");
            if (script.isBlank()) {
                throw new IllegalArgumentException("script must not be blank");
            }
            externalScopes = copyScopes(externalScopes);
            ctx = JsonValues.copyObject(ctx, "script ctx");
            Objects.requireNonNull(logger, "script logger must not be null");
            Objects.requireNonNull(cancellationToken, "script cancellation token must not be null");
            Objects.requireNonNull(deadline, "script deadline must not be null");
        }

        @Override
        public String toString() {
            return "ScriptInvocation[script=<redacted>, externalScopeKeys=" + externalScopes.keySet()
                    + ", ctxKeys=" + ctx.keySet() + ", logger=" + logger.getClass().getName()
                    + ", cancellationToken=<redacted>, deadline=" + deadline + "]";
        }

        private static Map<String, Map<String, Object>> copyScopes(
                Map<String, Map<String, Object>> source) {
            Objects.requireNonNull(source, "script external scopes must not be null");
            Map<String, Map<String, Object>> copy = new LinkedHashMap<>();
            source.forEach((key, value) -> copy.put(
                    JsonValues.requireText(key, "script external scope key"),
                    JsonValues.copyObject(value, "script external scope " + key)));
            return Map.copyOf(copy);
        }
    }

    /** Stable, source-redacted failure returned by a script adapter. */
    final class ScriptExecutionException extends RuntimeException {

        private final String code;

        /**
         * Creates a failure containing only a stable code and safe public message.
         *
         * @param code stable machine-readable script failure code
         * @param message redacted diagnostic which must not contain source or input values
         */
        public ScriptExecutionException(String code, String message) {
            super(JsonValues.requireText(message, "script failure message"));
            this.code = JsonValues.requireText(code, "script failure code");
        }

        /** @return stable machine-readable failure code */
        public String code() {
            return code;
        }
    }
}
