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

        private static final long HARD_MAX_CONTEXT_BYTES = 4_194_304;
        private static final int HARD_MAX_STRING_BYTES = 1_048_576;
        private static final int HARD_MAX_DEPTH = 64;
        private static final int HARD_MAX_NODES = 100_000;

        public ScriptInvocation {
            Objects.requireNonNull(script, "script must not be null");
            if (script.isBlank()) {
                throw new IllegalArgumentException("script must not be blank");
            }
            ResourceCounter resources = new ResourceCounter();
            resources.visit(externalScopes, 0);
            resources.visit(ctx, 0);
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

        /** Hard pre-copy ceiling; adapter-specific limits may be lower. */
        private static final class ResourceCounter {

            private long bytes;
            private int nodes;

            private void visit(Object value, int depth) {
                if (depth > HARD_MAX_DEPTH || ++nodes > HARD_MAX_NODES) {
                    reject();
                }
                if (value == null) {
                    addBytes(4);
                    return;
                }
                if (value instanceof String text) {
                    int length = utf8Length(text);
                    if (length > HARD_MAX_STRING_BYTES) {
                        reject();
                    }
                    addBytes((long) length + 2);
                    return;
                }
                if (value instanceof Map<?, ?> map) {
                    addBytes(2L + map.size());
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!(entry.getKey() instanceof String key)) {
                            continue;
                        }
                        int keyBytes = utf8Length(key);
                        if (keyBytes > HARD_MAX_STRING_BYTES) {
                            reject();
                        }
                        addBytes((long) keyBytes + 3);
                        visit(entry.getValue(), depth + 1);
                    }
                    return;
                }
                if (value instanceof Iterable<?> iterable) {
                    addBytes(2);
                    for (Object child : iterable) {
                        addBytes(1);
                        visit(child, depth + 1);
                    }
                    return;
                }
                if (value instanceof Number number) {
                    addBytes(estimatedNumberBytes(number));
                    return;
                }
                if (value instanceof Boolean) {
                    addBytes(5);
                    return;
                }
                addBytes(16);
            }

            private void addBytes(long amount) {
                bytes += amount;
                if (bytes > HARD_MAX_CONTEXT_BYTES) {
                    reject();
                }
            }

            private static int utf8Length(String value) {
                int length = 0;
                for (int offset = 0; offset < value.length();) {
                    int codePoint = value.codePointAt(offset);
                    length += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2
                            : codePoint <= 0xffff ? 3 : 4;
                    offset += Character.charCount(codePoint);
                }
                return length;
            }

            private static long estimatedNumberBytes(Number value) {
                if (value instanceof java.math.BigInteger integer) {
                    return Math.max(1, (long) Math.ceil(integer.bitLength() * 0.30103)) + 1;
                }
                if (value instanceof java.math.BigDecimal decimal) {
                    return (long) decimal.precision() + Math.abs((long) decimal.scale()) + 3;
                }
                return 32;
            }

            private static void reject() {
                throw new IllegalArgumentException(
                        "script invocation context exceeds the hard resource limit");
            }
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
