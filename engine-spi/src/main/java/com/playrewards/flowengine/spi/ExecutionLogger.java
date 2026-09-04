package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;
import java.util.Objects;

/**
 * Structured execution logger supplied by the host instead of direct standard-output access.
 *
 * <p>Each convenience method snapshots attributes as immutable JSON before delegating. Implementations
 * decide transport and retention, and must avoid exposing secrets or personal data in unsafe sinks.</p>
 */
public interface ExecutionLogger {

    /** Logs debug diagnostics with a deeply immutable attribute snapshot. */
    default void debug(String message, Throwable throwable, Map<String, ?> attributes) {
        log(ExecutionLogLevel.DEBUG, message, throwable, immutableAttributes(attributes));
    }

    /** Logs informational diagnostics with a deeply immutable attribute snapshot. */
    default void info(String message, Throwable throwable, Map<String, ?> attributes) {
        log(ExecutionLogLevel.INFO, message, throwable, immutableAttributes(attributes));
    }

    /** Logs warning diagnostics with a deeply immutable attribute snapshot. */
    default void warn(String message, Throwable throwable, Map<String, ?> attributes) {
        log(ExecutionLogLevel.WARN, message, throwable, immutableAttributes(attributes));
    }

    /** Logs error diagnostics with a deeply immutable attribute snapshot. */
    default void error(String message, Throwable throwable, Map<String, ?> attributes) {
        log(ExecutionLogLevel.ERROR, message, throwable, immutableAttributes(attributes));
    }

    /**
     * Accepts one normalized log record from the convenience methods.
     *
     * @param level immutable severity
     * @param message non-blank human-readable message
     * @param throwable optional failure cause
     * @param attributes deeply immutable JSON diagnostic attributes
     */
    void log(ExecutionLogLevel level, String message, Throwable throwable, Map<String, Object> attributes);

    private static Map<String, Object> immutableAttributes(Map<String, ?> attributes) {
        Objects.requireNonNull(attributes, "log attributes must not be null");
        return JsonValues.copyObject(attributes, "log attributes");
    }
}
