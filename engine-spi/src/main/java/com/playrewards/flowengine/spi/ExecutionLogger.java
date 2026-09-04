package com.playrewards.flowengine.spi;

import java.util.Map;

/**
 * Structured execution logger supplied by the host instead of direct standard-output access.
 *
 * <p>Each convenience method constructs an immutable {@link ExecutionLogEntry} before delegation.
 * Implementations decide transport and retention, and must avoid exposing secrets or personal data in
 * unsafe sinks.</p>
 */
public interface ExecutionLogger {

    /** Logs debug diagnostics with a deeply immutable attribute snapshot. */
    default void debug(String message, Throwable throwable, Map<String, ?> attributes) {
        write(ExecutionLogEntry.of(ExecutionLogLevel.DEBUG, message, throwable, attributes));
    }

    /** Logs informational diagnostics with a deeply immutable attribute snapshot. */
    default void info(String message, Throwable throwable, Map<String, ?> attributes) {
        write(ExecutionLogEntry.of(ExecutionLogLevel.INFO, message, throwable, attributes));
    }

    /** Logs warning diagnostics with a deeply immutable attribute snapshot. */
    default void warn(String message, Throwable throwable, Map<String, ?> attributes) {
        write(ExecutionLogEntry.of(ExecutionLogLevel.WARN, message, throwable, attributes));
    }

    /** Logs error diagnostics with a deeply immutable attribute snapshot. */
    default void error(String message, Throwable throwable, Map<String, ?> attributes) {
        write(ExecutionLogEntry.of(ExecutionLogLevel.ERROR, message, throwable, attributes));
    }

    /**
     * Accepts one already-normalized immutable log record from the convenience methods.
     *
     * @param entry immutable diagnostic entry; throwables are separated from JSON attributes
     */
    void write(ExecutionLogEntry entry);
}
