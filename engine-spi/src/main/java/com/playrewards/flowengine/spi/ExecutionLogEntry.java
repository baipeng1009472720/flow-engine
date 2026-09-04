package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable structured diagnostic handed to an execution-log sink.
 *
 * <p>Attributes are recursively copied as JSON at construction. The optional throwable remains a
 * separate field so it can never be nested in attributes or accidentally rendered by {@link #toString()}.</p>
 */
public record ExecutionLogEntry(ExecutionLogLevel level, String message, Throwable throwable,
                                Map<String, Object> attributes) {

    public ExecutionLogEntry {
        Objects.requireNonNull(level, "execution log level must not be null");
        message = JsonValues.requireText(message, "execution log message");
        attributes = JsonValues.copyObject(attributes, "execution log attributes");
    }

    /**
     * Creates an immutable entry from caller-provided JSON attributes of any compatible map subtype.
     *
     * @param level non-null diagnostic severity
     * @param message non-blank human-readable message
     * @param throwable optional failure cause retained outside attributes
     * @param attributes JSON-compatible attributes copied before the entry is returned
     * @return immutable log entry safe to hand to an {@link ExecutionLogger} sink
     */
    public static ExecutionLogEntry of(ExecutionLogLevel level, String message, Throwable throwable,
                                       Map<String, ?> attributes) {
        return new ExecutionLogEntry(level, message, throwable,
                JsonValues.copyObject(attributes, "execution log attributes"));
    }

    @Override
    public String toString() {
        return "ExecutionLogEntry[level=" + level + ", message=" + message
                + ", throwable=<redacted>, attributes=<redacted>]";
    }
}
