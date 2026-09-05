package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;

import java.time.Instant;
import java.util.Map;

/** Sanitized diagnostic with engine-bound identity and node-local monotonic sequence. */
public record ExecutionLogRecord(
        String logId,
        String executionId,
        String nodeExecutionId,
        Instant timestamp,
        ExecutionLogLevel level,
        String source,
        long sequence,
        String message,
        Map<String, Object> maskedAttributes,
        String errorId) {
    public ExecutionLogRecord {
        maskedAttributes = JsonValues.copyObject(maskedAttributes, "log attributes");
    }
}
