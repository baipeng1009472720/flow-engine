package com.playrewards.flowengine.contract.execution;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Instant;
import java.util.Objects;

/**
 * Terminal execution summary; a detailed result payload is redacted from diagnostic rendering.
 */
public record ExecutionResult(String executionId, ExecutionStatus status, Object result,
                              String errorCode, String errorId, Instant startedAt, Instant finishedAt) {

    public ExecutionResult {
        executionId = JsonValues.requireText(executionId, "executionId");
        Objects.requireNonNull(status, "status must not be null");
        result = JsonValues.copyValue(result, "result");
        if (errorCode != null) {
            errorCode = JsonValues.requireText(errorCode, "errorCode");
        }
        if (errorId != null) {
            errorId = JsonValues.requireText(errorId, "errorId");
        }
        Objects.requireNonNull(startedAt, "startedAt must not be null");
        Objects.requireNonNull(finishedAt, "finishedAt must not be null");
    }

    @Override
    public String toString() {
        return "ExecutionResult[executionId=" + executionId + ", status=" + status
                + ", result=<redacted>, errorCode=" + errorCode + ", errorId=" + errorId
                + ", startedAt=" + startedAt + ", finishedAt=" + finishedAt + "]";
    }
}
