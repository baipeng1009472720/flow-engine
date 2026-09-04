package com.playrewards.flowengine.contract.execution;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Instant;
import java.util.Objects;

/**
 * Terminal execution summary; a detailed result payload is redacted from diagnostic rendering.
 */
public record ExecutionResult(String executionId, ExecutionStatus status, Object result,
                              String errorCode, String errorMessage, String errorId,
                              Instant startedAt, Instant finishedAt) {

    public ExecutionResult {
        executionId = JsonValues.requireText(executionId, "executionId");
        Objects.requireNonNull(status, "status must not be null");
        if (status != ExecutionStatus.SUCCEEDED
                && status != ExecutionStatus.FAILED
                && status != ExecutionStatus.CANCELLED
                && status != ExecutionStatus.WAITING_MANUAL) {
            throw new IllegalArgumentException("execution result status must be terminal");
        }
        result = JsonValues.copyValue(result, "result");
        if (errorCode != null) {
            errorCode = JsonValues.requireText(errorCode, "errorCode");
        }
        if (errorMessage != null) {
            errorMessage = JsonValues.requireText(errorMessage, "errorMessage");
        }
        if (errorId != null) {
            errorId = JsonValues.requireText(errorId, "errorId");
        }
        if (status == ExecutionStatus.SUCCEEDED
                && (errorCode != null || errorMessage != null || errorId != null)) {
            throw new IllegalArgumentException("successful execution must not carry error fields");
        }
        if (status == ExecutionStatus.FAILED || status == ExecutionStatus.CANCELLED
                || status == ExecutionStatus.WAITING_MANUAL) {
            if (errorCode == null) {
                throw new IllegalArgumentException("unsuccessful execution errorCode must not be null");
            }
            if (result != null) {
                throw new IllegalArgumentException("unsuccessful execution must not carry business result");
            }
        }
        Objects.requireNonNull(startedAt, "startedAt must not be null");
        Objects.requireNonNull(finishedAt, "finishedAt must not be null");
    }

    /**
     * Preserves source compatibility for callers created before optional error messages were added.
     *
     * @param executionId unique run identity
     * @param status terminal lifecycle status
     * @param result optional successful business result
     * @param errorCode stable failure code, required for FAILED/CANCELLED and forbidden for SUCCEEDED
     * @param errorId optional diagnostic correlation identity
     * @param startedAt execution start time
     * @param finishedAt execution finish time
     */
    public ExecutionResult(String executionId, ExecutionStatus status, Object result,
            String errorCode, String errorId, Instant startedAt, Instant finishedAt) {
        this(executionId, status, result, errorCode, null, errorId, startedAt, finishedAt);
    }

    @Override
    public String toString() {
        return "ExecutionResult[executionId=" + executionId + ", status=" + status
                + ", result=<redacted>, errorCode=" + errorCode
                + ", errorMessagePresent=" + (errorMessage != null) + ", errorId=" + errorId
                + ", startedAt=" + startedAt + ", finishedAt=" + finishedAt + "]";
    }
}
