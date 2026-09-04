package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;

/** Expected node-level rejection carrying a stable public error code. */
public final class NodeExecutionException extends RuntimeException {

    private final String code;
    private final ExecutionStatus terminalStatus;

    /**
     * Creates a deterministic node failure.
     *
     * @param code stable machine-readable error code
     * @param message safe execution error detail
     */
    public NodeExecutionException(String code, String message) {
        this(code, message, ExecutionStatus.FAILED);
    }

    /** Creates a deterministic failure with an explicitly supported non-success terminal state. */
    public NodeExecutionException(String code, String message, ExecutionStatus terminalStatus) {
        super(message);
        this.code = JsonValues.requireText(code, "node execution error code");
        if (terminalStatus != ExecutionStatus.FAILED && terminalStatus != ExecutionStatus.WAITING_MANUAL) {
            throw new IllegalArgumentException("node failure status must be FAILED or WAITING_MANUAL");
        }
        this.terminalStatus = terminalStatus;
    }

    /** @return stable machine-readable error code */
    public String code() {
        return code;
    }

    /** @return terminal execution state selected by this failure */
    public ExecutionStatus terminalStatus() {
        return terminalStatus;
    }
}
