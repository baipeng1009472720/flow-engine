package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.json.JsonValues;

/** Expected node-level rejection carrying a stable public error code. */
public final class NodeExecutionException extends RuntimeException {

    private final String code;

    /**
     * Creates a deterministic node failure.
     *
     * @param code stable machine-readable error code
     * @param message safe execution error detail
     */
    public NodeExecutionException(String code, String message) {
        super(message);
        this.code = JsonValues.requireText(code, "node execution error code");
    }

    /** @return stable machine-readable error code */
    public String code() {
        return code;
    }
}
