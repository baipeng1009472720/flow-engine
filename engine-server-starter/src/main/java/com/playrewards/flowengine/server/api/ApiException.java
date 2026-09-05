package com.playrewards.flowengine.server.api;

/**
 * Safe, explicit HTTP error owned by the server boundary; never wraps private upstream messages.
 */
public final class ApiException extends RuntimeException {
    private final int status;
    private final String code, category;

    public ApiException(int status, String code, String category) {
        super(code);
        this.status = status;
        this.code = code;
        this.category = category;
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    public String category() {
        return category;
    }
}
