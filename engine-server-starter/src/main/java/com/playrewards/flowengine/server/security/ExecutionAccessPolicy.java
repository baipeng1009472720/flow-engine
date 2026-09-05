package com.playrewards.flowengine.server.security;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Trusted host authentication and per-flow authorization. Request bodies and tenant headers grant
 * no authority.
 */
public interface ExecutionAccessPolicy {
    enum Action {
        EXECUTE,
        READ,
        LOGS,
        PAYLOAD,
        CANCEL
    }

    /**
     * Resolves the authenticated tenant or throws SecurityException; never trust arbitrary request
     * metadata.
     */
    String tenantId(HttpServletRequest request);

    /** Authorizes the specific operation and flow, including independent full-payload access. */
    void authorize(String tenantId, FlowIdentity flow, Action action);
}
