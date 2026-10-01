package com.playrewards.flowengine.server.security;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Trusted host authentication and per-flow authorization. Request bodies and tenant headers grant
 * no authority.
 */
public interface ExecutionAccessPolicy {
    /**
     * Immutable, thread-safe authorization snapshot created from a trusted request. Implementations
     * must not retain the servlet request or any thread-bound security context.
     */
    interface CapturedAuthorization {
        /** Returns the trusted tenant used for engine-level isolation checks. */
        String tenantId();
    }

    enum Action {
        EXECUTE,
        READ,
        LOGS,
        PAYLOAD,
        CANCEL
    }

    /**
     * Authenticates on the request thread and captures every trusted claim needed by later
     * authorization, including work performed on asynchronous sender threads.
     */
    CapturedAuthorization capture(HttpServletRequest request);

    /**
     * Authorizes the specific operation and flow. The tenant is only an isolation key; policies
     * must also apply the captured caller's permissions and reject snapshots they did not issue.
     */
    void authorize(CapturedAuthorization caller, FlowIdentity flow, Action action);
}
