package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.Map;
import java.util.Objects;

/**
 * Durable opaque checkpoint owned by the Core and keyed by a stable execution identity.
 *
 * <p>The state body is JSON-only so storage adapters cannot inject their persistence objects into the
 * Core. {@code revision} supports adapter-level optimistic concurrency without exposing a database API.</p>
 */
public record ExecutionState(String executionId, long revision, ExecutionStatus status,
                             Map<String, Object> state) {

    public ExecutionState {
        executionId = JsonValues.requireText(executionId, "execution state executionId");
        if (revision < 0) {
            throw new IllegalArgumentException("execution state revision must not be negative");
        }
        Objects.requireNonNull(status, "execution state status must not be null");
        state = JsonValues.copyObject(state, "execution state");
    }
}
