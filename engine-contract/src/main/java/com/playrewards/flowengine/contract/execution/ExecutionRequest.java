package com.playrewards.flowengine.contract.execution;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Dynamic caller input for an execution, separated from reusable static bundle data.
 *
 * <p>Input and scope values can contain credentials or personal data. They are copied as immutable JSON
 * values but intentionally omitted from {@link #toString()} so ordinary server diagnostics cannot leak
 * them. {@code contextReference} is also redacted because it may identify a protected payload.</p>
 */
public record ExecutionRequest(String flowReference, ExecutionMode mode, ExecutionPurpose purpose,
                               Map<String, Object> startInputs,
                               Map<String, Map<String, Object>> externalScopes,
                               String contextReference, String idempotencyKey, String tenantId,
                               Instant deadline, Map<String, Object> metadata) {

    public ExecutionRequest {
        flowReference = JsonValues.requireText(flowReference, "flowReference");
        Objects.requireNonNull(mode, "mode must not be null");
        Objects.requireNonNull(purpose, "purpose must not be null");
        startInputs = JsonValues.copyObject(startInputs, "startInputs");
        externalScopes = copyScopes(externalScopes);
        if (contextReference != null) {
            contextReference = JsonValues.requireText(contextReference, "contextReference");
        }
        idempotencyKey = JsonValues.requireText(idempotencyKey, "idempotencyKey");
        tenantId = JsonValues.requireText(tenantId, "tenantId");
        Objects.requireNonNull(deadline, "deadline must not be null");
        metadata = copyMetadata(metadata);
    }

    @Override
    public String toString() {
        return "ExecutionRequest[flowReference=" + flowReference + ", mode=" + mode + ", purpose=" + purpose
                + ", startInputKeys=" + startInputs.keySet() + ", externalScopeKeys=" + externalScopes.keySet()
                + ", contextReference=<redacted>, idempotencyKey=<redacted>, tenantId=" + tenantId
                + ", deadline=" + deadline + ", metadataKeys=" + metadata.keySet() + "]";
    }

    private static Map<String, Map<String, Object>> copyScopes(Map<String, Map<String, Object>> scopes) {
        Objects.requireNonNull(scopes, "externalScopes must not be null");
        Map<String, Map<String, Object>> copied = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : scopes.entrySet()) {
            copied.put(JsonValues.requireText(entry.getKey(), "externalScopes key"),
                    JsonValues.copyObject(entry.getValue(), "externalScopes." + entry.getKey()));
        }
        return Map.copyOf(copied);
    }

    private static Map<String, Object> copyMetadata(Map<String, Object> values) {
        return JsonValues.copyObject(values, "metadata");
    }
}
