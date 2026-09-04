package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;

/**
 * Stable identity supplied when resolving external read-only execution scopes.
 *
 * <p>This request intentionally excludes execution inputs, scopes, metadata and deadlines. A context
 * loader receives only the reference and tenant identity needed to authorize and locate scopes.</p>
 */
public record ContextLoadRequest(String contextReference, String tenantId) {

    public ContextLoadRequest {
        contextReference = JsonValues.requireText(contextReference, "context reference");
        tenantId = JsonValues.requireText(tenantId, "context tenantId");
    }
}
