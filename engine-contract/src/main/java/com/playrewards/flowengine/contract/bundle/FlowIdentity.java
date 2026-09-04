package com.playrewards.flowengine.contract.bundle;

import com.playrewards.flowengine.contract.json.JsonValues;

/** Stable tenant-scoped identity of the exact flow revision contained in a bundle. */
public record FlowIdentity(String tenantId, String flowId, String version) {

    public FlowIdentity {
        tenantId = JsonValues.requireText(tenantId, "flow tenantId");
        flowId = JsonValues.requireText(flowId, "flow flowId");
        version = JsonValues.requireText(version, "flow version");
    }
}
