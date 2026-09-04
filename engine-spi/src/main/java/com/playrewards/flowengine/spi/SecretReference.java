package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;

/** Opaque tenant-scoped locator for secret material managed outside the flow bundle. */
public record SecretReference(String tenantId, String name) {

    public SecretReference {
        tenantId = JsonValues.requireText(tenantId, "secret tenantId");
        name = JsonValues.requireText(name, "secret name");
    }
}
