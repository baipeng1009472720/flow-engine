package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;

/** Opaque stable identity for JSON payload content held outside an execution checkpoint. */
public record PayloadReference(String value) {

    public PayloadReference {
        value = JsonValues.requireText(value, "payload reference");
    }

    @Override
    public String toString() {
        return "PayloadReference[value=<redacted>]";
    }
}
