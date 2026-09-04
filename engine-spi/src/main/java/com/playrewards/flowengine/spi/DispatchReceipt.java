package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.json.JsonValues;

/** Stable execution identity accepted by a local-first task dispatcher. */
public record DispatchReceipt(String executionId) {

    public DispatchReceipt {
        executionId = JsonValues.requireText(executionId, "dispatch executionId");
    }
}
