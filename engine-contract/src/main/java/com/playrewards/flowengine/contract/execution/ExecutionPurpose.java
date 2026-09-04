package com.playrewards.flowengine.contract.execution;

/** Caller intent used to enforce production versus isolated trial policies. */
public enum ExecutionPurpose {
    PRODUCTION,
    TRIAL
}
