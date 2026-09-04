package com.playrewards.flowengine.contract.component;

/**
 * Declares a component's externally observable effect for scheduling, retries and trial policy.
 */
public enum ComponentEffect {
    PURE,
    IDEMPOTENT,
    SIDE_EFFECT
}
