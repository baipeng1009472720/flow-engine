package com.playrewards.flowengine.contract.component;

import java.util.Objects;

/**
 * Execution guarantees and capacity advertised by a resolved component contract.
 *
 * <p>The compiler uses this declaration to decide whether retries and trial runs are legal; it does
 * not grant side-effect safety by itself.</p>
 */
public record ComponentCapabilities(ComponentEffect effect, boolean supportsTrial, boolean streaming,
                                    boolean retryable, int maxConcurrency) {

    public ComponentCapabilities {
        Objects.requireNonNull(effect, "component effect must not be null");
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be at least one");
        }
    }
}
