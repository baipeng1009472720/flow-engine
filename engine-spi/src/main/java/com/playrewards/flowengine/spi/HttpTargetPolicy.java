package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import java.net.URI;

/**
 * Validates one HTTP destination at every transport boundary.
 *
 * <p>Implementations own DNS and address classification. Core supplies only the immutable URI and
 * execution purpose, while transports must invoke the policy before connection and again for every
 * final/redirect destination.</p>
 */
@FunctionalInterface
public interface HttpTargetPolicy {

    /**
     * Rejects an unsafe destination without exposing its query or credentials.
     *
     * @param uri requested destination
     * @param purpose production or isolated trial policy selector
     */
    void validate(URI uri, ExecutionPurpose purpose);

    /** Safe policy rejection which never includes the rejected raw URI. */
    final class TargetRejectedException extends RuntimeException {

        /** Creates a redaction-safe target rejection. */
        public TargetRejectedException(String ignoredMessage) {
            super("HTTP target rejected by policy");
        }

        /** Creates a redaction-safe target rejection while discarding the potentially sensitive cause. */
        public TargetRejectedException(String ignoredMessage, Throwable cause) {
            // DNS/URI causes can contain target material, so the public policy exception discards it.
            super("HTTP target rejected by policy");
        }
    }
}
