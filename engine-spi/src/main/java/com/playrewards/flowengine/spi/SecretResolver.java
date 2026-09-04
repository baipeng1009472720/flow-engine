package com.playrewards.flowengine.spi;

import java.util.Optional;

/**
 * Resolves externally managed secret material without coupling the Core to a vault provider.
 *
 * <p>Implementations authorize tenant access and must never log raw secret contents. Components must
 * dispose of returned values as soon as their downstream call completes.</p>
 */
public interface SecretResolver {

    /**
     * Resolves currently available secret material.
     *
     * @param reference opaque tenant-scoped secret locator
     * @return short-lived secret material, or empty when unavailable
     * @throws SecurityException when the caller is not authorized to resolve the reference
     */
    Optional<SecretValue> resolve(SecretReference reference);
}
