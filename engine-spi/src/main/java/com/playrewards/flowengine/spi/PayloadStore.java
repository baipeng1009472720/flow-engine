package com.playrewards.flowengine.spi;

import java.util.Optional;

/**
 * Port for large or sensitive JSON payloads that should not be embedded in execution state.
 *
 * <p>Adapters generate opaque references and must return immutable JSON snapshots. Core code must
 * not log payload values or assume a storage location from the reference text.
 */
public interface PayloadStore {
    /**
     * Recovery values cannot expire with diagnostics; unsupported adapters reject durable
     * execution.
     */
    default PayloadReference storeCheckpoint(String executionId, Object payload) {
        throw new IllegalStateException("PINNED_CHECKPOINT_PAYLOAD_REQUIRED");
    }

    /**
     * Resolves ownership for authorization. Legacy adapters deny public payload access by default.
     */
    default Optional<String> executionId(PayloadReference reference) {
        return Optional.empty();
    }

    /**
     * Stores one JSON-compatible payload under an adapter-owned opaque reference.
     *
     * @param executionId non-blank stable owning execution identity
     * @param payload JSON-compatible content
     * @return opaque payload reference
     */
    PayloadReference store(String executionId, Object payload);

    /**
     * Reads a JSON-compatible immutable payload snapshot.
     *
     * @param reference opaque reference returned by {@link #store(String, Object)}
     * @return payload when retained, otherwise empty
     */
    Optional<Object> find(PayloadReference reference);
}
