package com.playrewards.flowengine.spi;

import java.util.Optional;

/**
 * Atomic idempotency-claim port with no dependency on a specific persistence primitive.
 *
 * <p>{@link #claim(IdempotencyRecord)} must be linearizable for a given key: concurrent callers see
 * exactly one winner record. It does not start, cancel, or execute a flow.</p>
 */
public interface IdempotencyStore {

    /**
     * Finds the current winner for an idempotency key.
     *
     * @param key stable tenant-scoped request identity
     * @return winner record, or empty before the first successful claim
     */
    Optional<IdempotencyRecord> find(IdempotencyKey key);

    /**
     * Atomically retains the existing winner or installs the supplied first winner.
     *
     * @param candidate candidate execution association
     * @return stored winner; equal to {@code candidate} only when this caller won or had already won
     */
    IdempotencyRecord claim(IdempotencyRecord candidate);
}
