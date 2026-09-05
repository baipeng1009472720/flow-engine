package com.playrewards.flowengine.spi;

import java.time.Instant;

/**
 * Lease identity is an unforgeable-by-convention write capability; adapters revalidate every write.
 */
public record ExecutionLease(
        String tenantId, String executionId, String owner, long fencingToken, Instant expiresAt) {}
