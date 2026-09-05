package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.execution.*;

import java.time.Instant;

/**
 * Observation snapshot only; never a durable recovery checkpoint. Payloads are separately
 * authorized.
 */
public record ExecutionRecord(
        String executionId,
        String rootExecutionId,
        String parentExecutionId,
        FlowIdentity flowIdentity,
        String bundleHash,
        ExecutionMode mode,
        ExecutionPurpose purpose,
        ExecutionStatus status,
        Instant startedAt,
        Instant finishedAt,
        String resultPayloadRef,
        String errorId,
        String errorCode,
        long eventCursor) {}
