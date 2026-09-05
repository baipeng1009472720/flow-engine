package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;

import java.time.Instant;

/**
 * One real node/scope/attempt. Identity survives state updates and is never reused for another
 * attempt.
 */
public record NodeExecutionRecord(
        String nodeExecutionId,
        String executionId,
        String nodeId,
        String nodeCode,
        ComponentType componentType,
        String scopePath,
        int attempt,
        ExecutionStatus status,
        Instant queuedAt,
        Instant startedAt,
        Instant finishedAt,
        String inputPayloadRef,
        String outputPayloadRef,
        String errorId,
        String errorCode) {}
