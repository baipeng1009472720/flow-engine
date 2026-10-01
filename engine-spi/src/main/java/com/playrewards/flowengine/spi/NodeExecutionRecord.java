package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import java.time.Instant;

/**
 * One real node/scope/attempt. Identity survives state updates and is never reused for another
 * attempt.
 *
 * @param errorMessage 节点给出的可公开失败消息（如脚本行号、END 配置的业务消息）；未知异常为 null，
 *     不包含异常原文或宿主信息
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
        String errorCode,
        String errorMessage) {

    /** 兼容新增 errorMessage 之前的调用方，消息为空。 */
    public NodeExecutionRecord(
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
            String errorCode) {
        this(nodeExecutionId, executionId, nodeId, nodeCode, componentType, scopePath, attempt,
                status, queuedAt, startedAt, finishedAt, inputPayloadRef, outputPayloadRef, errorId,
                errorCode, null);
    }
}
