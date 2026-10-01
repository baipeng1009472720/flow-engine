package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.execution.*;
import java.time.Instant;

/**
 * Observation snapshot only; never a durable recovery checkpoint. Payloads are separately
 * authorized.
 *
 * @param errorMessage 终态失败的可定位摘要：失败 END 的业务消息原文，或“失败节点 + 错误码 + 节点消息”；
 *     不包含异常原文、堆栈或宿主信息
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
        long eventCursor,
        String errorMessage) {

    /** 兼容新增 errorMessage 之前的调用方，消息为空。 */
    public ExecutionRecord(
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
            long eventCursor) {
        this(executionId, rootExecutionId, parentExecutionId, flowIdentity, bundleHash, mode,
                purpose, status, startedAt, finishedAt, resultPayloadRef, errorId, errorCode,
                eventCursor, null);
    }
}
