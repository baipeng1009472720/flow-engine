package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ContentChunk;

import java.util.*;

/** Separate bounded observation tables; access authorization belongs to the host-facing service. */
public interface ExecutionRecordStore extends ExecutionObservationStore {
    /** Durable terminal audit validates the live root execution fence in the same transaction. */
    default void commitTerminal(
            ExecutionRecord record,
            com.playrewards.flowengine.contract.execution.ExecutionEvent event,
            ExecutionLease lease) {
        throw new IllegalStateException("FENCED_TERMINAL_COMMIT_REQUIRED");
    }

    /** 原子提交终态记录及终态事件；返回即关键记录已持久化。失败不得暴露部分成功终态。 宿主必须提供真实事务实现，不能使用 saveExecution + append 模拟原子性。 */
    default void commitTerminal(
            ExecutionRecord record,
            com.playrewards.flowengine.contract.execution.ExecutionEvent event) {
        throw new IllegalStateException("ATOMIC_TERMINAL_COMMIT_REQUIRED");
    }

    /** Reserves capacity before any execution side effect; active entries cannot be evicted. */
    void saveExecution(ExecutionRecord record);

    Optional<ExecutionRecord> execution(String id);

    void saveNode(NodeExecutionRecord record);

    Optional<NodeExecutionRecord> node(String id);

    List<NodeExecutionRecord> nodes(String executionId);

    void appendLog(ExecutionLogRecord record);

    List<ExecutionLogRecord> logs(String nodeExecutionId);

    void appendContent(String nodeExecutionId, ContentChunk chunk);

    List<ContentChunk> content(String nodeExecutionId);

    /** Ensures critical states and ERROR logs are visible before a final response. */
    void flush(String executionId);
}
