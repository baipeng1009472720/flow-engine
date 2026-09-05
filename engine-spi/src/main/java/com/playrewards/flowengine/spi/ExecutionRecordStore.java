package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ContentChunk;

import java.util.*;

/** Separate bounded observation tables; access authorization belongs to the host-facing service. */
public interface ExecutionRecordStore extends ExecutionObservationStore {
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
