package com.playrewards.flowengine.contract.execution;

import com.playrewards.flowengine.contract.json.JsonValues;

/**
 * User-visible incremental content emitted by one node.
 *
 * <p>Sequence is monotonic per node and scope path. Payload is JSON-only and excluded from
 * diagnostic output because stream content can contain user data.
 */
public record ContentChunk(
        String executionId,
        String nodeCode,
        String scopePath,
        long sequence,
        String contentType,
        Object payload) {

    public ContentChunk {
        executionId = JsonValues.requireText(executionId, "chunk executionId");
        nodeCode = JsonValues.requireText(nodeCode, "chunk nodeCode");
        scopePath = JsonValues.requireText(scopePath, "chunk scopePath");
        if (sequence < 0) {
            throw new IllegalArgumentException("chunk sequence must not be negative");
        }
        contentType = JsonValues.requireText(contentType, "chunk contentType");
        payload = JsonValues.copyValue(payload, "chunk payload");
    }

    @Override
    public String toString() {
        return "ContentChunk[executionId="
                + executionId
                + ", nodeCode="
                + nodeCode
                + ", scopePath="
                + scopePath
                + ", sequence="
                + sequence
                + ", contentType="
                + contentType
                + ", payload=<redacted>]";
    }

    /** 在订阅接收和存储边界检查整个分片，包含不可信元数据；拒绝必须发生在写入前。 */
    public void validateResourceLimits() {
        JsonValues.measureJsonResources(
                java.util.Arrays.asList(
                        executionId, nodeCode, scopePath, sequence, contentType, payload),
                new JsonValues.JsonResourceLimits(16384, 8192, 16, 1024));
    }
}
