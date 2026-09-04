package com.playrewards.flowengine.contract.execution;

import com.playrewards.flowengine.contract.json.JsonValues;

/**
 * User-visible incremental content emitted by one node.
 *
 * <p>Sequence is monotonic per node and scope path. Payload is JSON-only and excluded from diagnostic
 * output because stream content can contain user data.</p>
 */
public record ContentChunk(String executionId, String nodeCode, String scopePath, long sequence,
                           String contentType, Object payload) {

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
        return "ContentChunk[executionId=" + executionId + ", nodeCode=" + nodeCode + ", scopePath="
                + scopePath + ", sequence=" + sequence + ", contentType=" + contentType
                + ", payload=<redacted>]";
    }
}
