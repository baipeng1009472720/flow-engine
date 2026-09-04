package com.playrewards.flowengine.contract.execution;

import com.playrewards.flowengine.contract.json.JsonValues;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Reliable ordered lifecycle transition suitable for persistence and SSE cursors.
 *
 * <p>Attributes are JSON-only diagnostic metadata and are not rendered by {@link #toString()} because
 * an event can carry protected error context.</p>
 */
public record ExecutionEvent(String eventId, String executionId, String eventType, ExecutionStatus status,
                             String nodeId, String nodeCode, String scopePath, int attempt, long sequence,
                             Instant occurredAt, Map<String, Object> attributes) {

    public ExecutionEvent {
        eventId = JsonValues.requireText(eventId, "event eventId");
        executionId = JsonValues.requireText(executionId, "event executionId");
        if (attempt < 0) {
            throw new IllegalArgumentException("event attempt must not be negative");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("event sequence must not be negative");
        }
        eventType = JsonValues.requireText(eventType, "event eventType");
        Objects.requireNonNull(status, "event status must not be null");
        if (nodeId != null) {
            nodeId = JsonValues.requireText(nodeId, "event nodeId");
        }
        if (nodeCode != null) {
            nodeCode = JsonValues.requireText(nodeCode, "event nodeCode");
        }
        if (scopePath != null) {
            scopePath = JsonValues.requireText(scopePath, "event scopePath");
        }
        Objects.requireNonNull(occurredAt, "event occurredAt must not be null");
        attributes = JsonValues.copyObject(attributes, "event attributes");
    }

    @Override
    public String toString() {
        return "ExecutionEvent[eventId=" + eventId + ", executionId=" + executionId + ", sequence=" + sequence
                + ", eventType=" + eventType + ", status=" + status + ", nodeId=" + nodeId + ", nodeCode="
                + nodeCode + ", scopePath=" + scopePath + ", attempt=" + attempt + ", occurredAt=" + occurredAt
                + ", attributes=<redacted>]";
    }
}
