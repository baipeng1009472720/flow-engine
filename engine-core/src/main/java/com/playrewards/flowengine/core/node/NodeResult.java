package com.playrewards.flowengine.core.node;

import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.json.JsonValues;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable final node output, activated control ports and optional terminal flow outcome. */
public record NodeResult(Map<String, Object> outputs, Set<String> activatedPorts,
                         TerminalOutcome terminalOutcome) {

    public NodeResult {
        outputs = JsonValues.copyObject(outputs, "node result outputs");
        Objects.requireNonNull(activatedPorts, "activated ports must not be null");
        Set<String> ports = new LinkedHashSet<>();
        activatedPorts.forEach(port -> ports.add(JsonValues.requireText(port, "activated port")));
        activatedPorts = Set.copyOf(ports);
    }

    /** Creates a non-terminal result that continues through selected semantic ports. */
    public static NodeResult continueWith(Map<String, Object> outputs, Set<String> activatedPorts) {
        return new NodeResult(outputs, activatedPorts, null);
    }

    /** Creates a successful END outcome carrying optional business data. */
    public static NodeResult successfulEnd(Object result) {
        return new NodeResult(Map.of(), Set.of(),
                new TerminalOutcome(ExecutionStatus.SUCCEEDED, result, null, null));
    }

    /** Creates a failed END outcome with no business result. */
    public static NodeResult failedEnd(String errorCode, String errorMessage) {
        return new NodeResult(Map.of(), Set.of(),
                new TerminalOutcome(ExecutionStatus.FAILED, null, errorCode, errorMessage));
    }

    /** Terminal data produced by END before it is mapped to the public execution contract. */
    public record TerminalOutcome(ExecutionStatus status, Object result, String errorCode, String errorMessage) {

        public TerminalOutcome {
            Objects.requireNonNull(status, "terminal status must not be null");
            if (status != ExecutionStatus.SUCCEEDED && status != ExecutionStatus.FAILED) {
                throw new IllegalArgumentException("END terminal status must be SUCCEEDED or FAILED");
            }
            result = JsonValues.copyValue(result, "terminal result");
            if (status == ExecutionStatus.SUCCEEDED) {
                if (errorCode != null || errorMessage != null) {
                    throw new IllegalArgumentException("successful END must not carry error fields");
                }
            } else {
                errorCode = JsonValues.requireText(errorCode, "failed END errorCode");
                if (errorMessage != null) {
                    errorMessage = JsonValues.requireText(errorMessage, "failed END errorMessage");
                }
                if (result != null) {
                    throw new IllegalArgumentException("failed END must not carry business result");
                }
            }
        }
    }
}
