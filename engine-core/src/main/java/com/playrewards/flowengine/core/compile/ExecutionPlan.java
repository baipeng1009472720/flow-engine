package com.playrewards.flowengine.core.compile;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;

/**
 * Deeply immutable, fully resolved execution topology.
 *
 * <p>The plan retains the source bundle hash, semantic port indexes, activation predecessors,
 * structural pairs and recursively compiled sub-flows so execution never rereads mutable host state.</p>
 */
public record ExecutionPlan(
        FlowIdentity flowIdentity,
        String bundleHash,
        Map<String, CompiledNode> nodesById,
        Map<String, List<String>> forwardAdjacency,
        Map<String, List<String>> reverseAdjacency,
        Map<String, Map<String, List<String>>> outgoingByPort,
        Map<String, Map<String, List<String>>> incomingByPort,
        Map<String, List<String>> activationPredecessors,
        Map<String, String> loopPairs,
        Map<String, String> lockPairs,
        Map<String, ExecutionPlan> subFlowPlans) {

    public ExecutionPlan {
        Objects.requireNonNull(flowIdentity, "flowIdentity must not be null");
        Objects.requireNonNull(bundleHash, "bundleHash must not be null");
        nodesById = immutableMap(nodesById, "nodesById must not be null");
        forwardAdjacency = copyListMap(forwardAdjacency);
        reverseAdjacency = copyListMap(reverseAdjacency);
        outgoingByPort = copyNestedListMap(outgoingByPort);
        incomingByPort = copyNestedListMap(incomingByPort);
        activationPredecessors = copyListMap(activationPredecessors);
        loopPairs = immutableMap(loopPairs, "loopPairs must not be null");
        lockPairs = immutableMap(lockPairs, "lockPairs must not be null");
        subFlowPlans = immutableMap(subFlowPlans, "subFlowPlans must not be null");
    }

    private static Map<String, List<String>> copyListMap(Map<String, List<String>> source) {
        Objects.requireNonNull(source, "plan index must not be null");
        Map<String, List<String>> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, List.copyOf(value)));
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, Map<String, List<String>>> copyNestedListMap(
            Map<String, Map<String, List<String>>> source) {
        Objects.requireNonNull(source, "semantic port index must not be null");
        Map<String, Map<String, List<String>>> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, copyListMap(value)));
        return Collections.unmodifiableMap(copy);
    }

    private static <T> Map<String, T> immutableMap(Map<String, T> source, String message) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(source, message)));
    }
}
