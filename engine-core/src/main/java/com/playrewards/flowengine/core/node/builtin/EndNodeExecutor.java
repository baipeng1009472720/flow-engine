package com.playrewards.flowengine.core.node.builtin;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.execution.ExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import java.util.Map;

/** Maps the current END configuration to one terminal success or failure outcome. */
public final class EndNodeExecutor implements NodeExecutor {

    @Override
    public ComponentType supports() {
        return ComponentType.END;
    }

    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        Map<String, Object> config = context.node().node().config();
        String status = text(config.get("resultStatus"));
        if ("SUCCESS".equals(status)) {
            if (config.containsKey("errorCode") || config.containsKey("errorMessage")) {
                throw invalid("successful END must not contain errorCode or errorMessage");
            }
            Object binding = config.get("returnBinding");
            if (binding == null) {
                return NodeInvocation.completed(NodeResult.successfulEnd(null));
            }
            Source source = source(binding, "returnBinding");
            ExecutionContext.ContextValue value = context.lookupSource(source.nodeId(), source.outputKey());
            if (!value.present()) {
                throw new NodeExecutionException("END_RETURN_VALUE_MISSING",
                        "successful END return source has no value");
            }
            return NodeInvocation.completed(NodeResult.successfulEnd(value.value()));
        }
        if ("FAILED".equals(status)) {
            if (config.containsKey("returnBinding")) {
                throw invalid("failed END must not contain returnBinding");
            }
            String errorCode = text(config.get("errorCode"));
            if (errorCode == null) {
                throw invalid("failed END requires non-blank errorCode");
            }
            Object rawMessage = config.get("errorMessage");
            String errorMessage = rawMessage == null ? null : text(rawMessage);
            if (rawMessage != null && errorMessage == null) {
                throw invalid("failed END errorMessage must be non-blank when present");
            }
            return NodeInvocation.completed(NodeResult.failedEnd(errorCode, errorMessage));
        }
        throw invalid("END resultStatus must be SUCCESS or FAILED");
    }

    private static Source source(Object value, String field) {
        if (!(value instanceof Map<?, ?> binding)) {
            throw invalid(field + " must be an object");
        }
        String nodeId = text(binding.get("sourceNodeId"));
        String outputKey = text(binding.get("sourceOutputKey"));
        if (nodeId == null || outputKey == null) {
            throw invalid(field + " requires sourceNodeId and sourceOutputKey");
        }
        return new Source(nodeId, outputKey);
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private static NodeExecutionException invalid(String message) {
        return new NodeExecutionException("END_CONFIG_INVALID", message);
    }

    private record Source(String nodeId, String outputKey) {
    }
}
