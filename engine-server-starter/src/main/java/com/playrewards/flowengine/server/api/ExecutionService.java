package com.playrewards.flowengine.server.api;

import com.alibaba.fastjson2.*;
import com.alibaba.fastjson2.schema.JSONSchema;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.Action;
import com.playrewards.flowengine.spi.*;

import jakarta.servlet.http.HttpServletRequest;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Authenticates before parsing/loading and holds only active cancellation handles; observations own
 * retention.
 */
public final class ExecutionService {
    private final FlowEngine engine;
    private final FlowCompiler compiler;
    private final FlowBusinessComponentRegistry components;
    private final Map<String, FlowBundleLoader> loaders;
    private final ExecutionContextLoader contexts;
    private final ExecutionAccessPolicy access;
    private final ExecutionRecordStore store;
    private final PayloadStore payloads;
    private final Duration maxDuration;
    private final ConcurrentHashMap<String, ExecutionHandle> handles = new ConcurrentHashMap<>();

    public ExecutionService(
            FlowEngine engine,
            FlowCompiler compiler,
            FlowBusinessComponentRegistry components,
            List<FlowBundleLoader> loaders,
            ExecutionContextLoader contexts,
            ExecutionAccessPolicy access,
            ExecutionRecordStore store,
            PayloadStore payloads,
            Duration maxDuration) {
        this.engine = engine;
        this.compiler = compiler;
        this.components = components;
        this.contexts = contexts;
        this.access = access;
        this.store = store;
        this.payloads = payloads;
        if (maxDuration.isZero()
                || maxDuration.isNegative()
                || maxDuration.compareTo(Duration.ofMinutes(5)) > 0)
            throw new IllegalArgumentException(
                    "execution max duration must be positive and at most five minutes");
        this.maxDuration = maxDuration;
        Map<String, FlowBundleLoader> index = new HashMap<>();
        for (var loader : loaders)
            if (index.putIfAbsent(loader.scheme(), loader) != null)
                throw new IllegalArgumentException("duplicate flow loader");
        this.loaders = Map.copyOf(index);
    }

    /**
     * Starts a trusted, compiled EPHEMERAL flow. A body may reference a loader but cannot supply a
     * bundle or class.
     */
    public Started start(HttpServletRequest http) throws Exception {
        String tenant = access.tenantId(http);
        byte[] bytes = http.getInputStream().readNBytes(262145);
        if (bytes.length > 262144)
            throw new ApiException(413, "REQUEST_TOO_LARGE", "RESOURCE_LIMIT");
        JSONObject json = JSON.parseObject(bytes);
        if (json == null) throw new IllegalArgumentException("request required");
        for (String key : json.keySet())
            if (!Set.of(
                            "flowReference",
                            "mode",
                            "purpose",
                            "startInputs",
                            "externalScopes",
                            "contextReference",
                            "idempotencyKey",
                            "tenantId",
                            "deadline",
                            "metadata",
                            "responseMode")
                    .contains(key)) throw new IllegalArgumentException("unknown request field");
        if (json.containsKey("tenantId") && !tenant.equals(json.getString("tenantId")))
            throw new SecurityException();
        String responseMode = json.getString("responseMode");
        if (responseMode == null) responseMode = "WAIT";
        if (!Set.of("WAIT", "ASYNC").contains(responseMode))
            throw new IllegalArgumentException("invalid response mode");
        json.remove("responseMode");
        json.put("tenantId", tenant);
        json.putIfAbsent("mode", "EPHEMERAL");
        json.putIfAbsent("purpose", "TRIAL");
        json.putIfAbsent("startInputs", Map.of());
        json.putIfAbsent("externalScopes", Map.of());
        json.putIfAbsent("metadata", Map.of());
        json.putIfAbsent("idempotencyKey", UUID.randomUUID().toString());
        json.putIfAbsent(
                "deadline",
                Instant.now()
                        .plus(
                                maxDuration.compareTo(Duration.ofSeconds(30)) > 0
                                        ? Duration.ofSeconds(30)
                                        : maxDuration)
                        .toString());
        ExecutionRequest request = json.to(ExecutionRequest.class);
        if (request.mode() != ExecutionMode.EPHEMERAL)
            throw new ApiException(409, "EXECUTION_MODE_UNSUPPORTED", "CONFIGURATION");
        if (!request.deadline().isAfter(Instant.now())
                || request.deadline().isAfter(Instant.now().plus(maxDuration)))
            throw new IllegalArgumentException("deadline outside bounds");
        String scheme = request.flowReference().split(":", 2)[0];
        FlowBundleLoader loader = loaders.get(scheme);
        if (loader == null) throw new IllegalArgumentException("unsupported flow reference");
        ExecutableFlowBundle bundle = loader.load(request.flowReference());
        authorize(tenant, bundle.flowIdentity(), Action.EXECUTE);
        if (request.contextReference() != null) {
            if (!request.externalScopes().isEmpty())
                throw new IllegalArgumentException("ambiguous external scopes");
            var scopes =
                    contexts.load(new ContextLoadRequest(request.contextReference(), tenant))
                            .scopes();
            request =
                    new ExecutionRequest(
                            request.flowReference(),
                            request.mode(),
                            request.purpose(),
                            request.startInputs(),
                            scopes,
                            request.contextReference(),
                            request.idempotencyKey(),
                            tenant,
                            request.deadline(),
                            request.metadata());
        }
        validateScopes(bundle, request);
        ExecutionPlan plan = compiler.compile(bundle);
        validateComponents(plan, tenant);
        ExecutionHandle handle = engine.execute(plan, request);
        handles.put(handle.executionId(), handle);
        handle.completion()
                .whenComplete((value, error) -> handles.remove(handle.executionId(), handle));
        return new Started(handle, responseMode);
    }

    private void validateScopes(ExecutableFlowBundle bundle, ExecutionRequest request) {
        if (!bundle.externalScopeSchemas().keySet().equals(request.externalScopes().keySet()))
            throw new IllegalArgumentException("external scope contract mismatch");
        bundle.externalScopeSchemas()
                .forEach(
                        (name, schema) -> {
                            if (!JSONSchema.of(new JSONObject(schema.schema()))
                                    .validate(request.externalScopes().get(name))
                                    .isSuccess())
                                throw new IllegalArgumentException(
                                        "external scope schema mismatch");
                        });
    }

    private void validateComponents(ExecutionPlan plan, String tenant) {
        authorize(tenant, plan.flowIdentity(), Action.EXECUTE);
        plan.nodesById()
                .values()
                .forEach(
                        node -> {
                            if (node.componentContract().componentType()
                                    == com.playrewards.flowengine.contract.component.ComponentType
                                            .BUSINESS)
                                components.required(
                                        node.componentContract().executorRef(),
                                        node.node().nodeCode());
                        });
        plan.subFlowPlans().values().forEach(child -> validateComponents(child, tenant));
    }

    private void authorize(String tenant, FlowIdentity flow, Action action) {
        if (!tenant.equals(flow.tenantId())) throw new SecurityException();
        access.authorize(tenant, flow, action);
    }

    /**
     * Authorizes every lookup before returning a snapshot, including child execution identities.
     */
    public ExecutionRecord execution(HttpServletRequest http, String id, Action action) {
        String tenant = access.tenantId(http);
        var record =
                store.execution(id).orElseThrow(() -> new ApiException(404, "NOT_FOUND", "INPUT"));
        authorize(tenant, record.flowIdentity(), action);
        return record;
    }

    public NodeExecutionRecord node(HttpServletRequest http, String id, Action action) {
        access.tenantId(http);
        var node = store.node(id).orElseThrow(() -> new ApiException(404, "NOT_FOUND", "INPUT"));
        execution(http, node.executionId(), action);
        return node;
    }

    public Object payload(HttpServletRequest http, String id) {
        access.tenantId(http);
        var reference = new PayloadReference(id);
        String owner =
                payloads.executionId(reference)
                        .orElseThrow(() -> new ApiException(404, "NOT_FOUND", "INPUT"));
        execution(http, owner, Action.PAYLOAD);
        return payloads.find(reference).orElse(null);
    }

    /**
     * Captures trusted identity before async handoff; thread-local authentication is not read on
     * sender workers.
     */
    public String streamTenant(HttpServletRequest request) {
        return access.tenantId(request);
    }

    /**
     * Rehydrates content only after separate PAYLOAD authorization for its actual owning flow,
     * including child flows.
     */
    public ExecutionEvent streamEvent(String tenant, ExecutionEvent event) {
        if (!eventVisible(tenant, event)) return null;
        Object rawReference = event.attributes().get("contentPayloadRef");
        if (!(rawReference instanceof String reference)) return event;
        var ref = new PayloadReference(reference);
        var owner = payloads.executionId(ref).flatMap(store::execution);
        if (owner.isEmpty()) return event;
        try {
            authorize(tenant, owner.get().flowIdentity(), Action.PAYLOAD);
        } catch (SecurityException denied) {
            return event;
        }
        Map<String, Object> attributes = new LinkedHashMap<>(event.attributes());
        attributes.put("payload", payloads.find(ref).orElse(null));
        return new ExecutionEvent(
                event.eventId(),
                event.executionId(),
                event.eventType(),
                event.status(),
                event.nodeId(),
                event.nodeCode(),
                event.scopePath(),
                event.attempt(),
                event.sequence(),
                event.occurredAt(),
                attributes);
    }

    /** 转发不授予子流程权限；真实来源与每层转发路径均须单独 READ 授权。 */
    public boolean eventVisible(String tenant, ExecutionEvent event) {
        Set<String> owners = new HashSet<>();
        owners.add(event.executionId());
        for (String key : List.of("originExecutionId", "childExecutionId")) {
            if (event.attributes().get(key) instanceof String id) owners.add(id);
        }
        if (event.attributes().get("forwardingPath") instanceof List<?> path) {
            for (Object id : path) if (id instanceof String value) owners.add(value);
        }
        try {
            for (String id : owners) {
                var owner = store.execution(id);
                if (owner.isEmpty()) return false;
                authorize(tenant, owner.get().flowIdentity(), Action.READ);
            }
            return true;
        } catch (SecurityException denied) {
            return false;
        }
    }

    public Map<String, Object> cancel(HttpServletRequest http, String id) {
        execution(http, id, Action.CANCEL);
        var handle = handles.get(id);
        return Map.of("executionId", id, "cancelRequested", handle != null && handle.cancel());
    }

    public ExecutionRecordStore store() {
        return store;
    }

    /**
     * Adds safe timing and error context while leaving full values behind separately authorized
     * references.
     */
    public Map<String, Object> summary(ExecutionRecord record) {
        var summary = JSON.parseObject(JSON.toJSONString(record));
        if (record.finishedAt() != null)
            summary.put(
                    "durationMillis",
                    Duration.between(record.startedAt(), record.finishedAt()).toMillis());
        if (record.errorCode() != null) {
            var error = new LinkedHashMap<String, Object>();
            error.put("code", record.errorCode());
            error.put("errorId", record.errorId());
            error.put("category", errorCategory(record.errorCode()));
            error.put("message", "execution did not complete successfully");
            error.put("retryable", false);
            error.put("executionId", record.executionId());
            error.put("details", Map.of());
            store.nodes(record.executionId()).stream()
                    .filter(n -> Objects.equals(n.errorId(), record.errorId()))
                    .findFirst()
                    .ifPresent(
                            n -> {
                                error.put("nodeCode", n.nodeCode());
                                error.put("scopePath", n.scopePath());
                                error.put("attempt", n.attempt());
                            });
            error.putIfAbsent("nodeCode", null);
            error.putIfAbsent("scopePath", null);
            error.putIfAbsent("attempt", 0);
            summary.put("error", error);
        }
        return summary;
    }

    /**
     * Joins immutable node identity with its owning flow without duplicating that metadata on every
     * update.
     */
    public Map<String, Object> nodeSummary(NodeExecutionRecord node) {
        var summary = JSON.parseObject(JSON.toJSONString(node));
        store.execution(node.executionId())
                .ifPresent(
                        execution -> {
                            summary.put("flowIdentity", execution.flowIdentity());
                            summary.put("bundleHash", execution.bundleHash());
                            summary.put("rootExecutionId", execution.rootExecutionId());
                            summary.put("parentExecutionId", execution.parentExecutionId());
                        });
        if (node.startedAt() != null)
            summary.put(
                    "queueWaitMillis",
                    Duration.between(node.queuedAt(), node.startedAt()).toMillis());
        if (node.startedAt() != null && node.finishedAt() != null)
            summary.put(
                    "durationMillis",
                    Duration.between(node.startedAt(), node.finishedAt()).toMillis());
        return summary;
    }

    private static String errorCategory(String code) {
        if (code.startsWith("HTTP_")) return "EXTERNAL_CALL";
        if (code.startsWith("LOCK_")) return "LOCK";
        if (code.contains("DEADLINE") || code.contains("TIMEOUT")) return "TIMEOUT";
        if (code.contains("CANCEL")) return "CANCELLATION";
        if (code.contains("OUTPUT") || code.contains("PROTOCOL")) return "PROTOCOL";
        if (code.contains("SCRIPT")) return "SCRIPT";
        if (code.contains("INPUT")) return "INPUT";
        if (code.contains("CAPACITY") || code.contains("LIMIT")) return "RESOURCE_LIMIT";
        return "INTERNAL";
    }

    /**
     * WAIT returns only the same authorized observation summary used by queries; values require
     * payload permission.
     */
    public ExecutionRecord waitFor(Started started) throws Exception {
        var completed =
                started.handle()
                        .completion()
                        .toCompletableFuture()
                        .get(maxDuration.toMillis() + 5000, TimeUnit.MILLISECONDS);
        var record = store.execution(started.handle().executionId()).orElseThrow();
        if (record.status() != completed.status()
                || !Objects.equals(record.errorId(), completed.errorId()))
            throw new IllegalStateException("OBSERVATION_RESULT_UNAVAILABLE");
        return record;
    }

    public record Started(ExecutionHandle handle, String responseMode) {}
}
