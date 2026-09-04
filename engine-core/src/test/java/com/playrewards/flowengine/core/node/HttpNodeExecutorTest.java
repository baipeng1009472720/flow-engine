package com.playrewards.flowengine.core.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.execution.ConcurrencyGovernor;
import com.playrewards.flowengine.core.execution.ExecutionHandle;
import com.playrewards.flowengine.core.execution.FlowEngine;
import com.playrewards.flowengine.core.execution.ScopePath;
import com.playrewards.flowengine.core.idempotency.IdempotencyCoordinator;
import com.playrewards.flowengine.core.node.builtin.HttpNodeExecutor;
import com.playrewards.flowengine.core.retry.RetryPolicy;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Verifies the HTTP node contract through the real FlowEngine and DagScheduler path. */
class HttpNodeExecutorTest {

    private static final ProtocolSchema OBJECT = protocol("response", Map.of("type", "object"));

    @Test
    void executes_all_methods_with_one_request_parameter_script_contract() throws Exception {
        for (String method : List.of("GET", "DELETE", "POST", "PUT", "PATCH")) {
            RecordingClient client = new RecordingClient();
            client.responses.add(response(200, "{\"accepted\":true}"));
            ScriptRuntime scripts = invocation -> CompletableFuture.completedFuture(new LinkedHashMap<>(Map.of(
                    "z", List.of(2, 1),
                    "a", Map.of("space", "a b"))));

            ExecutionResult result = execute(method, scripts, client, ComponentEffect.SIDE_EFFECT,
                    true, "https://public.example/path?existing=1", Map.of(), 0, 1_000, 1_024);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status(), method);
            HttpNodeClient.HttpRequest request = client.requests.getFirst();
            assertEquals(method, request.method());
            if (method.equals("GET") || method.equals("DELETE")) {
                assertEquals("existing=1&a=%7B%22space%22%3A%22a%20b%22%7D&z=%5B2%2C1%5D",
                        request.uri().getRawQuery());
                assertEquals(0, request.body().length);
            } else {
                assertEquals("existing=1", request.uri().getRawQuery());
                assertEquals("{\"a\":{\"space\":\"a b\"},\"z\":[2,1]}",
                        new String(request.body(), StandardCharsets.UTF_8));
                assertEquals("application/json; charset=utf-8", request.headers().get("Content-Type"));
            }
        }
    }

    @Test
    void maps_script_failures_and_rejects_non_object_results_without_sending() throws Exception {
        List<ScriptRuntime> failures = List.of(
                invocation -> {
                    throw new ScriptRuntime.ScriptExecutionException("SCRIPT_LIMIT_EXCEEDED", "script limit exceeded");
                },
                invocation -> CompletableFuture.failedFuture(
                        new ScriptRuntime.ScriptExecutionException("SCRIPT_TIMEOUT", "script execution timed out")),
                invocation -> CompletableFuture.completedFuture(List.of("not", "object")));
        List<String> expected = List.of("SCRIPT_LIMIT_EXCEEDED", "SCRIPT_TIMEOUT", "HTTP_PARAMS_INVALID");

        for (int index = 0; index < failures.size(); index++) {
            RecordingClient client = new RecordingClient();
            ExecutionResult result = execute("GET", failures.get(index), client, ComponentEffect.PURE,
                    false, "https://public.example/path", Map.of(), 0, 1_000, 1_024);

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals(expected.get(index), result.errorCode());
            assertTrue(client.requests.isEmpty());
        }
    }

    @Test
    void rejects_deprecated_fields_and_engine_controlled_or_duplicate_headers() throws Exception {
        List<Map<String, Object>> forbiddenConfigs = new ArrayList<>();
        forbiddenConfigs.add(Map.of("query", Map.of("secret", "leak")));
        forbiddenConfigs.add(Map.of("bodyBinding", Map.of("source", "start")));
        forbiddenConfigs.add(Map.of("inputBindings", List.of()));
        forbiddenConfigs.add(Map.of("headers", Map.of("Host", "attacker.example")));
        forbiddenConfigs.add(Map.of("headers", Map.of("Content-Length", "999")));
        forbiddenConfigs.add(Map.of("headers", Map.of("Idempotency-Key", "chosen-by-user")));
        forbiddenConfigs.add(Map.of("headers", Map.of("X-Test", "one", "x-test", "two")));
        for (Map<String, Object> forbidden : forbiddenConfigs) {
            RecordingClient client = new RecordingClient();
            ExecutionResult result = execute("POST", objectScript(Map.of()), client,
                    ComponentEffect.SIDE_EFFECT, false, "https://public.example/path",
                    forbidden, 0, 1_000, 1_024);

            assertEquals(ExecutionStatus.FAILED, result.status(), forbidden.toString());
            assertTrue(result.errorCode().startsWith("HTTP_CONFIG_"), forbidden.toString());
            assertTrue(client.requests.isEmpty());
        }
    }

    @Test
    void requires_exact_method_names_and_allows_legal_empty_header_values() throws Exception {
        RecordingClient lowercase = new RecordingClient();
        ExecutionResult invalidMethod = execute("get", objectScript(Map.of()), lowercase,
                ComponentEffect.PURE, false, "https://public.example/path", Map.of(), 0, 1_000, 1_024);
        assertEquals("HTTP_CONFIG_METHOD_INVALID", invalidMethod.errorCode());
        assertTrue(lowercase.requests.isEmpty());

        RecordingClient emptyHeader = new RecordingClient();
        ExecutionResult success = execute("GET", objectScript(Map.of()), emptyHeader,
                ComponentEffect.PURE, false, "https://public.example/path",
                Map.of("headers", Map.of("X-Empty", "")), 0, 1_000, 1_024);
        assertEquals(ExecutionStatus.SUCCEEDED, success.status());
        assertEquals("", emptyHeader.requests.getFirst().headers().get("X-Empty"));
    }

    @Test
    void validates_declared_result_and_does_not_publish_when_output_is_undeclared() throws Exception {
        RecordingClient valid = new RecordingClient();
        valid.responses.add(response(200, "{\"accepted\":true}"));
        ExecutionResult success = execute("GET", objectScript(Map.of()), valid, ComponentEffect.PURE,
                true, "https://public.example/path", Map.of(), 0, 1_000, 1_024);
        assertEquals(Map.of("accepted", true), success.result());

        RecordingClient invalid = new RecordingClient();
        invalid.responses.add(response(200, "[]"));
        ExecutionResult protocolFailure = execute("GET", objectScript(Map.of()), invalid,
                ComponentEffect.PURE, true, "https://public.example/path", Map.of(), 0, 1_000, 1_024);
        assertEquals("NODE_OUTPUT_INVALID", protocolFailure.errorCode());

        RecordingClient mismatched = new RecordingClient();
        mismatched.responses.add(response(200, "{\"accepted\":true}"));
        ExecutionResult mismatchedSnapshot = execute("GET", objectScript(Map.of()), mismatched,
                ComponentEffect.PURE, true, "https://public.example/path",
                Map.of("outputProtocol", Map.of(
                        "code", "different", "schema", Map.of("type", "string"))),
                0, 1_000, 1_024);
        assertEquals("HTTP_CONFIG_OUTPUT_INVALID", mismatchedSnapshot.errorCode());
        assertTrue(mismatched.requests.isEmpty());

        RecordingClient ignored = new RecordingClient();
        ignored.responses.add(response(200, "not-json"));
        ExecutionResult noOutput = execute("GET", objectScript(Map.of()), ignored, ComponentEffect.PURE,
                false, "https://public.example/path", Map.of(), 0, 1_000, 1_024);
        assertEquals(ExecutionStatus.SUCCEEDED, noOutput.status());
        assertNull(noOutput.result());
    }

    @Test
    void invalid_or_empty_response_is_deterministic_and_never_retried() throws Exception {
        RecordingClient invalid = new RecordingClient();
        invalid.responses.add(response(200, "not-json"));

        ExecutionResult invalidResult = execute("GET", objectScript(Map.of()), invalid,
                ComponentEffect.PURE, OBJECT, "https://public.example/path", Map.of(), 3,
                1_000, 1_024, Duration.ofSeconds(3));

        assertEquals(ExecutionStatus.FAILED, invalidResult.status());
        assertEquals("HTTP_RESPONSE_JSON_INVALID", invalidResult.errorCode());
        assertEquals(1, invalid.requests.size());

        RecordingClient required = new RecordingClient();
        ExecutionResult requiredResult = execute("GET", objectScript(Map.of()), required,
                ComponentEffect.PURE, OBJECT, "https://public.example/path", Map.of(), 3,
                1_000, 1_024, Duration.ofSeconds(3));
        assertEquals("NODE_OUTPUT_INVALID", requiredResult.errorCode());
        assertEquals(1, required.requests.size());

        RecordingClient nullable = new RecordingClient();
        ProtocolSchema nullableProtocol = protocol("nullable-response", Map.of());
        ExecutionResult nullableResult = execute("GET", objectScript(Map.of()), nullable,
                ComponentEffect.PURE, nullableProtocol, "https://public.example/path", Map.of(), 3,
                1_000, 1_024, Duration.ofSeconds(3));
        assertEquals(ExecutionStatus.SUCCEEDED, nullableResult.status());
        assertNull(nullableResult.result());
        assertEquals(1, nullable.requests.size());
    }

    @Test
    void retries_only_transient_safe_requests_and_keeps_attempt_count_bounded() throws Exception {
        RecordingClient retrying = new RecordingClient();
        retrying.responses.add(response(503, "temporary"));
        retrying.responses.add(response(429, "busy"));
        retrying.responses.add(response(200, "{\"accepted\":true}"));

        ExecutionResult success = execute("GET", objectScript(Map.of()), retrying, ComponentEffect.PURE,
                true, "https://public.example/path", Map.of(), 7, 1_000, 1_024);
        assertEquals(ExecutionStatus.SUCCEEDED, success.status());
        assertEquals(3, retrying.requests.size());

        RecordingClient deterministic = new RecordingClient();
        deterministic.responses.add(response(400, "bad"));
        ExecutionResult failure = execute("GET", objectScript(Map.of()), deterministic, ComponentEffect.PURE,
                false, "https://public.example/path", Map.of(), 3, 1_000, 1_024);
        assertEquals("HTTP_STATUS_400", failure.errorCode());
        assertEquals(1, deterministic.requests.size());
    }

    @Test
    void protected_side_effect_reuses_stable_idempotency_header_but_unprotected_never_retries()
            throws Exception {
        RecordingClient protectedClient = new RecordingClient();
        protectedClient.failures.add(new HttpNodeClient.HttpClientException(
                HttpNodeClient.FailureKind.CONNECTION_LOST_AFTER_SEND, "HTTP transport interrupted"));
        protectedClient.responses.add(response(200, "{}"));
        ExecutionResult protectedResult = execute("POST", objectScript(Map.of("value", 1)),
                protectedClient, ComponentEffect.IDEMPOTENT, false,
                "https://public.example/path", Map.of(), 2, 1_000, 1_024);
        assertEquals(ExecutionStatus.SUCCEEDED, protectedResult.status());
        assertEquals(2, protectedClient.requests.size());
        assertEquals(protectedClient.requests.get(0).headers().get("Idempotency-Key"),
                protectedClient.requests.get(1).headers().get("Idempotency-Key"));

        RecordingClient unprotectedClient = new RecordingClient();
        unprotectedClient.failures.add(new HttpNodeClient.HttpClientException(
                HttpNodeClient.FailureKind.TIMEOUT_AFTER_SEND, "HTTP request timed out"));
        ExecutionResult uncertain = execute("PATCH", objectScript(Map.of()), unprotectedClient,
                ComponentEffect.SIDE_EFFECT, false, "https://public.example/path",
                Map.of(), 3, 1_000, 1_024);
        assertEquals(ExecutionStatus.WAITING_MANUAL, uncertain.status());
        assertEquals("HTTP_SIDE_EFFECT_UNCERTAIN", uncertain.errorCode());
        assertEquals(1, unprotectedClient.requests.size());
    }

    @Test
    void pre_send_rejection_is_failed_not_manual_and_secrets_never_reach_public_errors() throws Exception {
        RecordingClient client = new RecordingClient();
        client.failures.add(new HttpNodeClient.HttpClientException(
                HttpNodeClient.FailureKind.TARGET_REJECTED, "HTTP target rejected"));
        String secret = "bearer-super-secret";
        ExecutionResult result = execute("POST", objectScript(Map.of("password", secret)), client,
                ComponentEffect.SIDE_EFFECT, false,
                "https://public.example/path?token=" + secret,
                Map.of("headers", Map.of("Authorization", secret)), 3, 1_000, 1_024);

        assertEquals(ExecutionStatus.FAILED, result.status());
        assertEquals("HTTP_TARGET_REJECTED", result.errorCode());
        assertFalse(result.toString().contains(secret));
        assertFalse(result.errorMessage().contains(secret));
        assertFalse(client.requests.getFirst().toString().contains(secret));
    }

    @Test
    void stable_node_key_isolated_by_tenant_request_flow_node_and_scope() {
        IdempotencyCoordinator coordinator = new IdempotencyCoordinator();
        String base = coordinator.nodeKey("tenant-a", "request-a", "v1", "node-a", ScopePath.root());

        assertEquals(base, coordinator.nodeKey("tenant-a", "request-a", "v1", "node-a", ScopePath.root()));
        assertNotEquals(base, coordinator.nodeKey("tenant-b", "request-a", "v1", "node-a", ScopePath.root()));
        assertNotEquals(base, coordinator.nodeKey("tenant-a", "request-b", "v1", "node-a", ScopePath.root()));
        assertNotEquals(base, coordinator.nodeKey("tenant-a", "request-a", "v2", "node-a", ScopePath.root()));
        assertNotEquals(base, coordinator.nodeKey("tenant-a", "request-a", "v1", "node-b", ScopePath.root()));
        assertNotEquals(base, coordinator.nodeKey("tenant-a", "request-a", "v1", "node-a",
                ScopePath.root().child("loop:index:1")));
    }

    @Test
    void execution_deadline_after_unprotected_send_enters_manual_state() throws Exception {
        RecordingClient client = new RecordingClient() {
            @Override
            public CompletionStage<HttpResponse> execute(HttpRequest request) {
                requests.add(request);
                request.sendObserver().sent();
                return new CompletableFuture<>();
            }
        };

        ExecutionResult result = execute("POST", objectScript(Map.of("operation", "charge")), client,
                ComponentEffect.SIDE_EFFECT, false, "https://public.example/path",
                Map.of(), 2, 1_000, 1_024, Duration.ofMillis(300));

        assertEquals(ExecutionStatus.WAITING_MANUAL, result.status(), result.errorCode());
        assertEquals("HTTP_SIDE_EFFECT_UNCERTAIN", result.errorCode());
        assertEquals(1, client.requests.size());
    }

    @Test
    void execution_deadline_before_send_remains_a_deterministic_failure() throws Exception {
        RecordingClient client = new RecordingClient() {
            @Override
            public CompletionStage<HttpResponse> execute(HttpRequest request) {
                requests.add(request);
                return new CompletableFuture<>();
            }
        };

        ExecutionResult result = execute("POST", objectScript(Map.of("operation", "charge")), client,
                ComponentEffect.SIDE_EFFECT, false, "https://public.example/path",
                Map.of(), 2, 1_000, 1_024, Duration.ofMillis(300));

        assertEquals(ExecutionStatus.FAILED, result.status(), result.errorCode());
        assertEquals("EXECUTION_DEADLINE_EXCEEDED", result.errorCode());
        assertEquals(1, client.requests.size());
    }

    @Test
    void already_expired_and_clock_jump_deadlines_use_the_explicit_send_state() throws Exception {
        Instant initial = Instant.parse("2026-09-05T00:00:00Z");
        Instant deadline = initial.plusSeconds(1);

        MutableClock expiredClock = new MutableClock(deadline);
        RecordingClient neverStarted = new RecordingClient();
        ExecutionResult expired = execute("POST", objectScript(Map.of()), neverStarted,
                ComponentEffect.SIDE_EFFECT, null, "https://public.example/path", Map.of(), 0,
                1_000, 1_024, expiredClock, deadline);
        assertEquals("EXECUTION_DEADLINE_EXCEEDED", expired.errorCode());
        assertTrue(neverStarted.requests.isEmpty());

        MutableClock beforeSendClock = new MutableClock(initial);
        RecordingClient beforeSend = new RecordingClient() {
            @Override
            public CompletionStage<HttpResponse> execute(HttpRequest request) {
                requests.add(request);
                beforeSendClock.set(deadline.plusMillis(1));
                return new CompletableFuture<>();
            }
        };
        ExecutionResult deterministic = execute("POST", objectScript(Map.of()), beforeSend,
                ComponentEffect.SIDE_EFFECT, null, "https://public.example/path", Map.of(), 0,
                1_000, 1_024, beforeSendClock, deadline);
        assertEquals(ExecutionStatus.FAILED, deterministic.status());
        assertEquals("EXECUTION_DEADLINE_EXCEEDED", deterministic.errorCode());

        MutableClock afterSendClock = new MutableClock(initial);
        RecordingClient afterSend = new RecordingClient() {
            @Override
            public CompletionStage<HttpResponse> execute(HttpRequest request) {
                requests.add(request);
                request.sendObserver().sent();
                afterSendClock.set(deadline.plusMillis(1));
                return new CompletableFuture<>();
            }
        };
        ExecutionResult uncertain = execute("POST", objectScript(Map.of()), afterSend,
                ComponentEffect.SIDE_EFFECT, null, "https://public.example/path", Map.of(), 0,
                1_000, 1_024, afterSendClock, deadline);
        assertEquals(ExecutionStatus.WAITING_MANUAL, uncertain.status());
        assertEquals("HTTP_SIDE_EFFECT_UNCERTAIN", uncertain.errorCode());
    }

    private static ExecutionResult execute(String method, ScriptRuntime scripts, RecordingClient client,
            ComponentEffect effect, boolean output, String url, Map<String, Object> extraConfig,
            int retryCount, int timeoutMillis, int maxResponseBytes) throws Exception {
        return execute(method, scripts, client, effect, output, url, extraConfig,
                retryCount, timeoutMillis, maxResponseBytes, Duration.ofSeconds(3));
    }

    private static ExecutionResult execute(String method, ScriptRuntime scripts, RecordingClient client,
            ComponentEffect effect, boolean output, String url, Map<String, Object> extraConfig,
            int retryCount, int timeoutMillis, int maxResponseBytes, Duration executionDuration)
            throws Exception {
        return execute(method, scripts, client, effect, output ? OBJECT : null, url, extraConfig,
                retryCount, timeoutMillis, maxResponseBytes, executionDuration);
    }

    private static ExecutionResult execute(String method, ScriptRuntime scripts, RecordingClient client,
            ComponentEffect effect, ProtocolSchema outputProtocol, String url, Map<String, Object> extraConfig,
            int retryCount, int timeoutMillis, int maxResponseBytes, Duration executionDuration)
            throws Exception {
        Clock clock = Clock.systemUTC();
        return execute(method, scripts, client, effect, outputProtocol, url, extraConfig, retryCount,
                timeoutMillis, maxResponseBytes, clock, clock.instant().plus(executionDuration));
    }

    private static ExecutionResult execute(String method, ScriptRuntime scripts, RecordingClient client,
            ComponentEffect effect, ProtocolSchema outputProtocol, String url, Map<String, Object> extraConfig,
            int retryCount, int timeoutMillis, int maxResponseBytes, Clock clock, Instant deadline)
            throws Exception {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("method", method);
        config.put("url", url);
        config.put("requestParamsScript", "return [:]");
        config.put("timeoutMillis", timeoutMillis);
        config.put("retryCount", retryCount);
        if (outputProtocol != null) {
            config.put("outputProtocol", Map.of(
                    "code", outputProtocol.code(), "schema", outputProtocol.schema()));
        }
        config.putAll(extraConfig);
        CompiledNode start = node("start", ComponentType.START, Map.of(), ComponentEffect.PURE, Map.of());
        CompiledNode http = node("http", ComponentType.HTTP, config, effect,
                outputProtocol != null
                        ? Map.of("result", new OutputSlotContract("result", outputProtocol)) : Map.of());
        CompiledNode end = node("end", ComponentType.END,
                outputProtocol != null ? Map.of("resultStatus", "SUCCESS", "returnBinding",
                        Map.of("sourceNodeId", "http", "sourceOutputKey", "result"))
                        : Map.of("resultStatus", "SUCCESS"),
                ComponentEffect.PURE, Map.of());
        ExecutionPlan plan = new ExecutionPlan(new FlowIdentity("tenant-a", "flow-a", "v1"), "hash",
                Map.of("start", start, "http", http, "end", end),
                Map.of("start", List.of("http"), "http", List.of("end")),
                Map.of("start", List.of(), "http", List.of("start"), "end", List.of("http")),
                Map.of("start", Map.of("NEXT", List.of("http")), "http", Map.of("NEXT", List.of("end"))),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
        HttpNodeExecutor executor = new HttpNodeExecutor(scripts, client,
                new RetryPolicy(2, Duration.ofMillis(1), Duration.ofMillis(2)),
                new IdempotencyCoordinator(), clock, Duration.ofSeconds(2), 1_024);
        NodeExecutorRegistry registry = NodeExecutorRegistry.withBuiltIns(List.of(executor));
        ExecutionRequest request = new ExecutionRequest("inline:flow", ExecutionMode.EPHEMERAL,
                ExecutionPurpose.TRIAL, Map.of(), Map.of(), null, "request-a", "tenant-a",
                deadline, Map.of());
        try (FlowEngine engine = new FlowEngine(registry,
                new ConcurrencyGovernor(32, 32, 32, 32, 32, clock), clock)) {
            ExecutionHandle handle = engine.execute(plan, request);
            return handle.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static CompiledNode node(String id, ComponentType type, Map<String, Object> config,
            ComponentEffect effect, Map<String, OutputSlotContract> outputs) {
        ComponentCapabilities capabilities = new ComponentCapabilities(effect, true, false, true, 8);
        return new CompiledNode(new FlowNode(id, "builtin." + type.name().toLowerCase(), id, id, config),
                new CompiledComponentContract("builtin." + type.name().toLowerCase(), type, null,
                        Map.of(), Map.of(), outputs, capabilities), Map.of());
    }

    private static ScriptRuntime objectScript(Map<String, Object> value) {
        return invocation -> CompletableFuture.completedFuture(value);
    }

    private static HttpNodeClient.HttpResponse response(int status, String body) {
        return new HttpNodeClient.HttpResponse(status,
                Map.of("Content-Type", List.of("application/json; charset=utf-8")),
                body.getBytes(StandardCharsets.UTF_8));
    }

    private static ProtocolSchema protocol(String code, Map<String, Object> schema) {
        return new ProtocolSchema(code, "1.0", schema);
    }

    private static class RecordingClient implements HttpNodeClient {

        protected final List<HttpRequest> requests = new ArrayList<>();
        private final List<HttpResponse> responses = new ArrayList<>();
        private final List<HttpClientException> failures = new ArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();

        @Override
        public CompletionStage<HttpResponse> execute(HttpRequest request) {
            requests.add(request);
            int attempt = attempts.getAndIncrement();
            if (attempt < failures.size()) {
                FailureKind kind = failures.get(attempt).kind();
                if (kind == FailureKind.TIMEOUT_AFTER_SEND
                        || kind == FailureKind.CONNECTION_LOST_AFTER_SEND) {
                    request.sendObserver().sent();
                } else {
                    request.sendObserver().resolved();
                }
                return CompletableFuture.failedFuture(failures.get(attempt));
            }
            int responseIndex = attempt - failures.size();
            request.sendObserver().sent();
            request.sendObserver().resolved();
            if (responseIndex >= responses.size()) {
                return CompletableFuture.completedFuture(response(204, ""));
            }
            return CompletableFuture.completedFuture(responses.get(responseIndex));
        }
    }

    private static final class MutableClock extends Clock {

        private final AtomicReference<Instant> now;

        private MutableClock(Instant initial) {
            this.now = new AtomicReference<>(initial);
        }

        private void set(Instant value) {
            now.set(value);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
