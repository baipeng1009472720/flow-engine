package com.playrewards.flowengine.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.CapturedAuthorization;
import com.playrewards.flowengine.spi.FlowBundleLoader;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Profile-gated security acceptance across the real HTTP, compiler, executor and observation path.
 *
 * <p>Focused unit tests still own exhaustive sandbox and address-policy cases. This suite proves
 * those failures remain fail-closed after server error mapping and cannot be used to cross a tenant
 * or reveal submitted sensitive values through ordinary diagnostics.</p>
 */
@SpringBootTest(
        classes = EngineSecurityIT.Host.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.threads.virtual.enabled=true",
            "flow-engine.execution.max-duration=10s",
            "flow-engine.payload.max-bytes=1048576"
        })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EngineSecurityIT {
    private static final String TENANT_A = "security-a";
    private static final String TENANT_B = "security-b";
    private static final String CANARY = "security-canary-7f4e";

    @LocalServerPort int port;
    private HttpClient client;

    @BeforeAll
    void createClient() {
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    }

    @Test
    void sandbox_and_private_http_failures_remain_masked_through_public_observation_apis()
            throws Exception {
        JSONObject script = start("security:script", TENANT_A, Map.of());
        assertFailure(script, "SCRIPT_SECURITY_VIOLATION");
        assertDiagnosticsContainNoCanary(script.getString("executionId"));

        JSONObject http = start("security:http-private", TENANT_A, Map.of());
        assertFailure(http, "HTTP_TARGET_REJECTED");
        assertDiagnosticsContainNoCanary(http.getString("executionId"));
    }

    @Test
    void execution_node_log_event_and_payload_queries_are_tenant_isolated() throws Exception {
        JSONObject result =
                start("security:payload", TENANT_A, Map.of("startInputs", Map.of("secret", CANARY)));
        assertEquals("SUCCEEDED", result.getString("status"), result::toJSONString);
        String executionId = result.getString("executionId");
        String payloadRef = result.getString("resultPayloadRef");
        assertNotNull(payloadRef);

        JSONArray nodes = JSON.parseArray(get("/v1/executions/" + executionId + "/nodes", TENANT_A).body());
        String nodeExecutionId = nodes.getJSONObject(0).getString("nodeExecutionId");
        for (String path :
                List.of(
                        "/v1/executions/" + executionId,
                        "/v1/executions/" + executionId + "/nodes",
                        "/v1/executions/" + executionId + "/events",
                        "/v1/node-executions/" + nodeExecutionId + "/logs",
                        "/v1/payloads/" + payloadRef)) {
            assertEquals(403, get(path, TENANT_B).statusCode(), path);
        }

        String ordinaryDiagnostics =
                get("/v1/executions/" + executionId, TENANT_A).body()
                        + get("/v1/executions/" + executionId + "/nodes", TENANT_A).body()
                        + get("/v1/executions/" + executionId + "/events", TENANT_A).body()
                        + get("/v1/node-executions/" + nodeExecutionId + "/logs", TENANT_A).body();
        assertFalse(ordinaryDiagnostics.contains(CANARY), ordinaryDiagnostics);
        assertTrue(get("/v1/payloads/" + payloadRef, TENANT_A).body().contains(CANARY));
    }

    @Test
    void oversized_requests_forged_hashes_unknown_versions_and_body_tenants_fail_closed()
            throws Exception {
        HttpResponse<String> oversized =
                send(
                        "/v1/executions",
                        TENANT_A,
                        "x".repeat(262_145),
                        HttpRequest.BodyPublishers.ofString("x".repeat(262_145)));
        assertEquals(413, oversized.statusCode());

        assertEquals(400, post("security:forged-hash", TENANT_A, Map.of()).statusCode());
        assertEquals(400, post("security:unknown-version", TENANT_A, Map.of()).statusCode());
        assertEquals(
                403,
                post("security:payload", TENANT_A, Map.of("tenantId", TENANT_B)).statusCode());
    }

    private JSONObject start(String reference, String tenant, Map<String, Object> extra)
            throws Exception {
        HttpResponse<String> response = post(reference, tenant, extra);
        assertEquals(200, response.statusCode(), response::body);
        assertFalse(response.body().contains(CANARY), response::body);
        return JSON.parseObject(response.body());
    }

    private HttpResponse<String> post(
            String reference, String tenant, Map<String, Object> extra) throws Exception {
        JSONObject body = new JSONObject();
        body.put("flowReference", reference);
        body.put("mode", "EPHEMERAL");
        body.put("responseMode", "WAIT");
        body.put("idempotencyKey", UUID.randomUUID().toString());
        body.putAll(extra);
        String json = body.toJSONString();
        return send(
                "/v1/executions",
                tenant,
                json,
                HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
    }

    private HttpResponse<String> get(String path, String tenant) throws Exception {
        return client.send(
                request(path, tenant).GET().build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private HttpResponse<String> send(
            String path, String tenant, String ignoredBody, HttpRequest.BodyPublisher publisher)
            throws Exception {
        return client.send(
                request(path, tenant).POST(publisher).build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private HttpRequest.Builder request(String path, String tenant) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(12))
                .header("Content-Type", "application/json")
                .header("X-Security-Tenant", tenant);
    }

    private void assertDiagnosticsContainNoCanary(String executionId) throws Exception {
        String nodesText = get("/v1/executions/" + executionId + "/nodes", TENANT_A).body();
        JSONArray nodes = JSON.parseArray(nodesText);
        StringBuilder diagnostics =
                new StringBuilder(get("/v1/executions/" + executionId, TENANT_A).body())
                        .append(nodesText)
                        .append(get("/v1/executions/" + executionId + "/events", TENANT_A).body());
        for (Object value : nodes) {
            String nodeExecutionId = ((JSONObject) value).getString("nodeExecutionId");
            diagnostics.append(get("/v1/node-executions/" + nodeExecutionId + "/logs", TENANT_A).body());
        }
        assertFalse(diagnostics.toString().contains(CANARY), diagnostics::toString);
    }

    private static void assertFailure(JSONObject result, String code) {
        assertEquals("FAILED", result.getString("status"), result::toJSONString);
        assertEquals(code, result.getJSONObject("error").getString("code"), result::toJSONString);
    }

    /** Minimal trusted host adapter used only by the security gate. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class Host {
        @Bean
        FlowBundleLoader securityBundles() {
            return new FlowBundleLoader() {
                @Override
                public String scheme() {
                    return "security";
                }

                @Override
                public ExecutableFlowBundle load(String reference) {
                    return switch (reference) {
                        case "security:script" -> scriptBundle();
                        case "security:http-private" -> privateHttpBundle();
                        case "security:payload" -> payloadBundle();
                        case "security:forged-hash" -> forgedHashBundle();
                        case "security:unknown-version" ->
                                throw new IllegalArgumentException("unsupported bundle version");
                        default -> throw new IllegalArgumentException("unknown security flow");
                    };
                }
            };
        }

        @Bean
        ExecutionAccessPolicy securityAccessPolicy() {
            return new ExecutionAccessPolicy() {
                @Override
                public CapturedAuthorization capture(HttpServletRequest request) {
                    String tenant = request.getHeader("X-Security-Tenant");
                    if (tenant == null || tenant.isBlank()) throw new SecurityException();
                    return new SecurityAuthorization(tenant);
                }

                @Override
                public void authorize(
                        CapturedAuthorization caller, FlowIdentity flow, Action action) {
                    if (!(caller instanceof SecurityAuthorization authorization)
                            || !authorization.tenantId().equals(flow.tenantId())
                            || action == null) throw new SecurityException();
                }
            };
        }

        private record SecurityAuthorization(String tenantId) implements CapturedAuthorization {}

        private static ExecutableFlowBundle scriptBundle() {
            ProtocolSchema any = new ProtocolSchema("any", "1.0", Map.of());
            return bundle(
                    "script",
                    List.of(
                            node("start", "security.start", Map.of()),
                            node(
                                    "script",
                                    "security.script",
                                    Map.of("groovyScript", "return new File('" + CANARY + "')")),
                            node("end", "security.end", Map.of("resultStatus", "SUCCESS"))),
                    List.of(edge("start", "script"), edge("script", "end")),
                    Map.of(
                            "security.start", contract("security.start", ComponentType.START, Map.of(), ComponentEffect.PURE),
                            "security.script",
                                    contract(
                                            "security.script",
                                            ComponentType.SCRIPT,
                                            Map.of("result", new OutputSlotContract("result", any)),
                                            ComponentEffect.PURE),
                            "security.end", contract("security.end", ComponentType.END, Map.of(), ComponentEffect.PURE)),
                    Map.of("any", any));
        }

        private static ExecutableFlowBundle privateHttpBundle() {
            return bundle(
                    "http-private",
                    List.of(
                            node("start", "security.start", Map.of()),
                            node(
                                    "http",
                                    "security.http",
                                    Map.of(
                                            "method", "GET",
                                            "url", "http://127.0.0.1:1/" + CANARY,
                                            "requestParamsScript", "return [:]",
                                            "timeoutMillis", 1000,
                                            "retryCount", 0)),
                            node("end", "security.end", Map.of("resultStatus", "SUCCESS"))),
                    List.of(edge("start", "http"), edge("http", "end")),
                    Map.of(
                            "security.start", contract("security.start", ComponentType.START, Map.of(), ComponentEffect.PURE),
                            "security.http", contract("security.http", ComponentType.HTTP, Map.of(), ComponentEffect.PURE),
                            "security.end", contract("security.end", ComponentType.END, Map.of(), ComponentEffect.PURE)),
                    Map.of());
        }

        private static ExecutableFlowBundle payloadBundle() {
            ProtocolSchema text =
                    new ProtocolSchema("text", "1.0", Map.of("type", "string", "x-sensitive", true));
            return bundle(
                    "payload",
                    List.of(
                            node(
                                    "start",
                                    "security.start.payload",
                                    Map.of(
                                            "inputProtocols",
                                            List.of(
                                                    Map.of(
                                                            "key", "secret",
                                                            "name", "secret",
                                                            "protocolCode", "text",
                                                            "required", true,
                                                            "schema", text.schema())))),
                            node(
                                    "end",
                                    "security.end",
                                    Map.of(
                                            "resultStatus", "SUCCESS",
                                            "returnBinding",
                                            Map.of(
                                                    "sourceNodeId", "start",
                                                    "sourceOutputKey", "secret")))),
                    List.of(edge("start", "end")),
                    Map.of(
                            "security.start.payload",
                                    contract(
                                            "security.start.payload",
                                            ComponentType.START,
                                            Map.of("secret", new OutputSlotContract("secret", text)),
                                            ComponentEffect.PURE),
                            "security.end", contract("security.end", ComponentType.END, Map.of(), ComponentEffect.PURE)),
                    Map.of("text", text));
        }

        private static ExecutableFlowBundle forgedHashBundle() {
            ExecutableFlowBundle valid = payloadBundle();
            return new ExecutableFlowBundle(
                    valid.contractVersion(),
                    valid.flowIdentity(),
                    valid.flowDefinition(),
                    valid.componentContracts(),
                    valid.protocolSchemas(),
                    valid.externalScopeSchemas(),
                    "0".repeat(64),
                    valid.trust());
        }

        private static ExecutableFlowBundle bundle(
                String flowId,
                List<FlowNode> nodes,
                List<FlowEdge> edges,
                Map<String, CompiledComponentContract> contracts,
                Map<String, ProtocolSchema> protocols) {
            return new ExecutableFlowBundle(
                            "1.0",
                            new FlowIdentity(TENANT_A, flowId, "v1"),
                            new FlowDefinition("2.0", nodes, edges),
                            contracts,
                            protocols,
                            Map.of(),
                            null,
                            null)
                    .withCalculatedHash();
        }

        private static FlowNode node(String id, String componentCode, Map<String, Object> config) {
            return new FlowNode(id, componentCode, id, id, config);
        }

        private static FlowEdge edge(String source, String target) {
            return new FlowEdge(source + '-' + target, source, target, "NEXT", "IN");
        }

        private static CompiledComponentContract contract(
                String code,
                ComponentType type,
                Map<String, OutputSlotContract> outputs,
                ComponentEffect effect) {
            return new CompiledComponentContract(
                    code,
                    type,
                    null,
                    Map.of(),
                    Map.of(),
                    outputs,
                    new ComponentCapabilities(effect, true, false, false, 64));
        }
    }
}
