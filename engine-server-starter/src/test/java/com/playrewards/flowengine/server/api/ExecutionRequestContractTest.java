package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;
import com.playrewards.flowengine.spi.FlowBundleLoader;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.test.web.servlet.MockMvc;

import java.util.*;

/** Authenticated fixture host proves body identity cannot override the trusted principal. */
@SpringBootTest(classes = ExecutionRequestContractTest.Host.class)
@AutoConfigureMockMvc
class ExecutionRequestContractTest {
    @Autowired MockMvc mvc;
    @Autowired ExecutionService service;
    @Autowired com.playrewards.flowengine.spi.PayloadStore payloads;

    @Test
    void read_permission_does_not_rehydrate_raw_sse_content_or_grant_payload_access()
            throws Exception {
        String body =
                mvc.perform(
                                post("/v1/executions")
                                        .principal(() -> "tenant")
                                        .contentType("application/json")
                                        .content("{\"flowReference\":\"test:flow\"}"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse()
                        .getContentAsString();
        String id = JSON.parseObject(body).getString("executionId");
        String reference = payloads.store(id, "private-stream-value").value();
        var event =
                new com.playrewards.flowengine.contract.execution.ExecutionEvent(
                        id + ":999",
                        id,
                        "CONTENT",
                        com.playrewards.flowengine.contract.execution.ExecutionStatus.RUNNING,
                        "start",
                        "start",
                        "root",
                        1,
                        999,
                        java.time.Instant.now(),
                        Map.of("contentPayloadRef", reference));
        assertFalse(
                JSON.toJSONString(service.streamEvent("tenant", event))
                        .contains("private-stream-value"));
        mvc.perform(get("/v1/payloads/" + reference).principal(() -> "tenant"))
                .andExpect(status().isForbidden());
    }

    @Configuration
    @EnableAutoConfiguration
    static class Host {
        @Bean
        ExecutionAccessPolicy access() {
            return new ExecutionAccessPolicy() {
                public String tenantId(HttpServletRequest request) {
                    if (request.getUserPrincipal() == null) throw new SecurityException();
                    return request.getUserPrincipal().getName();
                }

                public void authorize(String tenant, FlowIdentity flow, Action action) {
                    if (!tenant.equals(flow.tenantId()) || action == Action.PAYLOAD)
                        throw new SecurityException();
                }
            };
        }

        @Bean
        FlowBundleLoader loader() {
            return new FlowBundleLoader() {
                public String scheme() {
                    return "test";
                }

                public ExecutableFlowBundle load(String reference) {
                    return bundle();
                }
            };
        }
    }

    @Test
    void wait_async_tenant_isolation_pagination_and_resume_are_explicit() throws Exception {
        var response =
                mvc.perform(
                                post("/v1/executions")
                                        .principal(() -> "tenant")
                                        .contentType("application/json")
                                        .content("{\"flowReference\":\"test:flow\"}"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse();
        var result = JSON.parseObject(response.getContentAsString());
        assertEquals("SUCCEEDED", result.getString("status"));
        String id = result.getString("executionId");
        assertNotNull(id);
        mvc.perform(get("/v1/executions/" + id).principal(() -> "other"))
                .andExpect(status().isForbidden());
        mvc.perform(get("/v1/executions/" + id + "/nodes?limit=501").principal(() -> "tenant"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/v1/executions/" + id + "/resume").principal(() -> "tenant"))
                .andExpect(status().isConflict());
        mvc.perform(
                        post("/v1/executions")
                                .principal(() -> "tenant")
                                .contentType("application/json")
                                .content(
                                        "{\"flowReference\":\"test:flow\",\"responseMode\":\"ASYNC\"}"))
                .andExpect(status().isAccepted());
    }

    @Test
    void rejects_body_tenant_durable_untrusted_bundle_and_large_body() throws Exception {
        for (String body : List.of("{\"flowReference\":\"test:flow\",\"tenantId\":\"other\"}"))
            mvc.perform(
                            post("/v1/executions")
                                    .principal(() -> "tenant")
                                    .contentType("application/json")
                                    .content(body))
                    .andExpect(status().isForbidden());
        mvc.perform(
                        post("/v1/executions")
                                .principal(() -> "tenant")
                                .contentType("application/json")
                                .content("{\"flowReference\":\"test:flow\",\"mode\":\"DURABLE\"}"))
                .andExpect(status().isConflict());
        mvc.perform(
                        post("/v1/executions")
                                .principal(() -> "tenant")
                                .contentType("application/json")
                                .content("{\"bundle\":{}}"))
                .andExpect(status().isBadRequest());
        mvc.perform(
                        post("/v1/executions")
                                .principal(() -> "tenant")
                                .contentType("application/json")
                                .content("x".repeat(262145)))
                .andExpect(status().isPayloadTooLarge());
    }

    @Test
    void malformed_json_and_missing_required_reference_are_safe_client_errors() throws Exception {
        for (String body :
                List.of("{", "{}", "{\"flowReference\":\"test:flow\",\"mode\":\"UNKNOWN\"}"))
            mvc.perform(
                            post("/v1/executions")
                                    .principal(() -> "tenant")
                                    .contentType("application/json")
                                    .content(body))
                    .andExpect(status().isBadRequest());
    }

    static ExecutableFlowBundle bundle() {
        var caps = new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 4);
        return new ExecutableFlowBundle(
                        "1.0",
                        new FlowIdentity("tenant", "flow", "v1"),
                        new FlowDefinition(
                                "2.0",
                                List.of(
                                        new FlowNode("start", "start", "start", "start", Map.of()),
                                        new FlowNode(
                                                "end",
                                                "end",
                                                "end",
                                                "end",
                                                Map.of("resultStatus", "SUCCESS"))),
                                List.of(new FlowEdge("a", "start", "end", "NEXT", "IN"))),
                        Map.of(
                                "start",
                                new CompiledComponentContract(
                                        "start",
                                        ComponentType.START,
                                        null,
                                        Map.of(),
                                        Map.of(),
                                        Map.of(),
                                        caps),
                                "end",
                                new CompiledComponentContract(
                                        "end",
                                        ComponentType.END,
                                        null,
                                        Map.of(),
                                        Map.of(),
                                        Map.of(),
                                        caps)),
                        Map.of(),
                        Map.of(),
                        null,
                        null)
                .withCalculatedHash();
    }
}
