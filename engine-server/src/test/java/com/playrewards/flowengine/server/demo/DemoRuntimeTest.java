package com.playrewards.flowengine.server.demo;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.playrewards.flowengine.server.FlowEngineApplication;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.util.List;
import java.util.Map;

/**
 * Exercises real runtime assembly and immutable demo bundles through the public servlet contract.
 */
@SpringBootTest(
        classes = FlowEngineApplication.class,
        properties = {"flow-engine.demo.enabled=true", "server.address=127.0.0.1"})
@AutoConfigureMockMvc
class DemoRuntimeTest {
    @Autowired MockMvc mvc;

    @Test
    void all_supported_nodes_return_selected_value_and_real_http_response() throws Exception {
        var execution = start("all", "WAIT");
        var nodes =
                JSON.parseArray(
                        body(
                                get(
                                        "/v1/executions/"
                                                + execution.getString("executionId")
                                                + "/nodes")));
        assertEquals(
                "SUCCEEDED",
                execution.getString("status"),
                execution.toJSONString() + nodes.toJSONString());
        var result = object(get("/v1/payloads/" + execution.getString("resultPayloadRef")));
        assertEquals("stream-complete", result.getString("selected"));
        assertEquals("demo-http", result.getJSONObject("http").getString("message"));
        assertEquals(List.of(10, 20), result.getJSONArray("results").toJavaList(Integer.class));
        var types =
                nodes.toList(JSONObject.class).stream()
                        .map(n -> n.getString("componentType"))
                        .collect(java.util.stream.Collectors.toSet());
        assertEquals(
                java.util.Set.of(
                        "START",
                        "END",
                        "SCRIPT",
                        "BUSINESS",
                        "IF",
                        "VARIABLE",
                        "PARALLEL",
                        "LOOP",
                        "LOCK",
                        "SUB_FLOW",
                        "HTTP"),
                types);
    }

    @Test
    void main_returns_parallel_loop_and_child_values_with_real_node_logs() throws Exception {
        var execution = start("main", "WAIT");
        var nodes =
                JSON.parseArray(
                        body(
                                get(
                                        "/v1/executions/"
                                                + execution.getString("executionId")
                                                + "/nodes")));
        assertEquals(
                "SUCCEEDED",
                execution.getString("status"),
                execution.toJSONString() + nodes.toJSONString());
        var result = object(get("/v1/payloads/" + execution.getString("resultPayloadRef")));
        assertEquals(List.of(10, 20), result.getJSONArray("results").toJavaList(Integer.class));
        assertEquals("child-complete", result.getJSONObject("child").getString("message"));

        var bodies =
                nodes.toList(JSONObject.class).stream()
                        .filter(n -> "body".equals(n.getString("nodeCode")))
                        .toList();
        assertEquals(2, bodies.size());
        assertNotEquals(bodies.get(0).getString("scopePath"), bodies.get(1).getString("scopePath"));
        assertNotEquals(
                bodies.get(0).getString("nodeExecutionId"),
                bodies.get(1).getString("nodeExecutionId"));
        for (var node : bodies) {
            assertEquals("SUCCEEDED", node.getString("status"));
            var logs =
                    JSON.parseArray(
                            body(
                                    get(
                                            "/v1/node-executions/"
                                                    + node.getString("nodeExecutionId")
                                                    + "/logs")));
            assertTrue(
                    logs.toList(JSONObject.class).stream()
                            .anyMatch(
                                    log ->
                                            log.getJSONObject("maskedAttributes")
                                                    .containsKey("diagnosticPayloadRef")),
                    logs.toJSONString());
        }
        var events =
                JSON.parseArray(
                        body(
                                get("/v1/executions/"
                                                + execution.getString("executionId")
                                                + "/events")
                                        .param("limit", "500")));
        assertEquals(
                2,
                events.toList(JSONObject.class).stream()
                        .filter(e -> "CONTENT".equals(e.getString("eventType")))
                        .count());
    }

    @Test
    void protocol_failure_keeps_the_same_error_id_in_result_node_event_and_log() throws Exception {
        var execution = start("failure", "WAIT");
        assertEquals("FAILED", execution.getString("status"));
        String errorId = execution.getString("errorId");
        assertNotNull(errorId);
        var nodes =
                JSON.parseArray(
                        body(
                                get(
                                        "/v1/executions/"
                                                + execution.getString("executionId")
                                                + "/nodes")));
        var failed =
                nodes.toList(JSONObject.class).stream()
                        .filter(n -> "invalid".equals(n.getString("nodeCode")))
                        .findFirst()
                        .orElseThrow();
        assertEquals(errorId, failed.getString("errorId"));
        var logs =
                JSON.parseArray(
                        body(
                                get(
                                        "/v1/node-executions/"
                                                + failed.getString("nodeExecutionId")
                                                + "/logs")));
        assertTrue(
                logs.toList(JSONObject.class).stream()
                        .anyMatch(
                                log ->
                                        "ERROR".equals(log.getString("level"))
                                                && errorId.equals(log.getString("errorId"))));
        var events =
                JSON.parseArray(
                        body(
                                get(
                                        "/v1/executions/"
                                                + execution.getString("executionId")
                                                + "/events")));
        assertTrue(
                events.toList(JSONObject.class).stream()
                        .anyMatch(
                                event ->
                                        "FAILED".equals(event.getString("eventType"))
                                                && errorId.equals(
                                                        event.getJSONObject("attributes")
                                                                .getString("errorId"))));
    }

    @Test
    void async_run_can_be_cancelled_and_cannot_be_resumed_as_a_new_run() throws Exception {
        var started = start("slow", "ASYNC");
        String id = started.getString("executionId");
        assertNotNull(id);
        try {
            var cancellation = object(post("/v1/executions/" + id + "/cancel"));
            assertTrue(cancellation.getBooleanValue("cancelRequested"));
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            JSONObject execution;
            do {
                execution = object(get("/v1/executions/" + id));
                if ("CANCELLED".equals(execution.getString("status"))) break;
                Thread.sleep(10);
            } while (System.nanoTime() < deadline);
            assertEquals("CANCELLED", execution.getString("status"), execution.toJSONString());
            mvc.perform(local(post("/v1/executions/" + id + "/resume")))
                    .andExpect(status().isConflict());
            assertEquals("CANCELLED", object(get("/v1/executions/" + id)).getString("status"));
        } finally {
            object(post("/v1/executions/" + id + "/cancel"));
        }
    }

    private JSONObject start(String flow, String mode) throws Exception {
        return object(
                post("/v1/executions")
                        .contentType("application/json")
                        .content(
                                JSON.toJSONString(
                                        Map.of(
                                                "flowReference",
                                                "demo:" + flow,
                                                "responseMode",
                                                mode))));
    }

    private JSONObject object(MockHttpServletRequestBuilder request) throws Exception {
        return JSON.parseObject(body(request));
    }

    private String body(MockHttpServletRequestBuilder request) throws Exception {
        return mvc.perform(local(request))
                .andExpect(status().is2xxSuccessful())
                .andReturn()
                .getResponse()
                .getContentAsString(java.nio.charset.StandardCharsets.UTF_8);
    }

    private MockHttpServletRequestBuilder local(MockHttpServletRequestBuilder request) {
        return request.with(
                http -> {
                    http.setLocalAddr("127.0.0.1");
                    http.setRemoteAddr("127.0.0.1");
                    return http;
                });
    }
}
