package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.core.compile.RelatedFlowResolver;
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

/**
 * Parent execution permission does not grant access to private child or grandchild observations.
 */
@SpringBootTest(classes = NestedChildAuthorizationTest.Host.class)
@AutoConfigureMockMvc
class NestedChildAuthorizationTest {
    @Autowired MockMvc mvc;
    @Autowired SelectiveAccess access;
    @Autowired ExecutionService service;

    @Test
    void parent_read_does_not_reveal_child_nodes_through_events_or_sse() throws Exception {
        access.readable = Set.of("parent");
        String id = start();
        String events = events(id);
        assertFalse(events.contains("privateMiddleStart"), events);
        assertFalse(events.contains("privateLeafStart"), events);
        String stream = stream(id);
        assertFalse(stream.contains("privateMiddleStart"), stream);
        assertFalse(stream.contains("privateLeafStart"), stream);
        String childId =
                service.store().readAfter(id, -1).stream()
                        .filter(event -> event.eventType().equals("CHILD_CREATED"))
                        .map(event -> (String) event.attributes().get("childExecutionId"))
                        .findFirst()
                        .orElseThrow();
        mvc.perform(get("/v1/executions/" + childId).principal(() -> "reader"))
                .andExpect(status().isForbidden());
    }

    @Test
    void readable_middle_does_not_launder_private_grandchild_metadata() throws Exception {
        access.readable = Set.of("parent", "middle");
        String id = start();
        String events = events(id);
        assertTrue(events.contains("privateMiddleStart"), events);
        assertFalse(events.contains("privateLeafStart"), events);
        String stream = stream(id);
        assertTrue(stream.contains("privateMiddleStart"), stream);
        assertFalse(stream.contains("privateLeafStart"), stream);
    }

    @Test
    void committed_nested_terminals_keep_original_owner_and_forwarding_path() throws Exception {
        access.readable = Set.of("parent", "middle", "leaf");
        String id = start();
        var terminals =
                service.store().readAfter(id, -1).stream()
                        .filter(
                                event ->
                                        "COMPLETED"
                                                .equals(event.attributes().get("originEventType")))
                        .toList();
        assertEquals(2, terminals.size());
        for (var terminal : terminals) {
            String origin = (String) terminal.attributes().get("originExecutionId");
            assertEquals(
                    "SUCCEEDED", service.store().execution(origin).orElseThrow().status().name());
            assertTrue(((List<?>) terminal.attributes().get("forwardingPath")).contains(origin));
            assertTrue(
                    ((String) terminal.attributes().get("originEventId")).startsWith(origin + ":"));
        }
        assertTrue(events(id).contains("originEventType"));
        assertTrue(stream(id).contains("originEventType"));
    }

    private String start() throws Exception {
        String json =
                mvc.perform(
                                post("/v1/executions")
                                        .principal(() -> "reader")
                                        .contentType("application/json")
                                        .content("{\"flowReference\":\"nested:parent\"}"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse()
                        .getContentAsString();
        var result = JSON.parseObject(json);
        assertEquals("SUCCEEDED", result.getString("status"), json);
        return result.getString("executionId");
    }

    private String events(String id) throws Exception {
        return mvc.perform(
                        get("/v1/executions/" + id + "/events")
                                .param("limit", "500")
                                .principal(() -> "reader"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
    }

    private String stream(String id) throws Exception {
        var response =
                mvc.perform(get("/v1/executions/" + id + "/stream").principal(() -> "reader"))
                        .andExpect(request().asyncStarted())
                        .andReturn();
        response.getAsyncResult(5000);
        return mvc.perform(asyncDispatch(response))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();
    }

    static final class SelectiveAccess implements ExecutionAccessPolicy {
        volatile Set<String> readable = Set.of("parent");

        @Override
        public String tenantId(HttpServletRequest request) {
            if (request.getUserPrincipal() == null) throw new SecurityException();
            return "tenant";
        }

        @Override
        public void authorize(String tenant, FlowIdentity flow, Action action) {
            if (!tenant.equals(flow.tenantId())
                    || (action != Action.EXECUTE && !readable.contains(flow.flowId())))
                throw new SecurityException();
        }
    }

    @Configuration
    @EnableAutoConfiguration
    static class Host {
        @Bean
        SelectiveAccess access() {
            return new SelectiveAccess();
        }

        @Bean
        FlowBundleLoader loader() {
            return new FlowBundleLoader() {
                @Override
                public String scheme() {
                    return "nested";
                }

                @Override
                public ExecutableFlowBundle load(String reference) {
                    if (!"nested:parent".equals(reference)) throw new IllegalArgumentException();
                    return bundle("parent", "middle");
                }
            };
        }

        @Bean
        RelatedFlowResolver resolver() {
            return (parent, id) ->
                    switch (id) {
                        case "middle" -> bundle("middle", "leaf");
                        case "leaf" -> bundle("leaf", null);
                        default -> throw new IllegalArgumentException();
                    };
        }
    }

    static ExecutableFlowBundle bundle(String name, String child) {
        String start =
                switch (name) {
                    case "middle" -> "privateMiddleStart";
                    case "leaf" -> "privateLeafStart";
                    default -> "start";
                };
        var nodes =
                new ArrayList<FlowNode>(
                        List.of(
                                new FlowNode(start, "start", start, start, Map.of()),
                                new FlowNode(
                                        "end",
                                        "end",
                                        "end",
                                        "end",
                                        Map.of("resultStatus", "SUCCESS"))));
        var edges = new ArrayList<FlowEdge>();
        if (child != null) {
            nodes.add(
                    new FlowNode(
                            "invoke", "sub", "invoke", "invoke", Map.of("targetFlowId", child)));
            edges.add(new FlowEdge("a", start, "invoke", "NEXT", "IN"));
            edges.add(new FlowEdge("b", "invoke", "end", "NEXT", "IN"));
        } else edges.add(new FlowEdge("a", start, "end", "NEXT", "IN"));
        var contracts = new LinkedHashMap<String, CompiledComponentContract>();
        var caps = new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 4);
        Map.of(
                        "start",
                        ComponentType.START,
                        "end",
                        ComponentType.END,
                        "sub",
                        ComponentType.SUB_FLOW)
                .forEach(
                        (code, type) -> {
                            if (!code.equals("sub") || child != null)
                                contracts.put(
                                        code,
                                        new CompiledComponentContract(
                                                code, type, null, Map.of(), Map.of(), Map.of(),
                                                caps));
                        });
        return new ExecutableFlowBundle(
                        "1.0",
                        new FlowIdentity("tenant", name, "v1"),
                        new FlowDefinition("2.0", nodes, edges),
                        contracts,
                        Map.of(),
                        Map.of(),
                        null,
                        null)
                .withCalculatedHash();
    }
}
