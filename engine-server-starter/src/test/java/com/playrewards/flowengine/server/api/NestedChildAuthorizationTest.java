package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.CapturedAuthorization;
import com.playrewards.flowengine.spi.RelatedFlowBundleLoader;

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
        Map<String, String> children = childExecutionIds(id);
        String events = events(id);
        assertFalse(events.contains("privateMiddleStart"), events);
        assertFalse(events.contains("privateLeafStart"), events);
        assertDeniedMetadataAbsent(
                restEvents(events), Set.copyOf(children.values()), children.keySet());
        String stream = stream(id);
        assertFalse(stream.contains("privateMiddleStart"), stream);
        assertFalse(stream.contains("privateLeafStart"), stream);
        assertDeniedMetadataAbsent(
                sseEvents(stream), Set.copyOf(children.values()), children.keySet());
        String deniedMiddleId = children.get("middle");
        mvc.perform(get("/v1/executions/" + deniedMiddleId).principal(() -> "reader"))
                .andExpect(status().isForbidden());
        mvc.perform(
                        get("/v1/executions/" + deniedMiddleId + "/events")
                                .principal(() -> "reader"))
                .andExpect(status().isForbidden());
        mvc.perform(
                        get("/v1/executions/" + deniedMiddleId + "/stream")
                                .principal(() -> "reader"))
                .andExpect(status().isForbidden())
                .andExpect(request().asyncNotStarted());
    }

    @Test
    void readable_middle_does_not_launder_private_grandchild_metadata() throws Exception {
        access.readable = Set.of("parent", "middle");
        String id = start();
        Map<String, String> children = childExecutionIds(id);
        String events = events(id);
        assertTrue(events.contains("privateMiddleStart"), events);
        assertFalse(events.contains("privateLeafStart"), events);
        assertDeniedMetadataAbsent(
                restEvents(events), Set.of(children.get("leaf")), Set.of("leaf"));
        String stream = stream(id);
        assertTrue(stream.contains("privateMiddleStart"), stream);
        assertFalse(stream.contains("privateLeafStart"), stream);
        assertDeniedMetadataAbsent(
                sseEvents(stream), Set.of(children.get("leaf")), Set.of("leaf"));
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

    @Test
    void request_bound_authorization_survives_async_sse_handoff() throws Exception {
        access.readable = Set.of("parent", "middle", "leaf");
        String stream = stream(start());

        assertTrue(stream.contains("event:COMPLETED"), stream);
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

    private Map<String, String> childExecutionIds(String rootExecutionId) {
        Map<String, String> children = new HashMap<>();
        service.store().readAfter(rootExecutionId, -1).forEach(event -> {
            Object flowId = event.attributes().get("flowId");
            Object childExecutionId = event.attributes().get("childExecutionId");
            if (flowId instanceof String flow && childExecutionId instanceof String child) {
                children.put(flow, child);
            }
        });
        assertEquals(Set.of("middle", "leaf"), children.keySet());
        return Map.copyOf(children);
    }

    private static List<JSONObject> restEvents(String body) {
        return JSON.parseArray(body, JSONObject.class);
    }

    private static List<JSONObject> sseEvents(String body) {
        return body.lines()
                .filter(line -> line.startsWith("data:"))
                .map(line -> JSON.parseObject(line.substring("data:".length())))
                .toList();
    }

    private static void assertDeniedMetadataAbsent(
            List<JSONObject> visibleEvents,
            Set<String> deniedExecutionIds,
            Set<String> deniedFlowIds) {
        assertFalse(visibleEvents.isEmpty(), "fixture must expose authorized root events");
        for (JSONObject event : visibleEvents) {
            String diagnostic = event.toJSONString();
            assertFalse(
                    deniedExecutionIds.contains(event.getString("executionId")), diagnostic);
            JSONObject attributes = event.getJSONObject("attributes");
            assertNotNull(attributes, diagnostic);
            for (String key : List.of("childExecutionId", "originExecutionId")) {
                String executionId = attributes.getString(key);
                assertTrue(
                        executionId == null || !deniedExecutionIds.contains(executionId),
                        key + " leaked denied execution metadata: " + diagnostic);
            }
            for (String key : List.of("childEventId", "originEventId")) {
                String eventId = attributes.getString(key);
                assertTrue(
                        eventId == null
                                || deniedExecutionIds.stream()
                                        .noneMatch(deniedId -> eventId.startsWith(deniedId + ":")),
                        key + " leaked denied execution metadata: " + diagnostic);
            }
            String flowId = attributes.getString("flowId");
            assertTrue(
                    flowId == null || !deniedFlowIds.contains(flowId),
                    "flowId leaked denied flow metadata: " + diagnostic);
            if (attributes.get("forwardingPath") instanceof List<?> path) {
                assertTrue(
                        Collections.disjoint(path, deniedExecutionIds),
                        "forwardingPath leaked denied execution metadata: " + diagnostic);
            }
        }
    }

    static final class SelectiveAccess implements ExecutionAccessPolicy {
        volatile Set<String> readable = Set.of("parent");

        @Override
        public CapturedAuthorization capture(HttpServletRequest request) {
            if (request.getUserPrincipal() == null) throw new SecurityException();
            return new ReaderAuthorization(
                    "tenant", request.getUserPrincipal().getName(), Set.copyOf(readable));
        }

        @Override
        public void authorize(
                CapturedAuthorization caller, FlowIdentity flow, Action action) {
            if (!(caller instanceof ReaderAuthorization authorization)
                    || !authorization.tenantId().equals(flow.tenantId())
                    || (action != Action.EXECUTE
                            && !authorization.readable().contains(flow.flowId())))
                throw new SecurityException();
        }

        private record ReaderAuthorization(
                String tenantId, String subject, Set<String> readable)
                implements CapturedAuthorization {}
    }

    @Configuration
    @EnableAutoConfiguration
    static class Host {
        @Bean
        SelectiveAccess access() {
            return new SelectiveAccess();
        }

        @Bean
        RelatedFlowBundleLoader loader() {
            return new RelatedFlowBundleLoader() {
                @Override
                public String scheme() {
                    return "nested";
                }

                @Override
                public ExecutableFlowBundle load(String reference) {
                    if (!"nested:parent".equals(reference)) throw new IllegalArgumentException();
                    return bundle("parent", "middle");
                }

                @Override
                public ExecutableFlowBundle loadRelated(
                        String rootReference, FlowIdentity parent, String id) {
                    if (!"nested:parent".equals(rootReference)) throw new IllegalArgumentException();
                    return switch (id) {
                        case "middle" -> bundle("middle", "leaf");
                        case "leaf" -> bundle("leaf", null);
                        default -> throw new IllegalArgumentException();
                    };
                }
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
