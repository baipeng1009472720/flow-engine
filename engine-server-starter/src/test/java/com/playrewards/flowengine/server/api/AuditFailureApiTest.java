package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.adapter.memory.InMemoryObservationStore;
import com.playrewards.flowengine.contract.execution.ExecutionEvent;
import com.playrewards.flowengine.core.compile.RelatedFlowResolver;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.*;
import org.springframework.test.web.servlet.MockMvc;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/** 真实 FlowEngine 和 HTTP/SSE 边界注入审计故障，不使用假的执行结果。 */
@SpringBootTest(
        classes = AuditFailureApiTest.Host.class,
        properties = "flow-engine.observation.strict-audit=true")
@AutoConfigureMockMvc
class AuditFailureApiTest {
    @Autowired MockMvc mvc;
    @Autowired Faults faults;

    @ParameterizedTest
    @ValueSource(strings = {"flush", "payload", "append", "commitTerminal", "child-flush"})
    void wait_query_replay_and_stream_never_report_success_after_audit_failure(String fault)
            throws Exception {
        faults.operation.set(fault);
        try {
            var wait =
                    mvc.perform(
                                    post("/v1/executions")
                                            .principal(() -> "tenant")
                                            .contentType("application/json")
                                            .content(
                                                    "{\"flowReference\":\""
                                                            + (fault.equals("child-flush")
                                                                    ? "nested:parent"
                                                                    : "test:flow")
                                                            + "\"}"))
                            .andExpect(status().isOk())
                            .andReturn()
                            .getResponse()
                            .getContentAsString();
            var summary = JSON.parseObject(wait);
            String id = summary.getString("executionId");
            assertEquals("FAILED", summary.getString("status"));
            if (fault.equals("child-flush")) {
                assertEquals("OBSERVATION_AUDIT_FAILED", summary.getString("errorCode"));
                String childId =
                        faults.memory.readAfter(id, -1).stream()
                                .filter(e -> e.eventType().equals("CHILD_CREATED"))
                                .map(e -> (String) e.attributes().get("childExecutionId"))
                                .findFirst()
                                .orElseThrow();
                assertEquals(
                        faults.memory.execution(childId).orElseThrow().errorId(),
                        summary.getString("errorId"));
            }
            var queried =
                    mvc.perform(get("/v1/executions/" + id).principal(() -> "tenant"))
                            .andExpect(status().isOk())
                            .andReturn()
                            .getResponse()
                            .getContentAsString();
            assertEquals(
                    summary.getString("status"), JSON.parseObject(queried).getString("status"));
            assertEquals(
                    summary.getString("errorId"), JSON.parseObject(queried).getString("errorId"));
            var events =
                    mvc.perform(get("/v1/executions/" + id + "/events").principal(() -> "tenant"))
                            .andExpect(status().isOk())
                            .andReturn()
                            .getResponse()
                            .getContentAsString();
            assertFalse(events.contains("\"eventType\":\"COMPLETED\""));
            var stream =
                    mvc.perform(get("/v1/executions/" + id + "/stream").principal(() -> "tenant"))
                            .andExpect(request().asyncStarted())
                            .andReturn();
            stream.getAsyncResult(5000);
            String body =
                    mvc.perform(asyncDispatch(stream))
                            .andReturn()
                            .getResponse()
                            .getContentAsString();
            assertFalse(body.contains("event:COMPLETED"));
            assertTrue(body.contains("event:FAILED"), "failed terminal must also reach SSE/replay");
        } finally {
            faults.operation.set("");
        }
    }

    static final class Faults {
        final InMemoryObservationStore memory = new InMemoryObservationStore();
        final AtomicReference<String> operation = new AtomicReference<>("");
    }

    @Configuration
    @Import(ExecutionRequestContractTest.Host.class)
    static class Host {
        @Bean
        FlowBundleLoader nestedLoader() {
            return new FlowBundleLoader() {
                public String scheme() {
                    return "nested";
                }

                public com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle load(
                        String ref) {
                    return NestedChildAuthorizationTest.bundle("parent", "middle");
                }
            };
        }

        @Bean
        RelatedFlowResolver children() {
            return (parent, id) -> NestedChildAuthorizationTest.bundle("middle", null);
        }

        @Bean
        Faults faults() {
            return new Faults();
        }

        @Bean
        ExecutionRecordStore records(Faults faults) {
            return (ExecutionRecordStore)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {ExecutionRecordStore.class},
                            (proxy, method, args) -> {
                                String fault = faults.operation.get();
                                if (fault.equals("child-flush")
                                        && method.getName().equals("flush")
                                        && faults.memory
                                                        .execution((String) args[0])
                                                        .orElseThrow()
                                                        .parentExecutionId()
                                                != null)
                                    throw new IllegalStateException("private child flush");
                                if (method.getName().equals(fault)
                                        && (!fault.equals("append")
                                                || ((ExecutionEvent) args[0])
                                                        .eventType()
                                                        .equals("NODE_COMPLETED")))
                                    throw new IllegalStateException("private-adapter-message");
                                try {
                                    return method.invoke(faults.memory, args);
                                } catch (InvocationTargetException failure) {
                                    throw failure.getCause();
                                }
                            });
        }

        @Bean
        PayloadStore payloads(Faults faults) {
            return new PayloadStore() {
                public PayloadReference store(String id, Object value) {
                    if (faults.operation.get().equals("payload"))
                        throw new IllegalStateException("private");
                    return faults.memory.store(id, value);
                }

                public Optional<Object> find(PayloadReference reference) {
                    return faults.memory.find(reference);
                }

                public Optional<String> executionId(PayloadReference reference) {
                    return faults.memory.executionId(reference);
                }
            };
        }
    }
}
