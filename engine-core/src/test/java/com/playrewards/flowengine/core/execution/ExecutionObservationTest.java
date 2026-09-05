package com.playrewards.flowengine.core.execution;

import static com.playrewards.flowengine.core.execution.EphemeralExecutionTest.*;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.node.*;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Exercises the scheduler's real publisher lifecycle, not only executor return values. */
class ExecutionObservationTest {
    @Test
    void accepts_json_null_as_content_payload() {
        AtomicBoolean consumed = new AtomicBoolean();
        try (var drain = new ContentDrain(chunk -> consumed.set(true))) {
            drain.onNext(
                    new com.playrewards.flowengine.contract.execution.ContentChunk(
                            "execution", "node", "root", 0, "application/json", null));
            assertTrue(consumed.get(), "JSON null is valid content, not a missing chunk");
        }
    }

    @Test
    void subscribes_to_content_before_waiting_for_final_output() throws Exception {
        AtomicBoolean subscribed = new AtomicBoolean();
        var plan =
                compile(
                        List.of(
                                node("start", "start", "start", Map.of()),
                                node("stream", "stream", "stream", Map.of()),
                                node("end", "end", "end", Map.of("resultStatus", "SUCCESS"))),
                        List.of(
                                edge("a", "start", "stream", "NEXT", "IN"),
                                edge("b", "stream", "end", "NEXT", "IN")),
                        Map.of(
                                "start",
                                contract("start", ComponentType.START, Map.of(), Map.of()),
                                "stream",
                                contract("stream", ComponentType.SCRIPT, Map.of(), Map.of()),
                                "end",
                                contract("end", ComponentType.END, Map.of(), Map.of())));
        NodeExecutor executor =
                new NodeExecutor() {
                    public ComponentType supports() {
                        return ComponentType.SCRIPT;
                    }

                    public NodeInvocation execute(NodeExecutionContext context) {
                        var done = new CompletableFuture<NodeResult>();
                        return new NodeInvocation(
                                done,
                                subscriber -> {
                                    subscribed.set(true);
                                    subscriber.onSubscribe(
                                            new Flow.Subscription() {
                                                public void request(long n) {
                                                    subscriber.onComplete();
                                                    done.complete(
                                                            NodeResult.continueWith(
                                                                    Map.of(), Set.of("NEXT")));
                                                }

                                                public void cancel() {}
                                            });
                                });
                    }
                };
        try (var engine = new FlowEngine(NodeExecutorRegistry.withBuiltIns(List.of(executor)))) {
            var handle =
                    engine.execute(
                            plan, request(Map.of(), java.time.Instant.now().plusMillis(300)));
            var result = await(handle);
            assertTrue(
                    subscribed.get(), "scheduler must subscribe before awaiting the final value");
            assertEquals(
                    com.playrewards.flowengine.contract.execution.ExecutionStatus.SUCCEEDED,
                    result.status());
        }
    }
}
