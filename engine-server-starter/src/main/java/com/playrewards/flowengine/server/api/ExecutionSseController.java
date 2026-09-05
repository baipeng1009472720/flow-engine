package com.playrewards.flowengine.server.api;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.Action;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.concurrent.*;

/**
 * Network sending runs on a separate, finite worker pool; disconnect never cancels business
 * execution.
 */
@RestController
public class ExecutionSseController implements AutoCloseable {
    private final ExecutionService service;
    private final BoundedExecutionEventBus bus;
    private final ThreadPoolExecutor senders =
            new ThreadPoolExecutor(
                    0,
                    512,
                    30,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    Thread.ofVirtual().name("execution-sse-", 0).factory(),
                    new ThreadPoolExecutor.AbortPolicy());

    /** Uses shared replay storage but owns its finite network-sender pool. */
    public ExecutionSseController(ExecutionService service, BoundedExecutionEventBus bus) {
        this.service = service;
        this.bus = bus;
    }

    /**
     * Creates a new execution once; reconnect cursors are rejected on this side-effecting route.
     */
    @PostMapping(value = "/v1/executions/stream", produces = "text/event-stream")
    public SseEmitter start(HttpServletRequest request) throws Exception {
        if (request.getHeader("Last-Event-ID") != null)
            throw new IllegalArgumentException("reconnect requires GET with executionId");
        var started = service.start(request);
        return stream(request, started.handle().executionId());
    }

    /**
     * Attaches replay and live delivery to an authorized existing execution without restarting it.
     */
    @GetMapping(value = "/v1/executions/{id}/stream", produces = "text/event-stream")
    public SseEmitter stream(HttpServletRequest request, @PathVariable("id") String id) {
        service.execution(request, id, Action.READ);
        String tenant = service.streamTenant(request);
        var subscription = bus.subscribe(id, request.getHeader("Last-Event-ID"));
        var emitter = new SseEmitter(310000L);
        emitter.onCompletion(subscription::close);
        emitter.onTimeout(subscription::close);
        emitter.onError(error -> subscription.close());
        try {
            senders.execute(
                    () -> {
                        try (subscription) {
                            while (!subscription.closed()) {
                                var event = subscription.poll(1000);
                                if (event == null) {
                                    if (subscription.closed()) break;
                                    // A reconnect at the final cursor has nothing left to send.
                                    var record = service.store().execution(id);
                                    if (record.isEmpty() || record.get().finishedAt() != null)
                                        break;
                                    emitter.send(SseEmitter.event().comment("keepalive"));
                                    continue;
                                }
                                var visible = service.streamEvent(tenant, event);
                                if (visible == null) continue;
                                emitter.send(
                                        SseEmitter.event()
                                                .id(event.eventId())
                                                .name(event.eventType())
                                                .data(JSON.toJSONString(visible)));
                                if (event.nodeId() == null
                                        && java.util.Set.of(
                                                        "COMPLETED",
                                                        "FAILED",
                                                        "CANCELLED",
                                                        "WAITING")
                                                .contains(event.eventType())) break;
                            }
                            emitter.complete();
                        } catch (Exception failure) {
                            subscription.close();
                            emitter.completeWithError(
                                    new IllegalStateException("SSE_DISCONNECTED"));
                        }
                    });
        } catch (RejectedExecutionException failure) {
            subscription.close();
            throw new ApiException(429, "SUBSCRIBER_CAPACITY_EXCEEDED", "RESOURCE_LIMIT");
        }
        return emitter;
    }

    /**
     * Stops network work during host shutdown; execution cleanup is independently owned by
     * FlowEngine.
     */
    @Override
    public void close() {
        senders.shutdownNow();
    }
}
