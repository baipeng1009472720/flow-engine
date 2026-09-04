package com.playrewards.flowengine.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.adapter.http.JdkHttpNodeClient;
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
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.execution.ExecutionHandle;
import com.playrewards.flowengine.core.execution.FlowEngine;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import com.playrewards.flowengine.spi.LockLease;
import com.playrewards.flowengine.spi.LockRequest;
import com.playrewards.flowengine.spi.ScriptRuntime;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Proves the real FlowEngine-to-pinned-socket HTTP lifecycle in the composition module. */
class HttpFlowEndToEndTest {

    private static final ProtocolSchema OBJECT =
            new ProtocolSchema("response", "1.0", Map.of("type", "object"));
    private static final DistributedLockProvider NO_LOCKS = new DistributedLockProvider() {
        @Override
        public Optional<LockLease> tryAcquire(LockRequest request) {
            return Optional.empty();
        }

        @Override
        public boolean release(LockLease lease) {
            return false;
        }
    };

    @Test
    void successful_output_traverses_the_complete_runtime_and_real_transport() throws Exception {
        try (Fixture fixture = new Fixture();
                JdkHttpNodeClient client = fixture.client(1);
                FlowEngine engine = engine(client)) {
            ExecutionResult result = await(engine.execute(
                    plan("GET", fixture.uri("/success"), true), request()));

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals(Map.of("ok", true), result.result());
            assertEquals(1, fixture.successHits);
        }
    }

    @Test
    void cancel_after_unprotected_send_publishes_manual_only_after_transport_capacity_is_released()
            throws Exception {
        try (Fixture fixture = new Fixture();
                JdkHttpNodeClient client = fixture.client(1);
                FlowEngine engine = engine(client)) {
            ExecutionHandle handle = engine.execute(plan("POST", fixture.uri("/manual"), false), request());
            assertTrue(fixture.requestReceived.await(2, TimeUnit.SECONDS));

            assertTrue(handle.cancel());
            ExecutionResult result = await(handle);

            assertEquals(ExecutionStatus.WAITING_MANUAL, result.status());
            assertEquals("HTTP_SIDE_EFFECT_UNCERTAIN", result.errorCode());
            assertEquals(200, client.execute(fixture.transportRequest("/success"))
                    .get(2, TimeUnit.SECONDS).statusCode());
            fixture.allowManualResponse.countDown();
        } finally {
            // Fixture close releases a server handler even when the assertion above fails.
        }
    }

    @Test
    void engine_close_after_unprotected_send_preserves_manual_terminal_arbitration() throws Exception {
        try (Fixture fixture = new Fixture(); JdkHttpNodeClient client = fixture.client(1)) {
            FlowEngine engine = engine(client);
            ExecutionHandle handle = engine.execute(plan("PATCH", fixture.uri("/manual"), false), request());
            assertTrue(fixture.requestReceived.await(2, TimeUnit.SECONDS));

            Thread closer = Thread.ofVirtual().start(engine::close);
            ExecutionResult result = await(handle);
            closer.join(2_000);

            assertEquals(ExecutionStatus.WAITING_MANUAL, result.status());
            assertEquals("HTTP_SIDE_EFFECT_UNCERTAIN", result.errorCode());
            assertFalse(closer.isAlive());
            fixture.allowManualResponse.countDown();
        }
    }

    @Test
    void refused_connection_is_a_pre_send_failure_not_manual() throws Exception {
        int unusedPort;
        try (ServerSocket reserved = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            unusedPort = reserved.getLocalPort();
        }
        URI uri = URI.create("http://" + InetAddress.getLoopbackAddress().getHostAddress()
                + ":" + unusedPort + "/refused");
        HttpTargetPolicy local = (target, purpose, cancellation, deadline) ->
                new HttpTargetPolicy.ApprovedTarget(
                        target, List.of(InetAddress.getLoopbackAddress()), unusedPort);
        try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                local, local, 1, Duration.ofMillis(5)); FlowEngine engine = engine(client)) {
            ExecutionResult result = await(engine.execute(plan("POST", uri, false), request()));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals("HTTP_CONNECTION_FAILED", result.errorCode());
        }
    }

    private static FlowEngine engine(JdkHttpNodeClient client) {
        ScriptRuntime scripts = invocation ->
                java.util.concurrent.CompletableFuture.completedFuture(Map.of("operation", "charge"));
        return new FlowEngine(scripts, new FlowBusinessComponentRegistry(List.of()),
                NO_LOCKS, client, Clock.systemUTC());
    }

    private static ExecutionPlan plan(String method, URI uri, boolean output) {
        Map<String, Object> config = new java.util.LinkedHashMap<>();
        config.put("method", method);
        config.put("url", uri.toString());
        config.put("requestParamsScript", "return [operation: 'charge']");
        config.put("headers", Map.of());
        config.put("timeoutMillis", 3_000);
        config.put("retryCount", 0);
        if (output) {
            config.put("outputProtocol", Map.of("code", OBJECT.code(), "schema", OBJECT.schema()));
        }
        CompiledNode start = node("start", ComponentType.START, Map.of(), ComponentEffect.PURE, Map.of());
        CompiledNode http = node("http", ComponentType.HTTP, config, ComponentEffect.SIDE_EFFECT,
                output ? Map.of("result", new OutputSlotContract("result", OBJECT)) : Map.of());
        CompiledNode end = node("end", ComponentType.END,
                output ? Map.of("resultStatus", "SUCCESS", "returnBinding",
                        Map.of("sourceNodeId", "http", "sourceOutputKey", "result"))
                        : Map.of("resultStatus", "SUCCESS"), ComponentEffect.PURE, Map.of());
        return new ExecutionPlan(new FlowIdentity("tenant", "flow", "v1"), "hash",
                Map.of("start", start, "http", http, "end", end),
                Map.of("start", List.of("http"), "http", List.of("end")),
                Map.of("start", List.of(), "http", List.of("start"), "end", List.of("http")),
                Map.of("start", Map.of("NEXT", List.of("http")),
                        "http", Map.of("NEXT", List.of("end"))),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static CompiledNode node(String id, ComponentType type, Map<String, Object> config,
            ComponentEffect effect, Map<String, OutputSlotContract> outputs) {
        return new CompiledNode(new FlowNode(id, "builtin." + type.name().toLowerCase(), id, id, config),
                new CompiledComponentContract("builtin." + type.name().toLowerCase(), type, null,
                        Map.of(), Map.of(), outputs,
                        new ComponentCapabilities(effect, true, false, true, 8)), Map.of());
    }

    private static ExecutionRequest request() {
        return new ExecutionRequest("inline:flow", ExecutionMode.EPHEMERAL, ExecutionPurpose.TRIAL,
                Map.of(), Map.of(), null, "request-key", "tenant",
                Instant.now().plusSeconds(5), Map.of());
    }

    private static ExecutionResult await(ExecutionHandle handle) throws Exception {
        return handle.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static final class Fixture implements AutoCloseable {

        private final HttpServer server;
        private final java.util.concurrent.ExecutorService tasks =
                java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch requestReceived = new CountDownLatch(1);
        private final CountDownLatch allowManualResponse = new CountDownLatch(1);
        private volatile int successHits;

        private Fixture() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/success", exchange -> {
                successHits++;
                byte[] body = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.createContext("/manual", exchange -> {
                try {
                    exchange.getRequestBody().readAllBytes();
                    requestReceived.countDown();
                    allowManualResponse.await();
                    exchange.sendResponseHeaders(204, -1);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.setExecutor(tasks);
            server.start();
        }

        private URI uri(String path) {
            return URI.create("http://" + server.getAddress().getHostString() + ":"
                    + server.getAddress().getPort() + path);
        }

        private JdkHttpNodeClient client(int capacity) {
            HttpTargetPolicy local = (target, purpose, cancellation, deadline) ->
                    new HttpTargetPolicy.ApprovedTarget(
                            target, List.of(server.getAddress().getAddress()), server.getAddress().getPort());
            return new JdkHttpNodeClient(local, local, capacity, Duration.ofMillis(5));
        }

        private com.playrewards.flowengine.spi.HttpNodeClient.HttpRequest transportRequest(String path) {
            return new com.playrewards.flowengine.spi.HttpNodeClient.HttpRequest(
                    "GET", uri(path), Map.of(), new byte[0], Duration.ofSeconds(2),
                    Instant.now().plusSeconds(3), 1_024, ExecutionPurpose.TRIAL, () -> false);
        }

        @Override
        public void close() {
            allowManualResponse.countDown();
            server.stop(0);
            tasks.shutdownNow();
        }
    }
}
