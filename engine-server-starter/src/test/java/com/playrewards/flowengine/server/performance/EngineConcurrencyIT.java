package com.playrewards.flowengine.server.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.compile.FlowCompiler;
import com.playrewards.flowengine.core.execution.FlowEngine;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.CapturedAuthorization;
import com.playrewards.flowengine.spi.ComponentExecutionContext;
import com.playrewards.flowengine.spi.ComponentInvocation;
import com.playrewards.flowengine.spi.ComponentOutputs;
import com.playrewards.flowengine.spi.FlowBundleLoader;
import com.playrewards.flowengine.spi.FlowBusinessComponent;

import jakarta.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;

import java.io.InputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Profile-gated real HTTP capacity checks for the first-release engineering thresholds.
 *
 * <p>The fixture owns no external service. It deliberately keeps the production scheduler,
 * controller, SSE sender and observation limits real, while replacing only the host-specific
 * trusted bundle catalog and a deterministic blocking business operation.</p>
 */
@SpringBootTest(
        classes = EngineConcurrencyIT.Host.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.threads.virtual.enabled=true",
            "flow-engine.execution.max-duration=45s",
            "flow-engine.concurrency.global=2048",
            "flow-engine.concurrency.per-tenant=1024",
            "flow-engine.concurrency.per-flow=1024",
            "flow-engine.concurrency.per-component=512",
            "flow-engine.concurrency.per-resource=512",
            "flow-engine.events.max-subscribers=512",
            "flow-engine.events.buffer-per-execution=64",
            "flow-engine.observation.max-executions=5000",
            "flow-engine.observation.max-nodes=10000",
            "flow-engine.observation.events.max-count=64",
            "flow-engine.observation.logs.max-count=32",
            "flow-engine.observation.content.max-count=16",
            "flow-engine.observation.payloads.max-count=10000"
        })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EngineConcurrencyIT {
    private static final int EPHEMERAL_CONCURRENCY = 1_000;
    private static final int SSE_CONCURRENCY = 500;
    private static final int IO_CONCURRENCY = 200;
    private static final long MAX_HEAP_GROWTH_BYTES = 512L * 1024 * 1024;
    private static final int MAX_PLATFORM_THREAD_GROWTH = 96;
    private static final int MAX_EPHEMERAL_LOAD_GENERATOR_THREAD_GROWTH = 256;

    @LocalServerPort int port;
    @Autowired BenchmarkComponent component;
    @Autowired FlowEngine engine;
    @Autowired FlowCompiler compiler;
    @Autowired FlowBundleLoader bundles;
    @Autowired com.playrewards.flowengine.core.execution.ConcurrencyGovernor governor;

    private HttpClient client;
    private com.sun.net.httpserver.HttpServer downstream;
    private ExecutorService downstreamWorkers;

    @BeforeAll
    void createClient() throws IOException {
        client =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .version(HttpClient.Version.HTTP_1_1)
                        .build();
        downstream = com.sun.net.httpserver.HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), IO_CONCURRENCY);
        downstreamWorkers = Executors.newVirtualThreadPerTaskExecutor();
        downstream.setExecutor(downstreamWorkers);
        downstream.createContext(
                "/io",
                exchange -> {
                    try (exchange) {
                        Thread.sleep(Duration.ofSeconds(2));
                        byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        exchange.close();
                    }
                });
        downstream.start();
        component.ioTarget(URI.create(
                "http://127.0.0.1:" + downstream.getAddress().getPort() + "/io"));
    }

    @AfterAll
    void closeDownstream() {
        if (downstream != null) downstream.stop(0);
        if (downstreamWorkers != null) downstreamWorkers.close();
    }

    @AfterEach
    void releaseBlockedInvocations() {
        component.release();
    }

    @Test
    @Order(1)
    void one_thousand_concurrent_lightweight_ephemeral_executions_reach_success_with_bounded_resources()
            throws Exception {
        component.block();
        CapacityMetrics metrics =
                runConcurrent(
                        "ephemeral",
                        EPHEMERAL_CONCURRENCY,
                        index -> executeWait("benchmark:light", index),
                        this::releaseAfterAllExecutionsReachNodeAdmission);

        assertEquals(EPHEMERAL_CONCURRENCY, metrics.succeeded());
        assertEquals(0, metrics.failed());
        assertTrue(
                metrics.nodePermitInFlightHighWater() >= EPHEMERAL_CONCURRENCY,
                "all 1000 executions must be concurrently admitted by the server: " + metrics);
        assertEquals(0, governor.snapshot().inFlightAcquisitions());
        assertCapacityBounds(metrics);
        assertTrue(metrics.p99Millis() < 45_000, metrics::toString);
        publish(metrics);
    }

    @Test
    @Order(2)
    void five_hundred_concurrent_sse_connections_preserve_event_order_and_finish_without_loss()
            throws Exception {
        component.block();
        ResourceSampler sampler = new ResourceSampler(governor);
        long started = System.nanoTime();
        List<SseConnection> responses = new ArrayList<>(SSE_CONCURRENCY);
        List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        try (sampler; ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<SseConnection>> pending = new ArrayList<>();
            for (int index = 0; index < SSE_CONCURRENCY; index++) {
                int requestIndex = index;
                pending.add(
                        CompletableFuture.supplyAsync(
                                () -> openSse(requestIndex), callers));
            }
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
            for (CompletableFuture<SseConnection> response : pending) {
                SseConnection opened = response.join();
                assertEquals(200, opened.response().statusCode());
                responses.add(opened);
            }

            component.release();
            List<CompletableFuture<Void>> readers = new ArrayList<>();
            for (SseConnection response : responses) {
                readers.add(
                        CompletableFuture.runAsync(
                                () -> {
                                    assertOrderedTerminalEvents(response.response().body());
                                    latencies.add(elapsedMillis(response.openedAtNanos()));
                                },
                                callers));
            }
            CompletableFuture.allOf(readers.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
        } finally {
            for (SseConnection response : responses) {
                response.response().body().close();
            }
        }
        CapacityMetrics metrics =
                sampler.metrics(
                        "sse",
                        SSE_CONCURRENCY,
                        SSE_CONCURRENCY,
                        0,
                        elapsedMillis(started),
                        latencies);
        assertCapacityBounds(metrics);
        publish(metrics);
    }

    @Test
    @Order(3)
    void two_hundred_concurrent_blocking_io_executions_use_virtual_threads_and_complete()
            throws Exception {
        CapacityMetrics metrics =
                runConcurrent(
                        "blocking-io",
                        IO_CONCURRENCY,
                        index -> executeWait("benchmark:io", index));

        assertEquals(IO_CONCURRENCY, metrics.succeeded());
        assertEquals(0, metrics.failed());
        assertTrue(component.maxActive() >= 180, "blocking work did not overlap: " + component.maxActive());
        assertCapacityBounds(metrics);
        assertTrue(metrics.p99Millis() < 15_000, metrics::toString);
        publish(metrics);
    }

    private CapacityMetrics runConcurrent(String name, int count, CheckedIntConsumer operation)
            throws Exception {
        return runConcurrent(name, count, operation, () -> {});
    }

    private CapacityMetrics runConcurrent(
            String name, int count, CheckedIntConsumer operation, Runnable afterStart)
            throws Exception {
        CountDownLatch ready = new CountDownLatch(count);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        List<Long> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        long wallStarted = System.nanoTime();
        try (ResourceSampler sampler = new ResourceSampler(governor);
                ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> pending = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                int requestIndex = index;
                pending.add(
                        CompletableFuture.runAsync(
                                () -> {
                                    ready.countDown();
                                    await(start);
                                    long started = System.nanoTime();
                                    try {
                                        operation.accept(requestIndex);
                                        succeeded.incrementAndGet();
                                    } catch (Throwable failure) {
                                        failed.incrementAndGet();
                                        throw new IllegalStateException(failure);
                                    } finally {
                                        latencies.add(elapsedMillis(started));
                                    }
                                },
                                callers));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS), "load workers did not become ready");
            start.countDown();
            afterStart.run();
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                    .get(60, TimeUnit.SECONDS);
            return sampler.metrics(
                    name,
                    count,
                    succeeded.get(),
                    failed.get(),
                    elapsedMillis(wallStarted),
                    latencies);
        }
    }

    private void releaseAfterAllExecutionsReachNodeAdmission() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            while (System.nanoTime() < deadline) {
                var permits = governor.snapshot();
                if (permits.inFlightAcquisitions() >= EPHEMERAL_CONCURRENCY) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("capacity observation interrupted", failure);
                    }
                    return;
                }
                Thread.onSpinWait();
            }
            throw new AssertionError(
                    "1000 executions did not concurrently reach node admission: "
                            + governor.snapshot());
        } finally {
            component.release();
        }
    }

    private ExecutionPlan plan(String reference) {
        return compiler.compile(bundles.load(reference));
    }

    private void executeWait(String reference, int index) throws Exception {
        String body = JSON.toJSONString(
                Map.of(
                        "flowReference", reference,
                        "mode", "EPHEMERAL",
                        "responseMode", "WAIT",
                        "idempotencyKey", reference + '-' + index + '-' + UUID.randomUUID()));
        HttpResponse<String> response = client.send(
                requestBuilder("/v1/executions")
                        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, response.statusCode(), response::body);
        JSONObject result = JSON.parseObject(response.body());
        assertEquals("SUCCEEDED", result.getString("status"), result::toJSONString);
    }

    private static ExecutionRequest request(String reference, int index) {
        return new ExecutionRequest(
                reference,
                ExecutionMode.EPHEMERAL,
                ExecutionPurpose.PRODUCTION,
                Map.of(),
                Map.of(),
                null,
                reference + '-' + index + '-' + UUID.randomUUID(),
                "benchmark",
                Instant.now().plusSeconds(45),
                Map.of());
    }

    private HttpResponse<InputStream> sendSse(int index) {
        try {
            HttpRequest request =
                    requestBuilder("/v1/executions/stream")
                            .POST(
                                    HttpRequest.BodyPublishers.ofString(
                                            JSON.toJSONString(
                                                    Map.of(
                                                            "flowReference",
                                                            "benchmark:block",
                                                            "mode",
                                                            "EPHEMERAL",
                                                            "idempotencyKey",
                                                            "sse-" + index + '-' + UUID.randomUUID()))))
                            .build();
            return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private SseConnection openSse(int index) {
        long started = System.nanoTime();
        return new SseConnection(sendSse(index), started);
    }

    private HttpRequest.Builder requestBuilder(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(45))
                .header("Content-Type", "application/json")
                .header("X-Benchmark-Tenant", "benchmark");
    }

    private static void assertOrderedTerminalEvents(InputStream input) {
        try (input) {
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            List<JSONObject> events =
                    text.replace("\r\n", "\n").lines()
                            .filter(line -> line.startsWith("data:"))
                            .map(line -> JSON.parseObject(line.substring(5)))
                            .toList();
            assertTrue(events.size() >= 5, "incomplete SSE event stream: " + text);
            long previous = -1;
            Set<String> ids = new java.util.HashSet<>();
            for (JSONObject event : events) {
                long sequence = event.getLongValue("sequence");
                assertTrue(sequence > previous, "out-of-order SSE event sequence: " + text);
                assertTrue(ids.add(event.getString("eventId")), "duplicate SSE event: " + text);
                previous = sequence;
            }
            JSONObject terminal = events.getLast();
            assertEquals("COMPLETED", terminal.getString("eventType"), terminal::toJSONString);
            assertEquals("SUCCEEDED", terminal.getString("status"), terminal::toJSONString);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static void assertCapacityBounds(CapacityMetrics metrics) {
        assertTrue(metrics.heapGrowthBytes() <= MAX_HEAP_GROWTH_BYTES, metrics::toString);
        int platformThreadLimit =
                metrics.scenario().equals("ephemeral")
                        ? MAX_EPHEMERAL_LOAD_GENERATOR_THREAD_GROWTH
                        : MAX_PLATFORM_THREAD_GROWTH;
        assertTrue(metrics.platformThreadGrowth() <= platformThreadLimit, metrics::toString);
        assertTrue(metrics.nodePermitActiveHighWater() <= 2_048, metrics::toString);
        assertTrue(metrics.nodePermitWaitingHighWater() <= metrics.requested(), metrics::toString);
    }

    private static void publish(CapacityMetrics metrics) {
        System.out.println("TASK14_CAPACITY " + JSON.toJSONString(metrics));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    private static long percentile(List<Long> values, double percentile) {
        if (values.isEmpty()) return 0;
        List<Long> sorted = values.stream().sorted(Comparator.naturalOrder()).toList();
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(percentile * sorted.size()) - 1);
        return sorted.get(Math.max(0, index));
    }

    @FunctionalInterface
    private interface CheckedIntConsumer {
        void accept(int index) throws Exception;
    }

    private record SseConnection(HttpResponse<InputStream> response, long openedAtNanos) {}

    private record CapacityMetrics(
            String scenario,
            int requested,
            int succeeded,
            int failed,
            long wallMillis,
            long p95Millis,
            long p99Millis,
            long heapBaselineBytes,
            long heapHighWaterBytes,
            long heapGrowthBytes,
            int platformThreadsBaseline,
            int platformThreadsHighWater,
            int platformThreadGrowth,
            int nodePermitActiveHighWater,
            int nodePermitWaitingHighWater,
            int nodePermitInFlightHighWater) {}

    /** Samples process resources without retaining request or payload data. */
    private static final class ResourceSampler implements AutoCloseable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final long heapBaseline = usedHeap();
        private final int threadBaseline = platformThreads();
        private final AtomicLong heapHighWater = new AtomicLong(heapBaseline);
        private final AtomicInteger threadHighWater = new AtomicInteger(threadBaseline);
        private final com.playrewards.flowengine.core.execution.ConcurrencyGovernor governor;
        private final AtomicInteger permitActiveHighWater = new AtomicInteger();
        private final AtomicInteger permitWaitingHighWater = new AtomicInteger();
        private final AtomicInteger permitInFlightHighWater = new AtomicInteger();
        private final Thread sampler;

        private ResourceSampler(
                com.playrewards.flowengine.core.execution.ConcurrencyGovernor governor) {
            this.governor = governor;
            sampler =
                    Thread.ofPlatform()
                            .daemon()
                            .name("task14-resource-sampler")
                            .start(
                                    () -> {
                                        while (running.get()) {
                                            heapHighWater.accumulateAndGet(usedHeap(), Math::max);
                                            threadHighWater.accumulateAndGet(platformThreads(), Math::max);
                                            var permits = governor.snapshot();
                                            permitActiveHighWater.accumulateAndGet(
                                                    permits.activeAcquisitions(), Math::max);
                                            permitWaitingHighWater.accumulateAndGet(
                                                    permits.waitingAcquisitions(), Math::max);
                                            permitInFlightHighWater.accumulateAndGet(
                                                    permits.inFlightAcquisitions(), Math::max);
                                            try {
                                                Thread.sleep(10);
                                            } catch (InterruptedException ignored) {
                                                Thread.currentThread().interrupt();
                                                break;
                                            }
                                        }
                                    });
        }

        private CapacityMetrics metrics(
                String scenario,
                int requested,
                int succeeded,
                int failed,
                long wallMillis,
                List<Long> latencies) {
            return new CapacityMetrics(
                    scenario,
                    requested,
                    succeeded,
                    failed,
                    wallMillis,
                    percentile(latencies, 0.95),
                    percentile(latencies, 0.99),
                    heapBaseline,
                    heapHighWater.get(),
                    Math.max(0, heapHighWater.get() - heapBaseline),
                    threadBaseline,
                    threadHighWater.get(),
                    Math.max(0, threadHighWater.get() - threadBaseline),
                    permitActiveHighWater.get(),
                    permitWaitingHighWater.get(),
                    permitInFlightHighWater.get());
        }

        @Override
        public void close() throws InterruptedException {
            running.set(false);
            sampler.interrupt();
            sampler.join(1000);
        }

        private static long usedHeap() {
            var heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            return heap.getUsed();
        }

        private static int platformThreads() {
            return ManagementFactory.getThreadMXBean().getThreadCount();
        }
    }

    /** Deterministic host fixture for real server capacity tests. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class Host {
        @Bean
        FlowBundleLoader benchmarkBundles(BenchmarkComponent component) {
            return new FlowBundleLoader() {
                @Override
                public String scheme() {
                    return "benchmark";
                }

                @Override
                public ExecutableFlowBundle load(String reference) {
                    return switch (reference) {
                        case "benchmark:light" -> businessBundle("light", "block");
                        case "benchmark:block" -> businessBundle("block", "block");
                        case "benchmark:io" -> businessBundle("io", "io");
                        default -> throw new IllegalArgumentException("unknown benchmark flow");
                    };
                }
            };
        }

        @Bean
        BenchmarkComponent benchmarkComponent() {
            return new BenchmarkComponent();
        }

        @Bean
        ExecutionAccessPolicy benchmarkAccessPolicy() {
            return new ExecutionAccessPolicy() {
                @Override
                public CapturedAuthorization capture(HttpServletRequest request) {
                    String tenant = request.getHeader("X-Benchmark-Tenant");
                    if (!"benchmark".equals(tenant)) throw new SecurityException();
                    return new BenchmarkAuthorization(tenant);
                }

                @Override
                public void authorize(
                        CapturedAuthorization caller, FlowIdentity flow, Action action) {
                    if (!(caller instanceof BenchmarkAuthorization authorization)
                            || !authorization.tenantId().equals(flow.tenantId())
                            || action == null) throw new SecurityException();
                }
            };
        }

        private record BenchmarkAuthorization(String tenantId) implements CapturedAuthorization {}

        private static ExecutableFlowBundle businessBundle(String flowId, String mode) {
            return bundle(
                    flowId,
                    List.of(
                            node("start", "benchmark.start", Map.of()),
                            node("business", "benchmark.business", Map.of("mode", mode)),
                            node("end", "benchmark.end", Map.of("resultStatus", "SUCCESS"))),
                    List.of(edge("start", "business"), edge("business", "end")),
                    Map.of(
                            "benchmark.start", contract("benchmark.start", ComponentType.START, null),
                            "benchmark.business",
                                    contract(
                                            "benchmark.business",
                                            ComponentType.BUSINESS,
                                            "benchmark.component"),
                            "benchmark.end", contract("benchmark.end", ComponentType.END, null)));
        }

        private static ExecutableFlowBundle bundle(
                String flowId,
                List<FlowNode> nodes,
                List<FlowEdge> edges,
                Map<String, CompiledComponentContract> contracts) {
            return new ExecutableFlowBundle(
                            "1.0",
                            new FlowIdentity("benchmark", flowId, "v1"),
                            new FlowDefinition("2.0", nodes, edges),
                            contracts,
                            Map.of(),
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
                String code, ComponentType type, String executorRef) {
            return new CompiledComponentContract(
                    code,
                    type,
                    executorRef,
                    Map.of(),
                    Map.of(),
                    Map.of(),
                    BenchmarkComponent.CAPABILITIES);
        }
    }

    /** Blocking behavior is intentional so a passing run proves virtual-thread overlap. */
    static final class BenchmarkComponent implements FlowBusinessComponent {
        private static final ComponentCapabilities CAPABILITIES =
                new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 512);

        private final AtomicBoolean blocked = new AtomicBoolean();
        private volatile CountDownLatch release = new CountDownLatch(0);
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();
        private volatile URI ioTarget;
        private final HttpClient ioClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        @Override
        public String executorRef() {
            return "benchmark.component";
        }

        @Override
        public ComponentCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public ComponentInvocation execute(ComponentExecutionContext context) {
            int current = active.incrementAndGet();
            maxActive.accumulateAndGet(current, Math::max);
            try {
                String mode = String.valueOf(context.config().requiredJson("mode"));
                if ("block".equals(mode)) {
                    while (blocked.get()) {
                        context.cancellationToken().throwIfCancellationRequested();
                        try {
                            release.await(25, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            context.cancellationToken().throwIfCancellationRequested();
                            throw new IllegalStateException("benchmark component interrupted", failure);
                        }
                    }
                } else if ("io".equals(mode)) {
                    HttpResponse<Void> response = ioClient.send(
                            HttpRequest.newBuilder(ioTarget)
                                    .timeout(Duration.ofSeconds(10))
                                    .GET()
                                    .build(),
                            HttpResponse.BodyHandlers.discarding());
                    if (response.statusCode() != 200) {
                        throw new IllegalStateException("benchmark downstream failed");
                    }
                }
                return new ComponentInvocation(
                        CompletableFuture.completedFuture(new ComponentOutputs(Map.of())),
                        ComponentInvocation.emptyContent());
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("benchmark component interrupted", failure);
            } catch (IOException failure) {
                throw new IllegalStateException("benchmark downstream failed", failure);
            } finally {
                active.decrementAndGet();
            }
        }

        void block() {
            release = new CountDownLatch(1);
            blocked.set(true);
            maxActive.set(0);
        }

        void ioTarget(URI value) {
            blocked.set(false);
            ioTarget = value;
            maxActive.set(0);
        }

        void release() {
            blocked.set(false);
            release.countDown();
        }

        int maxActive() {
            return maxActive.get();
        }
    }
}
