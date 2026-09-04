package com.playrewards.flowengine.adapter.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Verifies production target rejection and the bounded real JDK transport. */
class HttpTargetPolicyTest {

    @Test
    void rejects_non_http_malformed_and_non_public_targets() throws Exception {
        DefaultHttpTargetPolicy policy = new DefaultHttpTargetPolicy();

        for (String target : List.of(
                "file:///etc/passwd",
                "http://user:pass@example.com/path",
                "http://example.com/path#fragment",
                "http://localhost/path",
                "http://127.0.0.1/path",
                "http://10.0.0.1/path",
                "http://169.254.1.1/path",
                "http://224.0.0.1/path",
                "http://0.0.0.0/path",
                "http://[::1]/path",
                "http://[::ffff:127.0.0.1]/path",
                "http://[2002:7f00:1::]/path",
                "http://[64:ff9b::7f00:1]/path",
                "http://[4000::1]/path",
                "http://192.0.2.1/path")) {
            assertThrows(HttpTargetPolicy.TargetRejectedException.class,
                    () -> policy.validate(URI.create(target), ExecutionPurpose.PRODUCTION), target);
        }
    }

    @Test
    void rejects_mixed_dns_answers_and_rebinding_on_revalidation() throws Exception {
        AtomicBoolean rebound = new AtomicBoolean();
        DefaultHttpTargetPolicy mixed = new DefaultHttpTargetPolicy(host -> List.of(
                InetAddress.getByName("93.184.216.34"), InetAddress.getByName("127.0.0.1")));
        assertThrows(HttpTargetPolicy.TargetRejectedException.class,
                () -> mixed.validate(URI.create("https://public.example/path"), ExecutionPurpose.PRODUCTION));

        DefaultHttpTargetPolicy rebinding = new DefaultHttpTargetPolicy(host -> rebound.get()
                ? List.of(InetAddress.getByName("10.0.0.7"))
                : List.of(InetAddress.getByName("93.184.216.34")));
        URI target = URI.create("https://public.example/path");
        rebinding.validate(target, ExecutionPurpose.PRODUCTION);
        rebound.set(true);
        assertThrows(HttpTargetPolicy.TargetRejectedException.class,
                () -> rebinding.validate(target, ExecutionPurpose.PRODUCTION));
    }

    @Test
    void transport_connects_only_to_the_address_approved_by_the_policy() throws Exception {
        try (PinnedFixture fixture = new PinnedFixture()) {
            AtomicReference<InetAddress> dnsAnswer = new AtomicReference<>(fixture.addressA);
            java.util.concurrent.CountDownLatch resolutionCaptured = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch allowResolution = new java.util.concurrent.CountDownLatch(1);
            AtomicInteger resolutions = new AtomicInteger();
            DefaultHttpTargetPolicy policy = new DefaultHttpTargetPolicy(host -> {
                InetAddress captured = dnsAnswer.get();
                resolutions.incrementAndGet();
                resolutionCaptured.countDown();
                try {
                    allowResolution.await();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new java.net.UnknownHostException("resolution interrupted");
                }
                return List.of(captured);
            }, address -> true);
            try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                    policy, policy, 1, Duration.ofMillis(5))) {
                var response = client.execute(request(
                        fixture.uri(), ExecutionPurpose.TRIAL, 1_024,
                        Duration.ofSeconds(2), NEVER_CANCELLED));
                assertTrue(resolutionCaptured.await(1, TimeUnit.SECONDS));
                dnsAnswer.set(fixture.addressB);
                allowResolution.countDown();

                assertEquals("a", new String(response.toCompletableFuture()
                        .get(2, TimeUnit.SECONDS).body(), StandardCharsets.UTF_8));
                assertEquals(1, fixture.hitsA.get());
                assertEquals(0, fixture.hitsB.get());
                assertEquals(1, resolutions.get());
            } finally {
                allowResolution.countDown();
            }
        }
    }

    @Test
    void dns_resolution_is_cancelled_or_timed_out_without_leaving_an_unbounded_waiter() throws Exception {
        CountDownLatch cancelStarted = new CountDownLatch(1);
        CountDownLatch cancelInterrupted = new CountDownLatch(1);
        DefaultHttpTargetPolicy cancellable = new DefaultHttpTargetPolicy(host -> {
            cancelStarted.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("resolver should have been interrupted");
            } catch (InterruptedException expected) {
                cancelInterrupted.countDown();
                throw new java.net.UnknownHostException("resolution interrupted");
            }
        }, address -> true);
        MutableCancellation cancellation = new MutableCancellation();
        AtomicReference<Throwable> cancelledFailure = new AtomicReference<>();
        Thread caller = Thread.ofVirtual().start(() -> {
            try {
                cancellable.approve(URI.create("https://cancel.example/path"), ExecutionPurpose.TRIAL,
                        cancellation, Instant.now().plusSeconds(3));
            } catch (Throwable failure) {
                cancelledFailure.set(failure);
            }
        });
        assertTrue(cancelStarted.await(1, TimeUnit.SECONDS));
        cancellation.cancel();
        caller.join(2_000);
        assertFalse(caller.isAlive());
        assertTrue(cancelledFailure.get() instanceof java.util.concurrent.CancellationException);
        assertTrue(cancelInterrupted.await(1, TimeUnit.SECONDS));

        CountDownLatch deadlineStarted = new CountDownLatch(1);
        CountDownLatch deadlineInterrupted = new CountDownLatch(1);
        DefaultHttpTargetPolicy expiring = new DefaultHttpTargetPolicy(host -> {
            deadlineStarted.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("resolver should have been interrupted");
            } catch (InterruptedException expected) {
                deadlineInterrupted.countDown();
                throw new java.net.UnknownHostException("resolution interrupted");
            }
        }, address -> true);
        AtomicReference<Throwable> deadlineFailure = new AtomicReference<>();
        Thread deadlineCaller = Thread.ofVirtual().start(() -> {
            try {
                expiring.approve(URI.create("https://deadline.example/path"), ExecutionPurpose.TRIAL,
                        NEVER_CANCELLED, Instant.now().plusMillis(100));
            } catch (Throwable failure) {
                deadlineFailure.set(failure);
            }
        });
        assertTrue(deadlineStarted.await(1, TimeUnit.SECONDS));
        deadlineCaller.join(2_000);
        assertFalse(deadlineCaller.isAlive());
        assertEquals(HttpTargetPolicy.TargetFailureKind.TIMEOUT,
                ((HttpTargetPolicy.TargetRejectedException) deadlineFailure.get()).kind());
        assertTrue(deadlineInterrupted.await(1, TimeUnit.SECONDS));
    }

    @Test
    void dns_resolution_capacity_rejects_overflow_instead_of_queueing_unbounded_work() throws Exception {
        CountDownLatch started = new CountDownLatch(64);
        CountDownLatch release = new CountDownLatch(1);
        DefaultHttpTargetPolicy saturated = new DefaultHttpTargetPolicy(host -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new java.net.UnknownHostException("resolution interrupted");
            }
            return List.of(InetAddress.getLoopbackAddress());
        }, address -> true);
        var callers = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> active = new java.util.ArrayList<>();
        try {
            for (int index = 0; index < 64; index++) {
                active.add(callers.submit(() -> saturated.approve(
                        URI.create("https://capacity.example/path"), ExecutionPurpose.TRIAL,
                        NEVER_CANCELLED, Instant.now().plusSeconds(5))));
            }
            assertTrue(started.await(2, TimeUnit.SECONDS));

            HttpTargetPolicy.TargetRejectedException overflow = assertThrows(
                    HttpTargetPolicy.TargetRejectedException.class,
                    () -> saturated.approve(URI.create("https://overflow.example/path"),
                            ExecutionPurpose.TRIAL, NEVER_CANCELLED, Instant.now().plusMillis(100)));
            assertEquals(HttpTargetPolicy.TargetFailureKind.CAPACITY, overflow.kind());
        } finally {
            release.countDown();
            for (Future<?> future : active) {
                future.get(2, TimeUnit.SECONDS);
            }
            callers.close();
        }
    }

    @Test
    void uses_separate_trial_policy_and_real_transport_preserves_repeated_response_headers() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HttpTargetPolicy denyProduction = (uri, purpose, cancellation, deadline) -> {
                throw new HttpTargetPolicy.TargetRejectedException("HTTP target rejected");
            };
            HttpTargetPolicy allowTrial = (uri, purpose, cancellation, deadline) -> {
                if (purpose != ExecutionPurpose.TRIAL) {
                    throw new HttpTargetPolicy.TargetRejectedException("HTTP target rejected");
                }
                return approveLocal(uri);
            };
            try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                    denyProduction, allowTrial, 2, Duration.ofMillis(5))) {
                HttpNodeClient.HttpResponse response = client.execute(request(
                        fixture.uri("/echo"), ExecutionPurpose.TRIAL, 1_024, Duration.ofSeconds(1), NEVER_CANCELLED))
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);

                assertEquals(200, response.statusCode());
                assertEquals("ok", new String(response.body(), StandardCharsets.UTF_8));
                assertEquals(List.of("one", "two"), response.headers().entrySet().stream()
                        .filter(entry -> entry.getKey().equalsIgnoreCase("X-Repeat"))
                        .findFirst().orElseThrow().getValue());
                CompletionException rejected = assertThrows(CompletionException.class,
                        () -> client.execute(request(fixture.uri("/echo"), ExecutionPurpose.PRODUCTION,
                                1_024, Duration.ofSeconds(1), NEVER_CANCELLED)).toCompletableFuture().join());
                assertEquals(HttpNodeClient.FailureKind.TARGET_REJECTED,
                        ((HttpNodeClient.HttpClientException) rejected.getCause()).kind());
            }
        }
    }

    @Test
    void real_transport_sends_all_five_methods_and_exact_json_body() throws Exception {
        try (Fixture fixture = new Fixture();
                JdkHttpNodeClient client = new JdkHttpNodeClient(ALLOW_ALL, ALLOW_ALL, 2,
                        Duration.ofMillis(5))) {
            for (String method : List.of("GET", "DELETE", "POST", "PUT", "PATCH")) {
                byte[] body = method.equals("GET") || method.equals("DELETE")
                        ? new byte[0] : "{\"value\":1}".getBytes(StandardCharsets.UTF_8);
                HttpNodeClient.HttpRequest request = new HttpNodeClient.HttpRequest(method,
                        fixture.uri("/capture"), Map.of("X-Test", "safe"), body,
                        Duration.ofSeconds(1), Instant.now().plusSeconds(2), 1_024,
                        ExecutionPurpose.TRIAL, NEVER_CANCELLED);

                HttpNodeClient.HttpResponse response = client.execute(request)
                        .toCompletableFuture().get(2, TimeUnit.SECONDS);

                assertEquals(200, response.statusCode());
                assertEquals(method, fixture.lastMethod.get());
                assertEquals(new String(body, StandardCharsets.UTF_8), fixture.lastBody.get());
                assertEquals("safe", fixture.lastHeader.get());
            }
        }
    }

    @Test
    void approves_once_and_never_follows_redirects() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger approvals = new AtomicInteger();
            HttpTargetPolicy oneApproval = (uri, purpose, cancellation, deadline) -> {
                approvals.incrementAndGet();
                return approveLocal(uri);
            };
            try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                    oneApproval, oneApproval, 1, Duration.ofMillis(5))) {
                assertEquals(200, client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                        1_024, Duration.ofSeconds(1), NEVER_CANCELLED))
                        .toCompletableFuture().join().statusCode());
                assertEquals(1, approvals.get());
            }

            try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                    ALLOW_ALL, ALLOW_ALL, 1, Duration.ofMillis(5))) {
                CompletionException redirect = assertThrows(CompletionException.class,
                        () -> client.execute(request(fixture.uri("/redirect"), ExecutionPurpose.TRIAL,
                                1_024, Duration.ofSeconds(1), NEVER_CANCELLED)).toCompletableFuture().join());
                assertEquals(HttpNodeClient.FailureKind.TARGET_REJECTED,
                        ((HttpNodeClient.HttpClientException) redirect.getCause()).kind());
                assertEquals(0, fixture.privateRedirectHits.get());
            }
        }
    }

    @Test
    void enforces_response_limit_timeout_cancellation_and_bounded_admission() throws Exception {
        try (Fixture fixture = new Fixture();
                JdkHttpNodeClient client = new JdkHttpNodeClient(ALLOW_ALL, ALLOW_ALL, 1,
                        Duration.ofMillis(5))) {
            CompletionException oversized = assertThrows(CompletionException.class,
                    () -> client.execute(request(fixture.uri("/large"), ExecutionPurpose.TRIAL,
                            8, Duration.ofSeconds(1), NEVER_CANCELLED)).toCompletableFuture().join());
            assertEquals(HttpNodeClient.FailureKind.RESPONSE_TOO_LARGE,
                    ((HttpNodeClient.HttpClientException) oversized.getCause()).kind());

            java.util.concurrent.ExecutionException timeout = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> client.execute(request(fixture.uri("/slow"), ExecutionPurpose.TRIAL,
                            1_024, Duration.ofMillis(100), NEVER_CANCELLED)).toCompletableFuture()
                            .get(2, TimeUnit.SECONDS));
            assertEquals(HttpNodeClient.FailureKind.TIMEOUT_AFTER_SEND,
                    ((HttpNodeClient.HttpClientException) timeout.getCause()).kind());

            var bodySource = client.execute(request(fixture.uri("/slow-body"), ExecutionPurpose.TRIAL,
                    1_024, Duration.ofMillis(100), NEVER_CANCELLED)).toCompletableFuture();
            assertTrue(fixture.slowBodyStarted.await(1, TimeUnit.SECONDS));
            java.util.concurrent.ExecutionException bodyTimeout = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> bodySource.get(2, TimeUnit.SECONDS));
            assertEquals(HttpNodeClient.FailureKind.TIMEOUT_AFTER_SEND,
                    ((HttpNodeClient.HttpClientException) bodyTimeout.getCause()).kind());

            MutableCancellation cancelled = new MutableCancellation();
            var active = client.execute(request(fixture.uri("/cancel"), ExecutionPurpose.TRIAL,
                    1_024, Duration.ofSeconds(2), cancelled)).toCompletableFuture();
            assertTrue(fixture.cancelStarted.await(1, TimeUnit.SECONDS));
            cancelled.cancel();
            assertThrows(java.util.concurrent.CancellationException.class,
                    () -> active.get(1, TimeUnit.SECONDS));

            var holding = client.execute(request(fixture.uri("/hold"), ExecutionPurpose.TRIAL,
                    1_024, Duration.ofSeconds(2), NEVER_CANCELLED));
            assertTrue(fixture.holdStarted.await(1, TimeUnit.SECONDS));
            MutableCancellation waitingCancellation = new MutableCancellation();
            var waiting = client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                    1_024, Duration.ofSeconds(2), waitingCancellation)).toCompletableFuture();
            waitingCancellation.cancel();
            assertThrows(java.util.concurrent.CancellationException.class,
                    () -> waiting.get(1, TimeUnit.SECONDS));

            java.util.concurrent.ExecutionException admissionTimeout = assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                            1_024, Duration.ofMillis(100), NEVER_CANCELLED)).toCompletableFuture()
                            .get(2, TimeUnit.SECONDS));
            assertEquals(HttpNodeClient.FailureKind.TIMEOUT_BEFORE_SEND,
                    ((HttpNodeClient.HttpClientException) admissionTimeout.getCause()).kind());

            MutableCancellation queuedCancellation = new MutableCancellation();
            var queued = client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                    1_024, Duration.ofSeconds(2), queuedCancellation)).toCompletableFuture();
            CompletionException overflow = assertThrows(CompletionException.class,
                    () -> client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                            1_024, Duration.ofSeconds(2), NEVER_CANCELLED)).toCompletableFuture().join());
            assertEquals(HttpNodeClient.FailureKind.ADMISSION_REJECTED,
                    ((HttpNodeClient.HttpClientException) overflow.getCause()).kind());
            queuedCancellation.cancel();
            assertThrows(java.util.concurrent.CancellationException.class,
                    () -> queued.get(1, TimeUnit.SECONDS));
            fixture.releaseHold.countDown();
            assertEquals(200, holding.toCompletableFuture().get(1, TimeUnit.SECONDS).statusCode());
        }
    }

    @Test
    void request_and_transport_failures_redact_query_headers_and_body() {
        String secret = "very-secret-value";
        HttpNodeClient.HttpRequest request = new HttpNodeClient.HttpRequest("POST",
                URI.create("https://example.com/path?token=" + secret),
                Map.of("Authorization", secret), secret.getBytes(StandardCharsets.UTF_8),
                Duration.ofSeconds(1), Instant.now().plusSeconds(2), 1_024,
                ExecutionPurpose.PRODUCTION, NEVER_CANCELLED);
        HttpNodeClient.HttpClientException failure = new HttpNodeClient.HttpClientException(
                HttpNodeClient.FailureKind.CONNECTION_FAILURE_BEFORE_SEND, "HTTP connection failed");

        assertFalse(request.toString().contains(secret));
        assertFalse(failure.toString().contains(secret));
    }

    private static HttpNodeClient.HttpRequest request(URI uri, ExecutionPurpose purpose,
            int maxBytes, Duration timeout, CancellationToken cancellation) {
        return new HttpNodeClient.HttpRequest("GET", uri, Map.of(), new byte[0], timeout,
                Instant.now().plusSeconds(3), maxBytes, purpose, cancellation);
    }

    private static final HttpTargetPolicy ALLOW_ALL =
            (uri, purpose, cancellation, deadline) -> approveLocal(uri);
    private static final CancellationToken NEVER_CANCELLED = () -> false;

    private static HttpTargetPolicy.ApprovedTarget approveLocal(URI uri) {
        try {
            int port = uri.getPort() > 0 ? uri.getPort()
                    : uri.getScheme().equalsIgnoreCase("https") ? 443 : 80;
            return new HttpTargetPolicy.ApprovedTarget(
                    uri, List.of(InetAddress.getByName(uri.getHost())), port);
        } catch (java.net.UnknownHostException failure) {
            throw new HttpTargetPolicy.TargetRejectedException("HTTP target rejected", failure);
        }
    }

    private static final class MutableCancellation implements CancellationToken {

        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile Runnable listener = () -> { };

        @Override
        public boolean isCancellationRequested() {
            return cancelled.get();
        }

        @Override
        public CancellationRegistration onCancellation(Runnable action) {
            listener = action;
            if (cancelled.get()) {
                action.run();
            }
            return () -> listener = () -> { };
        }

        private void cancel() {
            cancelled.set(true);
            listener.run();
        }
    }

    private static final class Fixture implements AutoCloseable {

        private final HttpServer server;
        private final java.util.concurrent.ExecutorService serverTasks =
                java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        private final AtomicReference<String> lastMethod = new AtomicReference<>();
        private final AtomicReference<String> lastBody = new AtomicReference<>();
        private final AtomicReference<String> lastHeader = new AtomicReference<>();
        private final AtomicInteger privateRedirectHits = new AtomicInteger();
        private final java.util.concurrent.CountDownLatch holdStarted = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch releaseHold = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch slowBodyStarted = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch releaseSlowBody = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch cancelStarted = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch releaseCancel = new java.util.concurrent.CountDownLatch(1);

        private Fixture() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/echo", exchange -> {
                exchange.getResponseHeaders().add("X-Repeat", "one");
                exchange.getResponseHeaders().add("X-Repeat", "two");
                byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.createContext("/large", exchange -> {
                byte[] body = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.createContext("/capture", exchange -> {
                lastMethod.set(exchange.getRequestMethod());
                lastBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                lastHeader.set(exchange.getRequestHeaders().getFirst("X-Test"));
                byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.createContext("/redirect", exchange -> {
                exchange.getResponseHeaders().add("Location", uri("/private").toString());
                exchange.sendResponseHeaders(302, -1);
                exchange.close();
            });
            server.createContext("/private", exchange -> {
                privateRedirectHits.incrementAndGet();
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            });
            server.createContext("/slow", exchange -> {
                try {
                    releaseSlowBody.await();
                    byte[] body = "late".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.createContext("/slow-body", exchange -> {
                try {
                    exchange.sendResponseHeaders(200, 4);
                    exchange.getResponseBody().flush();
                    slowBodyStarted.countDown();
                    releaseSlowBody.await();
                    exchange.getResponseBody().write("late".getBytes(StandardCharsets.UTF_8));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.createContext("/cancel", exchange -> {
                try {
                    cancelStarted.countDown();
                    releaseCancel.await();
                    exchange.sendResponseHeaders(204, -1);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.createContext("/hold", exchange -> {
                try {
                    holdStarted.countDown();
                    releaseHold.await();
                    byte[] body = "done".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, body.length);
                    exchange.getResponseBody().write(body);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.setExecutor(serverTasks);
            server.start();
        }

        private URI uri(String path) {
            return URI.create("http://" + server.getAddress().getHostString() + ":"
                    + server.getAddress().getPort() + path);
        }

        @Override
        public void close() {
            releaseHold.countDown();
            releaseSlowBody.countDown();
            releaseCancel.countDown();
            server.stop(0);
            serverTasks.shutdownNow();
        }
    }

    private static final class PinnedFixture implements AutoCloseable {

        private final InetAddress addressA = InetAddress.getByName("127.0.0.1");
        private final InetAddress addressB = InetAddress.getByName("::1");
        private final HttpServer serverA;
        private final HttpServer serverB;
        private final AtomicInteger hitsA = new AtomicInteger();
        private final AtomicInteger hitsB = new AtomicInteger();

        private PinnedFixture() throws IOException {
            serverA = HttpServer.create(new InetSocketAddress(addressA, 0), 0);
            int port = serverA.getAddress().getPort();
            serverB = HttpServer.create(new InetSocketAddress(addressB, port), 0);
            serverA.createContext("/pinned", exchange -> respond(exchange, hitsA, "a"));
            serverB.createContext("/pinned", exchange -> respond(exchange, hitsB, "b"));
            serverA.start();
            serverB.start();
        }

        private URI uri() {
            return URI.create("http://rebind.test:" + serverA.getAddress().getPort() + "/pinned");
        }

        private static void respond(com.sun.net.httpserver.HttpExchange exchange,
                AtomicInteger hits, String value) throws IOException {
            hits.incrementAndGet();
            byte[] body = value.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        }

        @Override
        public void close() {
            serverA.stop(0);
            serverB.stop(0);
        }
    }
}
