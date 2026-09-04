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
    void uses_separate_trial_policy_and_real_transport_preserves_repeated_response_headers() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HttpTargetPolicy denyProduction = (uri, purpose) -> {
                throw new HttpTargetPolicy.TargetRejectedException("HTTP target rejected");
            };
            HttpTargetPolicy allowTrial = (uri, purpose) -> {
                if (purpose != ExecutionPurpose.TRIAL) {
                    throw new HttpTargetPolicy.TargetRejectedException("HTTP target rejected");
                }
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
    void revalidates_after_connect_and_never_follows_redirects() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger validations = new AtomicInteger();
            HttpTargetPolicy rebinding = (uri, purpose) -> {
                if (validations.incrementAndGet() > 1) {
                    throw new HttpTargetPolicy.TargetRejectedException("rebound");
                }
            };
            try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                    rebinding, rebinding, 1, Duration.ofMillis(5))) {
                CompletionException rejected = assertThrows(CompletionException.class,
                        () -> client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                                1_024, Duration.ofSeconds(1), NEVER_CANCELLED)).toCompletableFuture().join());
                assertEquals(HttpNodeClient.FailureKind.TARGET_REJECTED,
                        ((HttpNodeClient.HttpClientException) rejected.getCause()).kind());
                assertEquals(2, validations.get());
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

            CompletionException timeout = assertThrows(CompletionException.class,
                    () -> client.execute(request(fixture.uri("/slow"), ExecutionPurpose.TRIAL,
                            1_024, Duration.ofMillis(30), NEVER_CANCELLED)).toCompletableFuture().join());
            assertEquals(HttpNodeClient.FailureKind.TIMEOUT_AFTER_SEND,
                    ((HttpNodeClient.HttpClientException) timeout.getCause()).kind());

            long bodyStarted = System.nanoTime();
            CompletionException bodyTimeout = assertThrows(CompletionException.class,
                    () -> client.execute(request(fixture.uri("/slow-body"), ExecutionPurpose.TRIAL,
                            1_024, Duration.ofMillis(30), NEVER_CANCELLED)).toCompletableFuture().join());
            assertEquals(HttpNodeClient.FailureKind.TIMEOUT_AFTER_SEND,
                    ((HttpNodeClient.HttpClientException) bodyTimeout.getCause()).kind());
            assertTrue(Duration.ofNanos(System.nanoTime() - bodyStarted).compareTo(Duration.ofMillis(200)) < 0);

            MutableCancellation cancelled = new MutableCancellation();
            var active = client.execute(request(fixture.uri("/slow"), ExecutionPurpose.TRIAL,
                    1_024, Duration.ofSeconds(2), cancelled)).toCompletableFuture();
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

            long admissionStarted = System.nanoTime();
            CompletionException admissionTimeout = assertThrows(CompletionException.class,
                    () -> client.execute(request(fixture.uri("/echo"), ExecutionPurpose.TRIAL,
                            1_024, Duration.ofMillis(30), NEVER_CANCELLED)).toCompletableFuture().join());
            assertEquals(HttpNodeClient.FailureKind.TIMEOUT_BEFORE_SEND,
                    ((HttpNodeClient.HttpClientException) admissionTimeout.getCause()).kind());
            assertTrue(Duration.ofNanos(System.nanoTime() - admissionStarted)
                    .compareTo(Duration.ofMillis(300)) < 0);

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

    private static final HttpTargetPolicy ALLOW_ALL = (uri, purpose) -> { };
    private static final CancellationToken NEVER_CANCELLED = () -> false;

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
        private final AtomicReference<String> lastMethod = new AtomicReference<>();
        private final AtomicReference<String> lastBody = new AtomicReference<>();
        private final AtomicReference<String> lastHeader = new AtomicReference<>();
        private final AtomicInteger privateRedirectHits = new AtomicInteger();
        private final java.util.concurrent.CountDownLatch holdStarted = new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.CountDownLatch releaseHold = new java.util.concurrent.CountDownLatch(1);

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
                    Thread.sleep(250);
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
                    Thread.sleep(250);
                    exchange.getResponseBody().write("late".getBytes(StandardCharsets.UTF_8));
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
            server.start();
        }

        private URI uri(String path) {
            return URI.create("http://" + server.getAddress().getHostString() + ":"
                    + server.getAddress().getPort() + path);
        }

        @Override
        public void close() {
            releaseHold.countDown();
            server.stop(0);
        }
    }
}
