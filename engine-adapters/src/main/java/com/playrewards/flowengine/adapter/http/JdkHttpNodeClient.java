package com.playrewards.flowengine.adapter.http;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDK HTTP adapter with redirect denial, finite admission, cancellation and streaming byte limits.
 *
 * <p>At most {@code maxConcurrentRequests} transports and the same number of admission waiters can
 * exist. Responses are read incrementally and closed on every path before the SPI completes.</p>
 */
public final class JdkHttpNodeClient implements HttpNodeClient, AutoCloseable {

    private final HttpTargetPolicy productionPolicy;
    private final HttpTargetPolicy trialPolicy;
    private final Semaphore transportAdmission;
    private final Semaphore waiterAdmission;
    private final Duration admissionPoll;
    private final ExecutorService tasks;
    private final HttpClient client;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<TransportFuture, Future<?>> sources = new ConcurrentHashMap<>();

    /** Creates the default public-target client with 64 active requests and 64 bounded waiters. */
    public JdkHttpNodeClient() {
        this(new DefaultHttpTargetPolicy(), new DefaultHttpTargetPolicy(), 64, Duration.ofMillis(10));
    }

    /** Creates a transport with separate production/trial policy entry points and finite admission. */
    public JdkHttpNodeClient(HttpTargetPolicy productionPolicy, HttpTargetPolicy trialPolicy,
            int maxConcurrentRequests, Duration admissionPoll) {
        this.productionPolicy = Objects.requireNonNull(productionPolicy, "productionPolicy must not be null");
        this.trialPolicy = Objects.requireNonNull(trialPolicy, "trialPolicy must not be null");
        if (maxConcurrentRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests must be positive");
        }
        this.transportAdmission = new Semaphore(maxConcurrentRequests, true);
        this.waiterAdmission = new Semaphore(maxConcurrentRequests, true);
        this.admissionPoll = positive(admissionPoll, "admissionPoll");
        this.tasks = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("flow-http-", 0).factory());
        this.client = HttpClient.newBuilder()
                .executor(tasks)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** Starts one bounded task; capacity overflow is rejected before entering the executor queue. */
    @Override
    public CompletableFuture<HttpResponse> execute(HttpNodeClient.HttpRequest request) {
        Objects.requireNonNull(request, "HTTP request must not be null");
        TransportFuture result = new TransportFuture();
        if (closed.get() || !waiterAdmission.tryAcquire()) {
            result.completeExceptionally(new HttpClientException(
                    FailureKind.ADMISSION_REJECTED, "HTTP capacity unavailable"));
            return result;
        }
        try {
            Future<?> source = tasks.submit(() -> run(request, result));
            sources.put(result, source);
            result.whenComplete((ignored, failure) -> sources.remove(result));
        } catch (RuntimeException failure) {
            waiterAdmission.release();
            result.completeExceptionally(new HttpClientException(
                    FailureKind.ADMISSION_REJECTED, "HTTP capacity unavailable", failure));
        }
        return result;
    }

    private void run(HttpNodeClient.HttpRequest request, TransportFuture result) {
        boolean waiterHeld = true;
        boolean transportHeld = false;
        AtomicBoolean sent = new AtomicBoolean();
        CompletableFuture<?>[] active = new CompletableFuture<?>[1];
        Thread worker = Thread.currentThread();
        result.worker(worker);
        HttpResponse successful = null;
        Throwable selectedFailure = null;
        boolean cancelled = false;
        Instant operationDeadline = operationDeadline(request);
        try (CancellationToken.CancellationRegistration ignored = request.cancellationToken()
                .onCancellation(() -> result.requestCancellation(active[0], worker))) {
            if (result.cancellationRequested()) {
                throw new CancellationException("HTTP request was cancelled");
            }
            request.cancellationToken().throwIfCancellationRequested();
            policy(request.purpose()).validate(request.uri(), request.purpose());
            while (true) {
                request.cancellationToken().throwIfCancellationRequested();
                long admissionWait = remainingNanos(operationDeadline);
                if (admissionWait <= 0) {
                    throw new HttpClientException(FailureKind.TIMEOUT_BEFORE_SEND,
                            "HTTP request timed out before send");
                }
                if (transportAdmission.tryAcquire(Math.min(admissionPoll.toNanos(), admissionWait),
                        TimeUnit.NANOSECONDS)) {
                    break;
                }
            }
            transportHeld = true;
            waiterAdmission.release();
            waiterHeld = false;
            request.cancellationToken().throwIfCancellationRequested();
            long beforeSend = remainingNanos(operationDeadline);
            if (beforeSend <= 0) {
                throw new HttpClientException(FailureKind.TIMEOUT_BEFORE_SEND,
                        "HTTP request timed out before send");
            }
            java.net.http.HttpRequest transportRequest = transportRequest(
                    request, Duration.ofNanos(beforeSend));
            CompletableFuture<java.net.http.HttpResponse<InputStream>> responseFuture =
                    client.sendAsync(transportRequest, BodyHandlers.ofInputStream());
            active[0] = responseFuture;
            sent.set(true);
            request.sendObserver().sent();
            java.net.http.HttpResponse<InputStream> response;
            try {
                long waitNanos = remainingNanos(operationDeadline);
                if (waitNanos <= 0) {
                    throw new HttpClientException(FailureKind.TIMEOUT_AFTER_SEND,
                            "HTTP request timed out after send");
                }
                response = responseFuture.get(waitNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException failure) {
                responseFuture.cancel(true);
                throw new HttpClientException(FailureKind.TIMEOUT_AFTER_SEND,
                        "HTTP request timed out after send", failure);
            } catch (ExecutionException failure) {
                throw transportFailure(unwrap(failure.getCause()), sent.get());
            }
            try (InputStream body = response.body()) {
                result.body(body);
                policy(request.purpose()).validate(response.uri(), request.purpose());
                rejectRedirect(response.statusCode());
                rejectEncoded(response.headers().map());
                byte[] bytes = readBounded(body, request.maxResponseBytes(), response.headers().map());
                request.sendObserver().resolved();
                successful = new HttpResponse(response.statusCode(), response.headers().map(), bytes);
            } finally {
                result.body(null);
            }
        } catch (CancellationException failure) {
            cancelled = true;
        } catch (HttpTargetPolicy.TargetRejectedException failure) {
            selectedFailure = new HttpClientException(
                    FailureKind.TARGET_REJECTED, "HTTP target rejected", failure);
        } catch (HttpClientException failure) {
            selectedFailure = failure;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            if (request.cancellationToken().isCancellationRequested() || result.cancellationRequested()) {
                cancelled = true;
            } else {
                selectedFailure = new HttpClientException(
                        sent.get() ? FailureKind.CONNECTION_LOST_AFTER_SEND
                                : FailureKind.CONNECTION_FAILURE_BEFORE_SEND,
                        "HTTP transport interrupted", failure);
            }
        } catch (IOException failure) {
            if (request.cancellationToken().isCancellationRequested() || result.cancellationRequested()) {
                cancelled = true;
            } else {
                selectedFailure = new HttpClientException(
                        FailureKind.CONNECTION_LOST_AFTER_SEND, "HTTP response read failed", failure);
            }
        } catch (RuntimeException failure) {
            selectedFailure = new HttpClientException(
                    sent.get() ? FailureKind.INVALID_RESPONSE : FailureKind.CONNECTION_FAILURE_BEFORE_SEND,
                    "HTTP transport failed", failure);
        } finally {
            CompletableFuture<?> future = active[0];
            if (result.cancellationRequested() && future != null) {
                future.cancel(true);
            }
            if (transportHeld) {
                transportAdmission.release();
            }
            if (waiterHeld) {
                waiterAdmission.release();
            }
        }
        if (cancelled || result.cancellationRequested()) {
            result.completeCancellation();
        } else if (selectedFailure != null) {
            result.completeExceptionally(selectedFailure);
        } else {
            result.complete(Objects.requireNonNull(successful, "HTTP response must not be null"));
        }
    }

    private static java.net.http.HttpRequest transportRequest(
            HttpNodeClient.HttpRequest request, Duration remainingTimeout) {
        java.net.http.HttpRequest.Builder builder = java.net.http.HttpRequest.newBuilder(request.uri())
                .timeout(remainingTimeout)
                .header("Accept-Encoding", "identity");
        request.headers().forEach(builder::header);
        byte[] body = request.body();
        return builder.method(request.method(), body.length == 0
                ? java.net.http.HttpRequest.BodyPublishers.noBody()
                : java.net.http.HttpRequest.BodyPublishers.ofByteArray(body)).build();
    }

    private HttpTargetPolicy policy(ExecutionPurpose purpose) {
        return purpose == ExecutionPurpose.TRIAL ? trialPolicy : productionPolicy;
    }

    private static void rejectRedirect(int status) {
        if (status >= 300 && status <= 399) {
            throw new HttpClientException(FailureKind.TARGET_REJECTED,
                    "HTTP redirect is disabled");
        }
    }

    private static void rejectEncoded(Map<String, List<String>> headers) {
        headers.forEach((name, values) -> {
            if (name.equalsIgnoreCase("Content-Encoding")
                    && values.stream().anyMatch(value -> !value.equalsIgnoreCase("identity"))) {
                throw new HttpClientException(FailureKind.RESPONSE_TOO_LARGE,
                        "encoded HTTP responses are disabled");
            }
        });
    }

    private static byte[] readBounded(InputStream stream, int limit,
            Map<String, List<String>> headers) throws IOException {
        declaredLength(headers).ifPresent(length -> {
            if (length > limit) {
                throw new HttpClientException(FailureKind.RESPONSE_TOO_LARGE,
                        "HTTP response exceeded the byte limit");
            }
        });
        ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(limit, 8_192));
        byte[] buffer = new byte[(int) Math.min((long) limit + 1L, 8_192L)];
        long total = 0;
        int read;
        while ((read = stream.read(buffer)) != -1) {
            total += read;
            if (total > limit) {
                throw new HttpClientException(FailureKind.RESPONSE_TOO_LARGE,
                        "HTTP response exceeded the byte limit");
            }
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static java.util.OptionalLong declaredLength(Map<String, List<String>> headers) {
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            if (entry.getKey().equalsIgnoreCase("Content-Length") && !entry.getValue().isEmpty()) {
                try {
                    return java.util.OptionalLong.of(Long.parseLong(entry.getValue().getFirst()));
                } catch (NumberFormatException failure) {
                    return java.util.OptionalLong.empty();
                }
            }
        }
        return java.util.OptionalLong.empty();
    }

    private static Instant operationDeadline(HttpNodeClient.HttpRequest request) {
        Instant timeoutDeadline;
        try {
            timeoutDeadline = Instant.now().plus(request.timeout());
        } catch (ArithmeticException | java.time.DateTimeException failure) {
            timeoutDeadline = Instant.MAX;
        }
        return timeoutDeadline.isBefore(request.deadline()) ? timeoutDeadline : request.deadline();
    }

    private static long remainingNanos(Instant deadline) {
        try {
            return Math.max(0, Duration.between(Instant.now(), deadline).toNanos());
        } catch (ArithmeticException failure) {
            return deadline.isAfter(Instant.now()) ? Long.MAX_VALUE : 0;
        }
    }

    private static HttpClientException transportFailure(Throwable failure, boolean sent) {
        if (failure instanceof HttpTimeoutException) {
            return new HttpClientException(sent ? FailureKind.TIMEOUT_AFTER_SEND
                    : FailureKind.TIMEOUT_BEFORE_SEND, "HTTP request timed out", failure);
        }
        if (failure instanceof ConnectException) {
            return new HttpClientException(sent ? FailureKind.CONNECTION_LOST_AFTER_SEND
                    : FailureKind.CONNECTION_FAILURE_BEFORE_SEND, "HTTP connection failed", failure);
        }
        return new HttpClientException(sent ? FailureKind.CONNECTION_LOST_AFTER_SEND
                : FailureKind.CONNECTION_FAILURE_BEFORE_SEND, "HTTP transport failed", failure);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof java.util.concurrent.CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    /** Cancels in-flight sources, releases waiters through task cleanup and owns no shared executor. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        sources.forEach((result, source) -> {
            result.requestCancellation(null, null);
        });
        tasks.shutdownNow();
    }

    /** Defers public cancellation completion until the worker has released response/admission resources. */
    private static final class TransportFuture extends CompletableFuture<HttpResponse> {

        private final AtomicBoolean cancellationRequested = new AtomicBoolean();
        private final AtomicReference<Thread> worker = new AtomicReference<>();
        private final AtomicReference<InputStream> body = new AtomicReference<>();

        private void worker(Thread taskWorker) {
            worker.set(taskWorker);
            if (cancellationRequested.get()) {
                taskWorker.interrupt();
            }
        }

        private void body(InputStream responseBody) {
            body.set(responseBody);
            if (responseBody != null && cancellationRequested.get()) {
                try {
                    responseBody.close();
                } catch (IOException ignored) {
                    // The worker owns the definitive cleanup outcome.
                }
            }
        }

        private void requestCancellation(CompletableFuture<?> active, Thread worker) {
            cancellationRequested.set(true);
            if (active != null) {
                active.cancel(true);
            }
            InputStream responseBody = body.get();
            if (responseBody != null) {
                try {
                    responseBody.close();
                } catch (IOException ignored) {
                    // Closing an already-failed body is best-effort and remains redaction-safe.
                }
            }
            Thread runningWorker = worker != null ? worker : this.worker.get();
            if (runningWorker != null) {
                runningWorker.interrupt();
            }
        }

        private boolean cancellationRequested() {
            return cancellationRequested.get();
        }

        private void completeCancellation() {
            super.cancel(true);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (isDone()) {
                return false;
            }
            requestCancellation(null, null);
            return true;
        }
    }
}
