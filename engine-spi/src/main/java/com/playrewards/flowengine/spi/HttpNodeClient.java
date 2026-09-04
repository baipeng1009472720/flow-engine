package com.playrewards.flowengine.spi;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.json.JsonValues;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Adapter-neutral bounded HTTP transport used by the HTTP node executor.
 *
 * <p>The SPI carries copied bytes and headers only. Streams, JDK clients, buffers, resolved socket
 * addresses and adapter exceptions never cross into Core.</p>
 */
@FunctionalInterface
public interface HttpNodeClient {

    /** Starts one request whose completion owns all source-response cleanup. */
    CompletionStage<HttpResponse> execute(HttpRequest request);

    /** Immutable redaction-safe request passed from Core to one transport adapter. */
    record HttpRequest(String method, URI uri, Map<String, String> headers, byte[] body,
                       Duration timeout, Instant deadline, int maxResponseBytes,
                       ExecutionPurpose purpose, CancellationToken cancellationToken,
                       SendObserver sendObserver) {

        public HttpRequest {
            method = JsonValues.requireText(method, "HTTP method");
            Objects.requireNonNull(uri, "HTTP URI must not be null");
            headers = copyRequestHeaders(headers);
            body = Objects.requireNonNull(body, "HTTP body must not be null").clone();
            Objects.requireNonNull(timeout, "HTTP timeout must not be null");
            if (timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("HTTP timeout must be positive");
            }
            Objects.requireNonNull(deadline, "HTTP deadline must not be null");
            if (maxResponseBytes < 1) {
                throw new IllegalArgumentException("HTTP maxResponseBytes must be positive");
            }
            Objects.requireNonNull(purpose, "HTTP purpose must not be null");
            Objects.requireNonNull(cancellationToken, "HTTP cancellationToken must not be null");
            Objects.requireNonNull(sendObserver, "HTTP sendObserver must not be null");
        }

        /** Compatibility constructor for callers which do not track unprotected side effects. */
        public HttpRequest(String method, URI uri, Map<String, String> headers, byte[] body,
                Duration timeout, Instant deadline, int maxResponseBytes,
                ExecutionPurpose purpose, CancellationToken cancellationToken) {
            this(method, uri, headers, body, timeout, deadline, maxResponseBytes, purpose,
                    cancellationToken, SendObserver.NOOP);
        }

        @Override
        public byte[] body() {
            return body.clone();
        }

        @Override
        public String toString() {
            return "HttpRequest[method=" + method + ", target=<redacted>, headerNames="
                    + headers.keySet() + ", body=<redacted>, timeout=" + timeout
                    + ", deadline=" + deadline + ", maxResponseBytes=" + maxResponseBytes
                    + ", purpose=" + purpose
                    + ", cancellationToken=<redacted>, sendObserver=<redacted>]";
        }

        private static Map<String, String> copyRequestHeaders(Map<String, String> source) {
            Objects.requireNonNull(source, "HTTP headers must not be null");
            Map<String, String> copy = new LinkedHashMap<>();
            source.forEach((key, value) -> copy.put(
                    JsonValues.requireText(key, "HTTP header name"),
                    Objects.requireNonNull(value, "HTTP header value must not be null")));
            return Map.copyOf(copy);
        }
    }

    /**
     * One-way lifecycle signal for an unprotected side effect.
     *
     * <p>Adapters call {@link #sent()} only after a transport source exists, and call
     * {@link #resolved()} after a definitive response or deterministic pre-send outcome. An
     * unresolved sent marker lets the scheduler arbitrate an execution deadline safely.</p>
     */
    interface SendObserver {

        SendObserver NOOP = new SendObserver() {
            @Override
            public void sent() {
            }

            @Override
            public void resolved() {
            }

            @Override
            public boolean isSentUnresolved() {
                return false;
            }
        };

        /** Marks that the external peer may observe the request. */
        void sent();

        /** Clears the marker after a definitive outcome. */
        void resolved();

        /**
         * @return whether the peer may have observed the request and no definitive outcome exists
         */
        boolean isSentUnresolved();
    }

    /** Fully buffered bounded response; repeated response headers retain every value. */
    record HttpResponse(int statusCode, Map<String, List<String>> headers, byte[] body) {

        public HttpResponse {
            if (statusCode < 100 || statusCode > 599) {
                throw new IllegalArgumentException("HTTP status code is invalid");
            }
            headers = copyResponseHeaders(headers);
            body = Objects.requireNonNull(body, "HTTP response body must not be null").clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }

        @Override
        public String toString() {
            return "HttpResponse[statusCode=" + statusCode + ", headerNames=" + headers.keySet()
                    + ", body=<redacted>, bodyBytes=" + body.length + "]";
        }

        private static Map<String, List<String>> copyResponseHeaders(Map<String, List<String>> source) {
            Objects.requireNonNull(source, "HTTP response headers must not be null");
            Map<String, List<String>> copy = new LinkedHashMap<>();
            source.forEach((key, values) -> {
                List<String> copiedValues = new ArrayList<>();
                Objects.requireNonNull(values, "HTTP response header values must not be null")
                        .forEach(value -> copiedValues.add(Objects.requireNonNull(
                                value, "HTTP response header value must not be null")));
                copy.put(Objects.requireNonNull(key, "HTTP response header name must not be null"),
                        List.copyOf(copiedValues));
            });
            return Map.copyOf(copy);
        }
    }

    /** Stable transport failure classification used by retry and manual-recovery decisions. */
    enum FailureKind {
        TARGET_REJECTED,
        ADMISSION_REJECTED,
        TIMEOUT_BEFORE_SEND,
        CONNECTION_FAILURE_BEFORE_SEND,
        TIMEOUT_AFTER_SEND,
        CONNECTION_LOST_AFTER_SEND,
        RESPONSE_TOO_LARGE,
        INVALID_RESPONSE
    }

    /** Adapter failure containing only a stable kind and a fixed public message. */
    final class HttpClientException extends RuntimeException {

        private final FailureKind kind;

        /** Creates a safe classified transport failure. */
        public HttpClientException(FailureKind kind, String ignoredMessage) {
            super(safeMessage(kind));
            this.kind = Objects.requireNonNull(kind, "HTTP failure kind must not be null");
        }

        /** Creates a safe classified transport failure while discarding an unsafe adapter cause. */
        public HttpClientException(FailureKind kind, String ignoredMessage, Throwable cause) {
            // Adapter causes can contain a raw URI, query or payload excerpt. The SPI boundary keeps
            // only the stable classification; access-controlled adapter diagnostics belong elsewhere.
            super(safeMessage(kind));
            this.kind = Objects.requireNonNull(kind, "HTTP failure kind must not be null");
        }

        /** @return stable retry/safety classification */
        public FailureKind kind() {
            return kind;
        }

        private static String safeMessage(FailureKind kind) {
            Objects.requireNonNull(kind, "HTTP failure kind must not be null");
            return switch (kind) {
                case TARGET_REJECTED -> "HTTP target rejected";
                case ADMISSION_REJECTED -> "HTTP capacity unavailable";
                case TIMEOUT_BEFORE_SEND -> "HTTP request timed out before send";
                case CONNECTION_FAILURE_BEFORE_SEND -> "HTTP connection failed before send";
                case TIMEOUT_AFTER_SEND -> "HTTP request timed out after send";
                case CONNECTION_LOST_AFTER_SEND -> "HTTP connection was lost after send";
                case RESPONSE_TOO_LARGE -> "HTTP response exceeded the byte limit";
                case INVALID_RESPONSE -> "HTTP response was invalid";
            };
        }
    }
}
