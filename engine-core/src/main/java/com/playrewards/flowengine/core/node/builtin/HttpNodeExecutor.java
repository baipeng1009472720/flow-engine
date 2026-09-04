package com.playrewards.flowengine.core.node.builtin;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.core.idempotency.IdempotencyCoordinator;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.core.node.ScriptExecutionSupport;
import com.playrewards.flowengine.core.retry.RetryPolicy;
import com.playrewards.flowengine.core.execution.CleanupBudget;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.HttpNodeClient.FailureKind;
import com.playrewards.flowengine.spi.HttpNodeClient.HttpClientException;
import com.playrewards.flowengine.spi.ScriptRuntime;
import com.playrewards.flowengine.spi.ScriptRuntime.ScriptInvocation;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes the single HTTP canvas contract without depending on a concrete transport or DNS stack.
 *
 * <p>The scheduler already invokes this code on a virtual thread, so waiting for adapter-owned
 * asynchronous stages does not consume a platform thread. Every wait polls cancellation/deadline,
 * and source futures are cancelled when the node stops.</p>
 */
public final class HttpNodeExecutor implements NodeExecutor {

    private static final Set<String> METHODS = Set.of("GET", "POST", "PUT", "PATCH", "DELETE");
    private static final Set<String> QUERY_METHODS = Set.of("GET", "DELETE");
    private static final Set<String> BODY_METHODS = Set.of("POST", "PUT", "PATCH");
    private static final Set<String> FORBIDDEN_HEADERS = Set.of(
            "accept-encoding", "connection", "content-length", "content-type", "host", "keep-alive",
            "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade",
            "idempotency-key");
    private static final Set<String> DEPRECATED_FIELDS = Set.of("query", "bodyBinding", "inputBindings");
    private static final long STOP_POLL_MILLIS = 20;
    private static final JsonValues.JsonResourceLimits HTTP_JSON_LIMITS =
            new JsonValues.JsonResourceLimits(1_048_576, 262_144, 32, 10_000);

    private final ScriptRuntime scripts;
    private final HttpNodeClient client;
    private final RetryPolicy retryPolicy;
    private final IdempotencyCoordinator idempotency;
    private final Clock clock;
    private final Duration maxTimeout;
    private final int maxResponseBytes;

    /** Creates one HTTP executor with explicit finite engine limits. */
    public HttpNodeExecutor(ScriptRuntime scripts, HttpNodeClient client, RetryPolicy retryPolicy,
            IdempotencyCoordinator idempotency, Clock clock, Duration maxTimeout,
            int maxResponseBytes) {
        this.scripts = Objects.requireNonNull(scripts, "scripts must not be null");
        this.client = Objects.requireNonNull(client, "HTTP client must not be null");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.maxTimeout = positive(maxTimeout, "maxTimeout");
        if (maxResponseBytes < 1) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
        this.maxResponseBytes = maxResponseBytes;
    }

    @Override
    public ComponentType supports() {
        return ComponentType.HTTP;
    }

    /** Runs script, request attempts and response validation as one scheduler-owned node attempt. */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        context.throwIfStopped();
        Config config = config(context);
        Map<String, Object> parameters = parameters(context, config.requestParamsScript());
        URI uri = QUERY_METHODS.contains(config.method())
                ? appendQuery(config.uri(), parameters) : config.uri();
        byte[] body = BODY_METHODS.contains(config.method())
                ? canonicalJson(parameters).getBytes(StandardCharsets.UTF_8) : new byte[0];
        Map<String, String> headers = new LinkedHashMap<>(config.headers());
        if (BODY_METHODS.contains(config.method())) {
            headers.put("Content-Type", "application/json; charset=utf-8");
        }
        boolean protectedSideEffect = BODY_METHODS.contains(config.method())
                && context.node().componentContract().capabilities().effect() == ComponentEffect.IDEMPOTENT
                && context.node().componentContract().capabilities().retryable();
        if (protectedSideEffect) {
            headers.put("Idempotency-Key", idempotency.nodeKey(
                    context.request().tenantId(), context.request().idempotencyKey(),
                    context.flowIdentity().version(), context.node().node().nodeCode(), context.scopePath()));
        }
        HttpNodeClient.SendObserver sendObserver = BODY_METHODS.contains(config.method()) && !protectedSideEffect
                ? context.unprotectedSideEffectObserver() : HttpNodeClient.SendObserver.NOOP;
        int retries = retryPolicy.boundedRetries(config.retryCount());
        HttpNodeClient.HttpResponse response = null;
        for (int attempt = 1; attempt <= retries + 1; attempt++) {
            context.throwIfStopped();
            Duration timeout = effectiveTimeout(config.timeout(), context.request().deadline());
            HttpNodeClient.HttpRequest request = new HttpNodeClient.HttpRequest(
                    config.method(), uri, headers, body, timeout, context.request().deadline(),
                    maxResponseBytes, context.request().purpose(), context.cancellationToken(), sendObserver);
            try {
                response = awaitHttp(client.execute(request), context, sendObserver);
                sendObserver.resolved();
            } catch (HttpClientException failure) {
                if (isUnprotectedUncertain(config.method(), protectedSideEffect, failure.kind())) {
                    throw new NodeExecutionException("HTTP_SIDE_EFFECT_UNCERTAIN",
                            "HTTP side effect may have been sent; manual confirmation is required",
                            ExecutionStatus.WAITING_MANUAL);
                }
                sendObserver.resolved();
                if (!clock.instant().isBefore(context.request().deadline())) {
                    context.throwIfStopped();
                }
                if (attempt <= retries && retryable(config.method(), protectedSideEffect, failure.kind())) {
                    backoff(context, retryPolicy.backoffBeforeAttempt(attempt + 1));
                    continue;
                }
                throw safeFailure(failure);
            }
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                break;
            }
            if (attempt <= retries && retryableStatus(config.method(), protectedSideEffect,
                    response.statusCode())) {
                backoff(context, retryPolicy.backoffBeforeAttempt(attempt + 1));
                continue;
            }
            throw new NodeExecutionException("HTTP_STATUS_" + response.statusCode(),
                    "HTTP response status was " + response.statusCode());
        }
        if (response == null) {
            throw new NodeExecutionException("HTTP_EXECUTION_FAILED", "HTTP execution failed");
        }
        if (!config.outputDeclared()) {
            return NodeInvocation.completed(NodeResult.continueWith(Map.of(), Set.of("NEXT")));
        }
        Object decoded = decodeJson(response);
        Map<String, Object> outputs = new LinkedHashMap<>();
        outputs.put("result", decoded);
        return NodeInvocation.completed(NodeResult.continueWith(outputs, Set.of("NEXT")));
    }

    private Config config(NodeExecutionContext context) {
        Map<String, Object> values = context.node().node().config();
        for (String deprecated : DEPRECATED_FIELDS) {
            if (values.containsKey(deprecated)) {
                throw new NodeExecutionException("HTTP_CONFIG_DEPRECATED_FIELD",
                        "HTTP deprecated configuration field is not supported: " + deprecated);
            }
        }
        String method = text(values.get("method"));
        if (method == null || !METHODS.contains(method)) {
            throw new NodeExecutionException("HTTP_CONFIG_METHOD_INVALID",
                    "HTTP method must be GET, POST, PUT, PATCH or DELETE");
        }
        String rawUrl = text(values.get("url"));
        URI uri;
        try {
            uri = rawUrl == null ? null : new URI(rawUrl);
        } catch (URISyntaxException failure) {
            uri = null;
        }
        if (uri == null || !uri.isAbsolute()) {
            throw new NodeExecutionException("HTTP_CONFIG_URL_INVALID", "HTTP url must be an absolute URI");
        }
        String script = text(values.get("requestParamsScript"));
        if (script == null) {
            throw new NodeExecutionException("HTTP_CONFIG_SCRIPT_INVALID",
                    "HTTP requestParamsScript must be non-blank");
        }
        int timeoutMillis = nonNegativeInteger(values.get("timeoutMillis"), "timeoutMillis", false);
        int retryCount = nonNegativeInteger(values.get("retryCount"), "retryCount", true);
        Map<String, String> headers = headers(values.get("headers"));
        Object configuredOutput = values.get("outputProtocol");
        boolean outputConfigured = values.containsKey("outputProtocol") && configuredOutput != null;
        Map<String, OutputSlotContract> outputs = context.node().componentContract().outputSlots();
        boolean outputDeclared = outputs.size() == 1 && outputs.containsKey("result");
        if (outputConfigured != outputDeclared || (!outputs.isEmpty() && !outputDeclared)
                || outputDeclared && !matchesOutputProtocol(configuredOutput, outputs.get("result"))) {
            throw new NodeExecutionException("HTTP_CONFIG_OUTPUT_INVALID",
                    "HTTP outputProtocol and the unique result output contract must agree");
        }
        return new Config(method, uri, script, headers, Duration.ofMillis(timeoutMillis),
                retryCount, outputDeclared);
    }

    private static boolean matchesOutputProtocol(Object configured, OutputSlotContract output) {
        if (!(configured instanceof Map<?, ?> snapshot)) {
            return false;
        }
        Object rawCode = snapshot.containsKey("protocolCode")
                ? snapshot.get("protocolCode") : snapshot.get("code");
        if (!(rawCode instanceof String code) || code.isBlank()
                || !(snapshot.get("schema") instanceof Map<?, ?> rawSchema)) {
            return false;
        }
        try {
            Map<String, Object> schema = castObject(rawSchema);
            return output.name().equals("result") && output.protocol().code().equals(code)
                    && output.protocol().schema().equals(JsonValues.copyObject(schema, "HTTP output protocol"));
        } catch (IllegalArgumentException failure) {
            return false;
        }
    }

    private Map<String, Object> parameters(NodeExecutionContext context, String script) {
        ScriptExecutionSupport.ScriptExecution execution = ScriptExecutionSupport.start(
                () -> scripts.execute(new ScriptInvocation(script, context.externalScopes(), context.ctxView(),
                        context.executionLogger(), context.cancellationToken(), context.request().deadline())));
        Object value = await(execution.completion(), execution.sourceCompletion(), context);
        if (!(value instanceof Map<?, ?> raw)) {
            throw new NodeExecutionException("HTTP_PARAMS_INVALID",
                    "HTTP requestParamsScript must return a JSON object");
        }
        try {
            Map<String, Object> cast = castObject(raw);
            JsonValues.measureJsonResources(List.of(cast), HTTP_JSON_LIMITS);
            return JsonValues.copyObject(cast, "HTTP request parameters");
        } catch (JsonValues.JsonResourceLimitException failure) {
            throw new NodeExecutionException("HTTP_PARAMS_LIMIT_EXCEEDED",
                    "HTTP request parameters exceeded the JSON resource limit");
        } catch (IllegalArgumentException failure) {
            throw new NodeExecutionException("HTTP_PARAMS_INVALID",
                    "HTTP requestParamsScript must return a JSON object");
        }
    }

    private static Map<String, Object> castObject(Map<?, ?> raw) {
        Map<String, Object> values = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (!(key instanceof String text)) {
                throw new IllegalArgumentException("HTTP parameter keys must be strings");
            }
            values.put(text, value);
        });
        return values;
    }

    private static Map<String, String> headers(Object raw) {
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Map<?, ?> configured)) {
            throw new NodeExecutionException("HTTP_CONFIG_HEADERS_INVALID", "HTTP headers must be an object");
        }
        Map<String, String> headers = new LinkedHashMap<>();
        Set<String> seen = new java.util.HashSet<>();
        for (Map.Entry<?, ?> entry : configured.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof String value)
                    || !validHeaderName(name) || !validHeaderValue(value)) {
                throw new NodeExecutionException("HTTP_CONFIG_HEADERS_INVALID",
                        "HTTP header name or value is invalid");
            }
            String normalized = name.toLowerCase(Locale.ROOT);
            if (!seen.add(normalized)) {
                throw new NodeExecutionException("HTTP_CONFIG_HEADERS_DUPLICATE",
                        "HTTP header names must be unique ignoring case");
            }
            if (FORBIDDEN_HEADERS.contains(normalized)) {
                throw new NodeExecutionException("HTTP_CONFIG_HEADER_FORBIDDEN",
                        "HTTP header is controlled by the engine");
            }
            headers.put(name, value);
        }
        return Map.copyOf(headers);
    }

    private static boolean validHeaderName(String name) {
        if (name.isEmpty()) {
            return false;
        }
        for (int index = 0; index < name.length(); index++) {
            char value = name.charAt(index);
            if (!(Character.isLetterOrDigit(value) || "!#$%&'*+-.^_`|~".indexOf(value) >= 0)) {
                return false;
            }
        }
        return true;
    }

    private static boolean validHeaderValue(String value) {
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\r' || character == '\n' || character == 0x7f
                    || (character < 0x20 && character != '\t')) {
                return false;
            }
        }
        return true;
    }

    private static int nonNegativeInteger(Object raw, String field, boolean zeroAllowed) {
        if (!(raw instanceof Number number)) {
            throw new NodeExecutionException("HTTP_CONFIG_NUMBER_INVALID", "HTTP " + field + " is invalid");
        }
        long value = number.longValue();
        if (value != number.doubleValue() || value < (zeroAllowed ? 0 : 1) || value > Integer.MAX_VALUE) {
            throw new NodeExecutionException("HTTP_CONFIG_NUMBER_INVALID", "HTTP " + field + " is invalid");
        }
        return (int) value;
    }

    private Duration effectiveTimeout(Duration configured, Instant executionDeadline) {
        Duration remaining = Duration.between(clock.instant(), executionDeadline);
        if (remaining.isZero() || remaining.isNegative()) {
            throw new com.playrewards.flowengine.core.execution.ExecutionDeadlineExceededException(
                    "execution deadline exceeded");
        }
        Duration bounded = configured.compareTo(maxTimeout) > 0 ? maxTimeout : configured;
        return bounded.compareTo(remaining) > 0 ? remaining : bounded;
    }

    private static boolean retryable(String method, boolean protectedSideEffect, FailureKind kind) {
        if (!(QUERY_METHODS.contains(method) || protectedSideEffect)) {
            return false;
        }
        return switch (kind) {
            case TIMEOUT_BEFORE_SEND, CONNECTION_FAILURE_BEFORE_SEND,
                    TIMEOUT_AFTER_SEND, CONNECTION_LOST_AFTER_SEND -> true;
            default -> false;
        };
    }

    private static boolean retryableStatus(String method, boolean protectedSideEffect, int status) {
        return (QUERY_METHODS.contains(method) || protectedSideEffect)
                && (status == 408 || status == 429 || status >= 500 && status <= 599);
    }

    private static boolean isUnprotectedUncertain(
            String method, boolean protectedSideEffect, FailureKind kind) {
        return BODY_METHODS.contains(method) && !protectedSideEffect
                && (kind == FailureKind.TIMEOUT_AFTER_SEND || kind == FailureKind.CONNECTION_LOST_AFTER_SEND);
    }

    private static NodeExecutionException safeFailure(HttpClientException failure) {
        String code = switch (failure.kind()) {
            case TARGET_REJECTED -> "HTTP_TARGET_REJECTED";
            case ADMISSION_REJECTED -> "HTTP_CAPACITY_EXCEEDED";
            case TIMEOUT_BEFORE_SEND, TIMEOUT_AFTER_SEND -> "HTTP_TIMEOUT";
            case CONNECTION_FAILURE_BEFORE_SEND, CONNECTION_LOST_AFTER_SEND -> "HTTP_CONNECTION_FAILED";
            case RESPONSE_TOO_LARGE -> "HTTP_RESPONSE_TOO_LARGE";
            case INVALID_RESPONSE -> "HTTP_RESPONSE_INVALID";
        };
        return new NodeExecutionException(code, failure.getMessage());
    }

    private static Object decodeJson(HttpNodeClient.HttpResponse response) {
        byte[] body = response.body();
        if (body.length == 0) {
            return null;
        }
        Charset charset = responseCharset(response.headers());
        try {
            String json = new String(body, charset);
            if (!JSON.isValid(json)) {
                throw new JSONException("invalid JSON");
            }
            Object decoded = JSON.parse(json);
            JsonValues.measureJsonResources(java.util.Collections.singletonList(decoded), HTTP_JSON_LIMITS);
            return JsonValues.copyValue(decoded, "HTTP response");
        } catch (JsonValues.JsonResourceLimitException failure) {
            throw new NodeExecutionException("HTTP_RESPONSE_JSON_LIMIT_EXCEEDED",
                    "HTTP response JSON exceeded the structural resource limit");
        } catch (JSONException | IllegalArgumentException failure) {
            throw new NodeExecutionException("HTTP_RESPONSE_JSON_INVALID",
                    "HTTP response body is not valid bounded JSON");
        }
    }

    private static Charset responseCharset(Map<String, List<String>> headers) {
        String contentType = headers.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase("Content-Type"))
                .flatMap(entry -> entry.getValue().stream()).findFirst().orElse("");
        for (String part : contentType.split(";")) {
            String value = part.strip();
            if (value.toLowerCase(Locale.ROOT).startsWith("charset=")) {
                String name = value.substring("charset=".length()).strip().replace("\"", "");
                String normalized = name.toUpperCase(Locale.ROOT);
                if (!Set.of("UTF-8", "UTF-16", "UTF-16LE", "UTF-16BE", "UTF-32",
                        "UTF-32LE", "UTF-32BE").contains(normalized)) {
                    throw new NodeExecutionException("HTTP_RESPONSE_CHARSET_UNSUPPORTED",
                            "HTTP response charset is not a supported JSON encoding");
                }
                return Charset.forName(normalized);
            }
        }
        return StandardCharsets.UTF_8;
    }

    private static URI appendQuery(URI uri, Map<String, Object> parameters) {
        if (parameters.isEmpty()) {
            return uri;
        }
        StringBuilder query = new StringBuilder(uri.getRawQuery() == null ? "" : uri.getRawQuery());
        new TreeMap<>(parameters).forEach((key, value) -> {
            if (!query.isEmpty()) {
                query.append('&');
            }
            query.append(percentEncode(key)).append('=').append(percentEncode(queryValue(value)));
        });
        try {
            StringBuilder target = new StringBuilder(uri.getScheme()).append("://")
                    .append(uri.getRawAuthority());
            if (uri.getRawPath() != null) {
                target.append(uri.getRawPath());
            }
            target.append('?').append(query);
            return new URI(target.toString());
        } catch (URISyntaxException failure) {
            throw new NodeExecutionException("HTTP_CONFIG_URL_INVALID", "HTTP url query is invalid");
        }
    }

    private static String queryValue(Object value) {
        if (value instanceof String text) {
            return text;
        }
        return canonicalJson(value);
    }

    private static String canonicalJson(Object value) {
        return JSON.toJSONString(canonicalValue(value));
    }

    private static Object canonicalValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((key, nested) -> sorted.put((String) key, canonicalValue(nested)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            List<Object> values = new ArrayList<>(list.size());
            list.forEach(item -> values.add(canonicalValue(item)));
            return values;
        }
        return value;
    }

    private static String percentEncode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte item : value.getBytes(StandardCharsets.UTF_8)) {
            int octet = item & 0xff;
            if (octet >= 'a' && octet <= 'z' || octet >= 'A' && octet <= 'Z'
                    || octet >= '0' && octet <= '9' || octet == '-' || octet == '.'
                    || octet == '_' || octet == '~') {
                encoded.append((char) octet);
            } else {
                encoded.append('%');
                encoded.append(Character.toUpperCase(Character.forDigit(octet >>> 4, 16)));
                encoded.append(Character.toUpperCase(Character.forDigit(octet & 0xf, 16)));
            }
        }
        return encoded.toString();
    }

    private static <T> T await(CompletionStage<T> completion, NodeExecutionContext context) {
        return await(completion, completion, context);
    }

    private <T> T awaitHttp(CompletionStage<T> completion, NodeExecutionContext context,
            HttpNodeClient.SendObserver sendObserver) {
        Objects.requireNonNull(completion, "HTTP completion must not be null");
        var future = completion.toCompletableFuture();
        try {
            while (true) {
                context.cancellationToken().throwIfCancellationRequested();
                Instant decisionDeadline = context.request().deadline();
                Instant now = clock.instant();
                if (!now.isBefore(decisionDeadline)) {
                    cancelAndAwait(completion);
                    if (sendObserver.isSentUnresolved()) {
                        throw new HttpClientException(FailureKind.TIMEOUT_AFTER_SEND,
                                "HTTP request timed out after send");
                    }
                    throw new com.playrewards.flowengine.core.execution.ExecutionDeadlineExceededException(
                            "execution deadline exceeded");
                }
                try {
                    long untilDecision = Math.max(1, Duration.between(now, decisionDeadline).toNanos());
                    return future.get(Math.min(TimeUnit.MILLISECONDS.toNanos(STOP_POLL_MILLIS),
                            untilDecision), TimeUnit.NANOSECONDS);
                } catch (TimeoutException ignored) {
                    // Keep the in-flight side-effect classification separate from scheduler deadline mapping.
                }
            }
        } catch (InterruptedException failure) {
            cancelAndAwait(completion);
            Thread.currentThread().interrupt();
            throw new CancellationException("HTTP node wait was interrupted");
        } catch (ExecutionException failure) {
            Throwable cause = unwrap(failure.getCause());
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new HttpClientException(FailureKind.CONNECTION_LOST_AFTER_SEND,
                    "HTTP transport failed");
        } finally {
            if (context.cancellationToken().isCancellationRequested()) {
                cancel(completion);
            }
        }
    }

    private static <T> T await(CompletionStage<T> completion, CompletionStage<?> source,
            NodeExecutionContext context) {
        Objects.requireNonNull(completion, "completion must not be null");
        Objects.requireNonNull(source, "source completion must not be null");
        var future = completion.toCompletableFuture();
        try {
            while (true) {
                context.throwIfStopped();
                try {
                    return future.get(STOP_POLL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                    // Polling keeps adapter-neutral stages responsive without a dedicated waiter queue.
                }
            }
        } catch (InterruptedException failure) {
            cancelAndAwait(source);
            Thread.currentThread().interrupt();
            throw new CancellationException("HTTP node wait was interrupted");
        } catch (ExecutionException failure) {
            Throwable cause = unwrap(failure.getCause());
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new NodeExecutionException("HTTP_EXECUTION_FAILED", "HTTP execution failed");
        } finally {
            if (context.cancellationToken().isCancellationRequested()) {
                cancel(source);
            }
        }
    }

    private static void cancel(CompletionStage<?> source) {
        if (source instanceof Future<?> future) {
            future.cancel(true);
        } else {
            source.toCompletableFuture().cancel(true);
        }
    }

    private static void cancelAndAwait(CompletionStage<?> source) {
        cancel(source);
        CompletableFuture<?> future = source.toCompletableFuture();
        CleanupBudget budget = CleanupBudget.standard();
        boolean interrupted = false;
        while (!future.isDone() && budget.remainingNanos() > 0) {
            try {
                future.get(budget.remainingNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException ignored) {
                interrupted = true;
            } catch (ExecutionException | CancellationException ignored) {
                break;
            } catch (TimeoutException ignored) {
                break;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void backoff(NodeExecutionContext context, Duration duration) {
        long remaining = duration.toNanos();
        while (remaining > 0) {
            context.throwIfStopped();
            long slice = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(STOP_POLL_MILLIS));
            long started = System.nanoTime();
            try {
                TimeUnit.NANOSECONDS.sleep(slice);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new CancellationException("HTTP retry wait was interrupted");
            }
            remaining -= Math.max(1, System.nanoTime() - started);
        }
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }

    private record Config(String method, URI uri, String requestParamsScript,
                          Map<String, String> headers, Duration timeout,
                          int retryCount, boolean outputDeclared) {
    }
}
