package com.playrewards.flowengine.adapter.http;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Address-pinned HTTP/1.1 adapter with finite admission, cancellation and bounded response framing.
 *
 * <p>The policy resolves and approves concrete addresses. This transport connects only to those
 * addresses and never resolves the original host again. HTTPS layers TLS over the already-connected
 * socket while retaining the URI host for SNI and HTTPS hostname verification.</p>
 */
public final class JdkHttpNodeClient implements HttpNodeClient, AutoCloseable {

    private static final int MAX_HEADER_BYTES = 65_536;
    private static final int MAX_HEADER_COUNT = 128;
    private static final Set<String> METHODS = Set.of("GET", "POST", "PUT", "PATCH", "DELETE");
    private static final Set<String> RESERVED_REQUEST_HEADERS = Set.of(
            "accept-encoding", "connection", "content-length", "host", "transfer-encoding");

    private final HttpTargetPolicy productionPolicy;
    private final HttpTargetPolicy trialPolicy;
    private final Semaphore transportAdmission;
    private final Semaphore waiterAdmission;
    private final Duration admissionPoll;
    private final ExecutorService tasks;
    private final ScheduledExecutorService deadlines;
    private final SSLSocketFactory sslSockets;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<TransportFuture, Boolean> sources = new ConcurrentHashMap<>();

    /** Creates the default public-target client with 64 active requests and 64 bounded waiters. */
    public JdkHttpNodeClient() {
        this(new DefaultHttpTargetPolicy(), new DefaultHttpTargetPolicy(),
                64, Duration.ofMillis(10), defaultSslSockets());
    }

    /** Creates a transport with separate production/trial policy entry points and finite admission. */
    public JdkHttpNodeClient(HttpTargetPolicy productionPolicy, HttpTargetPolicy trialPolicy,
            int maxConcurrentRequests, Duration admissionPoll) {
        this(productionPolicy, trialPolicy, maxConcurrentRequests, admissionPoll, defaultSslSockets());
    }

    /** Package-local TLS seam used to prove SNI/hostname-preserving address pinning. */
    JdkHttpNodeClient(HttpTargetPolicy productionPolicy, HttpTargetPolicy trialPolicy,
            int maxConcurrentRequests, Duration admissionPoll, SSLSocketFactory sslSockets) {
        this.productionPolicy = Objects.requireNonNull(productionPolicy, "productionPolicy must not be null");
        this.trialPolicy = Objects.requireNonNull(trialPolicy, "trialPolicy must not be null");
        if (maxConcurrentRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests must be positive");
        }
        this.transportAdmission = new Semaphore(maxConcurrentRequests, true);
        this.waiterAdmission = new Semaphore(maxConcurrentRequests, true);
        this.admissionPoll = positive(admissionPoll, "admissionPoll");
        this.sslSockets = Objects.requireNonNull(sslSockets, "sslSockets must not be null");
        this.tasks = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("flow-http-", 0).factory());
        this.deadlines = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("flow-http-deadline-", 0).factory());
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
        sources.put(result, Boolean.TRUE);
        result.whenComplete((ignored, failure) -> sources.remove(result));
        try {
            tasks.submit(() -> run(request, result));
        } catch (RuntimeException failure) {
            sources.remove(result);
            waiterAdmission.release();
            result.completeExceptionally(new HttpClientException(
                    FailureKind.ADMISSION_REJECTED, "HTTP capacity unavailable", failure));
        }
        return result;
    }

    private void run(HttpNodeClient.HttpRequest request, TransportFuture result) {
        boolean waiterHeld = true;
        boolean transportHeld = false;
        AtomicBoolean possiblySent = new AtomicBoolean();
        AtomicBoolean timedOut = new AtomicBoolean();
        Thread worker = Thread.currentThread();
        result.worker(worker);
        HttpResponse successful = null;
        Throwable selectedFailure = null;
        boolean cancelled = false;
        ScheduledFuture<?> deadlineStop = null;
        Instant operationDeadline = operationDeadline(request);
        try (CancellationToken.CancellationRegistration ignored =
                request.cancellationToken().onCancellation(result::requestCancellation)) {
            if (result.cancellationRequested()) {
                throw new CancellationException("HTTP request was cancelled");
            }
            request.cancellationToken().throwIfCancellationRequested();
            HttpTargetPolicy.ApprovedTarget approved = policy(request.purpose()).approve(
                    request.uri(), request.purpose(), request.cancellationToken(), operationDeadline);
            requireMatchingApproval(request.uri(), approved);
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
            byte[] requestBytes = requestBytes(request);
            long deadlineDelay = remainingNanos(operationDeadline);
            if (deadlineDelay <= 0) {
                throw new HttpClientException(FailureKind.TIMEOUT_BEFORE_SEND,
                        "HTTP request timed out before send");
            }
            deadlineStop = deadlines.schedule(() -> {
                timedOut.set(true);
                result.interruptTransport();
            }, deadlineDelay, TimeUnit.NANOSECONDS);
            Socket socket = connect(approved, operationDeadline, result, timedOut);
            OutputStream output = socket.getOutputStream();
            // Once write begins the kernel or peer may observe any prefix even when write/flush
            // later fails. Connection and stream-acquisition failures remain definitive pre-send.
            possiblySent.set(true);
            request.sendObserver().possiblySent();
            output.write(requestBytes);
            output.flush();
            successful = readResponse(socket.getInputStream(), request.maxResponseBytes());
            request.sendObserver().resolved();
        } catch (CancellationException failure) {
            cancelled = true;
        } catch (HttpTargetPolicy.TargetRejectedException failure) {
            selectedFailure = switch (failure.kind()) {
                case REJECTED -> new HttpClientException(
                        FailureKind.TARGET_REJECTED, "HTTP target rejected", failure);
                case TIMEOUT -> new HttpClientException(
                        FailureKind.TIMEOUT_BEFORE_SEND, "HTTP request timed out before send", failure);
                case CAPACITY -> new HttpClientException(
                        FailureKind.ADMISSION_REJECTED, "HTTP capacity unavailable", failure);
            };
        } catch (HttpClientException failure) {
            selectedFailure = failure;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            if (request.cancellationToken().isCancellationRequested() || result.cancellationRequested()) {
                cancelled = true;
            } else {
                selectedFailure = timedOut.get()
                        ? new HttpClientException(possiblySent.get()
                                ? FailureKind.TIMEOUT_AFTER_SEND : FailureKind.TIMEOUT_BEFORE_SEND,
                                "HTTP request timed out", failure)
                        : new HttpClientException(possiblySent.get()
                                ? FailureKind.CONNECTION_LOST_AFTER_SEND
                                : FailureKind.CONNECTION_FAILURE_BEFORE_SEND,
                                "HTTP transport interrupted", failure);
            }
        } catch (IOException failure) {
            if (request.cancellationToken().isCancellationRequested() || result.cancellationRequested()) {
                cancelled = true;
            } else if (timedOut.get() || failure instanceof SocketTimeoutException) {
                selectedFailure = new HttpClientException(possiblySent.get()
                        ? FailureKind.TIMEOUT_AFTER_SEND : FailureKind.TIMEOUT_BEFORE_SEND,
                        "HTTP request timed out", failure);
            } else {
                selectedFailure = new HttpClientException(possiblySent.get()
                        ? FailureKind.CONNECTION_LOST_AFTER_SEND
                        : FailureKind.CONNECTION_FAILURE_BEFORE_SEND,
                        "HTTP transport failed", failure);
            }
        } catch (RuntimeException failure) {
            selectedFailure = new HttpClientException(
                    possiblySent.get() ? FailureKind.INVALID_RESPONSE
                            : FailureKind.CONNECTION_FAILURE_BEFORE_SEND,
                    "HTTP transport failed", failure);
        } finally {
            if (deadlineStop != null) {
                deadlineStop.cancel(false);
            }
            result.closeTransport();
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

    private Socket connect(HttpTargetPolicy.ApprovedTarget approved, Instant deadline,
            TransportFuture result, AtomicBoolean timedOut) throws IOException {
        IOException lastFailure = null;
        for (InetAddress address : approved.addresses()) {
            if (remainingNanos(deadline) <= 0 || timedOut.get()) {
                throw new SocketTimeoutException("HTTP connection timed out");
            }
            Socket plain = new Socket();
            result.socket(plain);
            try {
                plain.connect(new InetSocketAddress(address, approved.port()), timeoutMillis(deadline));
                plain.setSoTimeout(timeoutMillis(deadline));
                if (!approved.uri().getScheme().equalsIgnoreCase("https")) {
                    return plain;
                }
                String host = unbracket(approved.uri().getHost());
                SSLSocket tls = (SSLSocket) sslSockets.createSocket(plain, host, approved.port(), true);
                SSLParameters parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                if (!isIpLiteral(host)) {
                    parameters.setServerNames(List.of(new SNIHostName(host)));
                }
                tls.setSSLParameters(parameters);
                tls.setSoTimeout(timeoutMillis(deadline));
                result.layeredSocket(plain, tls);
                tls.startHandshake();
                return tls;
            } catch (ConnectException failure) {
                close(plain);
                lastFailure = failure;
            } catch (IOException | RuntimeException failure) {
                close(plain);
                throw failure;
            }
        }
        throw lastFailure == null ? new ConnectException("HTTP connection failed") : lastFailure;
    }

    private static byte[] requestBytes(HttpNodeClient.HttpRequest request) {
        if (!METHODS.contains(request.method())) {
            throw invalidRequest();
        }
        URI uri = request.uri();
        if (uri.isOpaque() || uri.getHost() == null || uri.getRawAuthority() == null
                || uri.getRawUserInfo() != null || uri.getRawFragment() != null) {
            throw invalidRequest();
        }
        StringBuilder target = new StringBuilder(
                uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath());
        if (uri.getRawQuery() != null) {
            target.append('?').append(uri.getRawQuery());
        }
        StringBuilder headers = new StringBuilder(request.method()).append(' ').append(target)
                .append(" HTTP/1.1\r\nHost: ").append(hostHeader(uri))
                .append("\r\nConnection: close\r\nAccept-Encoding: identity\r\n");
        Set<String> seen = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> header : request.headers().entrySet()) {
            if (!seen.add(header.getKey()) || !validHeaderName(header.getKey())
                    || !validHeaderValue(header.getValue())
                    || RESERVED_REQUEST_HEADERS.contains(header.getKey().toLowerCase(Locale.ROOT))) {
                throw invalidRequest();
            }
            headers.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
        }
        byte[] body = request.body();
        if (body.length > 0 || Set.of("POST", "PUT", "PATCH").contains(request.method())) {
            headers.append("Content-Length: ").append(body.length).append("\r\n");
        }
        headers.append("\r\n");
        byte[] head = headers.toString().getBytes(StandardCharsets.ISO_8859_1);
        byte[] encoded = new byte[head.length + body.length];
        System.arraycopy(head, 0, encoded, 0, head.length);
        System.arraycopy(body, 0, encoded, head.length, body.length);
        return encoded;
    }

    private static HttpResponse readResponse(InputStream input, int maxBytes) throws IOException {
        LineReader lines = new LineReader(input, MAX_HEADER_BYTES);
        String statusLine = requiredLine(lines);
        String[] statusParts = statusLine.split(" ", 3);
        if (statusParts.length < 2 || !(statusParts[0].equals("HTTP/1.1") || statusParts[0].equals("HTTP/1.0"))) {
            throw invalidResponse();
        }
        int status;
        try {
            status = Integer.parseInt(statusParts[1]);
        } catch (NumberFormatException failure) {
            throw invalidResponse();
        }
        if (status < 100 || status > 599) {
            throw invalidResponse();
        }
        Map<String, List<String>> headers = new LinkedHashMap<>();
        int count = 0;
        while (true) {
            String line = requiredLine(lines);
            if (line.isEmpty()) {
                break;
            }
            if (++count > MAX_HEADER_COUNT || line.startsWith(" ") || line.startsWith("\t")) {
                throw invalidResponse();
            }
            int colon = line.indexOf(':');
            if (colon <= 0) {
                throw invalidResponse();
            }
            String name = line.substring(0, colon);
            String value = line.substring(colon + 1).strip();
            if (!validHeaderName(name) || !validHeaderValue(value)) {
                throw invalidResponse();
            }
            String existing = headers.keySet().stream()
                    .filter(key -> key.equalsIgnoreCase(name)).findFirst().orElse(name);
            headers.computeIfAbsent(existing, ignored -> new ArrayList<>()).add(value);
        }
        rejectRedirect(status);
        rejectEncoded(headers);
        byte[] body;
        if (status / 100 == 1 || status == 204 || status == 304) {
            body = new byte[0];
        } else {
            List<String> transferEncoding = headerValues(headers, "Transfer-Encoding");
            List<String> contentLength = headerValues(headers, "Content-Length");
            if (!transferEncoding.isEmpty() && !contentLength.isEmpty()) {
                throw invalidResponse();
            }
            if (!transferEncoding.isEmpty()) {
                if (transferEncoding.size() != 1
                        || !transferEncoding.getFirst().equalsIgnoreCase("chunked")) {
                    throw invalidResponse();
                }
                body = readChunked(input, lines, maxBytes);
            } else if (!contentLength.isEmpty()) {
                long length = contentLength(contentLength);
                if (length > maxBytes) {
                    throw tooLarge();
                }
                body = readExact(input, (int) length);
            } else {
                body = readUntilEof(input, maxBytes);
            }
        }
        Map<String, List<String>> immutableHeaders = new LinkedHashMap<>();
        headers.forEach((name, values) -> immutableHeaders.put(name, List.copyOf(values)));
        return new HttpResponse(status, immutableHeaders, body);
    }

    private static byte[] readChunked(InputStream input, LineReader lines, int maxBytes) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(maxBytes, 8_192));
        while (true) {
            String sizeLine = requiredLine(lines);
            String rawSize = sizeLine.split(";", 2)[0].strip();
            long size;
            try {
                size = Long.parseUnsignedLong(rawSize, 16);
            } catch (NumberFormatException failure) {
                throw invalidResponse();
            }
            if (size == 0) {
                int trailers = 0;
                String trailer;
                while (!(trailer = requiredLine(lines)).isEmpty()) {
                    if (++trailers > MAX_HEADER_COUNT) {
                        throw invalidResponse();
                    }
                    int colon = trailer.indexOf(':');
                    if (colon <= 0 || !validHeaderName(trailer.substring(0, colon))
                            || !validHeaderValue(trailer.substring(colon + 1).strip())) {
                        throw invalidResponse();
                    }
                }
                return output.toByteArray();
            }
            if (size > maxBytes - output.size() || size > Integer.MAX_VALUE) {
                throw tooLarge();
            }
            output.writeBytes(readExact(input, (int) size));
            if (input.read() != '\r' || input.read() != '\n') {
                throw invalidResponse();
            }
        }
    }

    private static byte[] readExact(InputStream input, int length) throws IOException {
        byte[] body = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = input.read(body, offset, length - offset);
            if (read < 0) {
                throw new EOFException("HTTP response ended early");
            }
            offset += read;
        }
        return body;
    }

    private static byte[] readUntilEof(InputStream input, int limit) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(limit, 8_192));
        byte[] buffer = new byte[(int) Math.min((long) limit + 1L, 8_192L)];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            if (read > limit - output.size()) {
                throw tooLarge();
            }
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static long contentLength(List<String> values) {
        Long selected = null;
        for (String value : values) {
            for (String item : value.split(",")) {
                long parsed;
                try {
                    parsed = Long.parseLong(item.strip());
                } catch (NumberFormatException failure) {
                    throw invalidResponse();
                }
                if (parsed < 0 || selected != null && selected != parsed) {
                    throw invalidResponse();
                }
                selected = parsed;
            }
        }
        if (selected == null) {
            throw invalidResponse();
        }
        return selected;
    }

    private static List<String> headerValues(Map<String, List<String>> headers, String name) {
        return headers.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .flatMap(entry -> entry.getValue().stream()).toList();
    }

    private static void rejectRedirect(int status) {
        if (status >= 300 && status <= 399) {
            throw new HttpClientException(FailureKind.TARGET_REJECTED, "HTTP redirect is disabled");
        }
    }

    private static void rejectEncoded(Map<String, List<String>> headers) {
        headerValues(headers, "Content-Encoding").forEach(value -> {
            if (!value.equalsIgnoreCase("identity")) {
                throw tooLarge();
            }
        });
    }

    private HttpTargetPolicy policy(ExecutionPurpose purpose) {
        return purpose == ExecutionPurpose.TRIAL ? trialPolicy : productionPolicy;
    }

    private static void requireMatchingApproval(URI requested, HttpTargetPolicy.ApprovedTarget approved) {
        if (!requested.equals(approved.uri())) {
            throw new HttpTargetPolicy.TargetRejectedException("HTTP target approval mismatch");
        }
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

    private static int timeoutMillis(Instant deadline) throws SocketTimeoutException {
        long remaining = remainingNanos(deadline);
        if (remaining <= 0) {
            throw new SocketTimeoutException("HTTP operation timed out");
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
        if (millis == 0) {
            millis = 1;
        }
        return (int) Math.min(Integer.MAX_VALUE, millis);
    }

    private static long remainingNanos(Instant deadline) {
        try {
            return Math.max(0, Duration.between(Instant.now(), deadline).toNanos());
        } catch (ArithmeticException failure) {
            return deadline.isAfter(Instant.now()) ? Long.MAX_VALUE : 0;
        }
    }

    private static String hostHeader(URI uri) {
        String host = unbracket(uri.getHost());
        String authority = host.indexOf(':') >= 0 ? "[" + host + "]" : host;
        int port = uri.getPort();
        boolean defaultPort = port < 0 || uri.getScheme().equalsIgnoreCase("http") && port == 80
                || uri.getScheme().equalsIgnoreCase("https") && port == 443;
        return defaultPort ? authority : authority + ":" + port;
    }

    private static boolean validHeaderName(String name) {
        if (name == null || name.isEmpty()) {
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
        if (value == null) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character > 0xff || character == '\r' || character == '\n' || character == 0x7f
                    || character < 0x20 && character != '\t') {
                return false;
            }
        }
        return true;
    }

    private static boolean isIpLiteral(String host) {
        if (host.indexOf(':') >= 0) {
            return true;
        }
        return host.chars().allMatch(value -> value == '.' || value >= '0' && value <= '9');
    }

    private static String unbracket(String host) {
        return host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1) : host;
    }

    private static HttpClientException invalidRequest() {
        return new HttpClientException(FailureKind.INVALID_RESPONSE, "HTTP request was invalid");
    }

    private static HttpClientException invalidResponse() {
        return new HttpClientException(FailureKind.INVALID_RESPONSE, "HTTP response was invalid");
    }

    private static HttpClientException tooLarge() {
        return new HttpClientException(FailureKind.RESPONSE_TOO_LARGE, "HTTP response exceeded the byte limit");
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static SSLSocketFactory defaultSslSockets() {
        try {
            return SSLContext.getDefault().getSocketFactory();
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("default TLS context is unavailable", failure);
        }
    }

    private static void close(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Resource cleanup is best-effort; the stable transport failure is selected separately.
        }
    }

    /** Cancels tasks and closes active sockets; each public future completes only after permit cleanup. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        sources.keySet().forEach(TransportFuture::requestCancellation);
        tasks.shutdownNow();
        deadlines.shutdownNow();
    }

    /** Defers public cancellation completion until the worker has closed transport and released permits. */
    private static final class TransportFuture extends CompletableFuture<HttpResponse> {

        private final AtomicBoolean cancellationRequested = new AtomicBoolean();
        private final AtomicReference<Thread> worker = new AtomicReference<>();
        private final AtomicReference<Socket> socket = new AtomicReference<>();

        private void worker(Thread taskWorker) {
            worker.set(taskWorker);
            if (cancellationRequested.get()) {
                taskWorker.interrupt();
            }
        }

        private void socket(Socket activeSocket) {
            Socket previous = socket.getAndSet(activeSocket);
            if (previous != null && previous != activeSocket) {
                close(previous);
            }
            if (activeSocket != null && cancellationRequested.get()) {
                close(activeSocket);
            }
        }

        private void layeredSocket(Socket transportSocket, Socket layeredSocket) {
            if (!socket.compareAndSet(transportSocket, layeredSocket)) {
                close(layeredSocket);
                return;
            }
            if (cancellationRequested.get()) {
                close(layeredSocket);
            }
        }

        private void requestCancellation() {
            cancellationRequested.set(true);
            interruptTransport();
        }

        private void interruptTransport() {
            Socket active = socket.get();
            if (active != null) {
                close(active);
            }
            Thread running = worker.get();
            if (running != null) {
                running.interrupt();
            }
        }

        private void closeTransport() {
            Socket active = socket.getAndSet(null);
            if (active != null) {
                close(active);
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
            requestCancellation();
            return true;
        }
    }

    /** Strict CRLF line reader sharing one finite response-header byte budget. */
    private static final class LineReader {

        private final InputStream input;
        private final int limit;
        private int consumed;

        private LineReader(InputStream input, int limit) {
            this.input = input;
            this.limit = limit;
        }

        private String readLine() throws IOException {
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            boolean readAny = false;
            while (true) {
                int value = input.read();
                if (value < 0) {
                    if (!readAny) {
                        return null;
                    }
                    throw new EOFException("HTTP response line ended early");
                }
                readAny = true;
                if (++consumed > limit) {
                    throw invalidResponse();
                }
                if (value == '\r') {
                    int newline = input.read();
                    if (newline != '\n') {
                        throw invalidResponse();
                    }
                    if (++consumed > limit) {
                        throw invalidResponse();
                    }
                    return line.toString(StandardCharsets.ISO_8859_1);
                }
                if (value == '\n' || value == 0 || value == 0x7f
                        || value < 0x20 && value != '\t') {
                    throw invalidResponse();
                }
                line.write(value);
            }
        }
    }

    private static String requiredLine(LineReader lines) throws IOException {
        String line = lines.readLine();
        if (line == null) {
            throw new EOFException("HTTP response ended before headers completed");
        }
        return line;
    }
}
