package com.playrewards.flowengine.adapter.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.Test;

/** Exercises real pinned sockets and the adapter's strict HTTP/1.1 and TLS boundaries. */
class HttpTransportProtocolTest {

    private static final CancellationToken NEVER_CANCELLED = () -> false;

    @Test
    void supports_chunked_and_connection_close_bodies_with_finite_framing() throws Exception {
        try (RawServer chunked = new RawServer(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + "4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Trace: safe\r\n\r\n");
                JdkHttpNodeClient client = client(chunked, null)) {
            assertEquals("Wikipedia", text(client.execute(request(chunked.uri("http"), 64, Map.of()))
                    .get(2, TimeUnit.SECONDS).body()));
        }

        try (RawServer closeDelimited = new RawServer(
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nclose-body");
                JdkHttpNodeClient client = client(closeDelimited, null)) {
            assertEquals("close-body", text(client.execute(
                    request(closeDelimited.uri("http"), 64, Map.of()))
                    .get(2, TimeUnit.SECONDS).body()));
        }
    }

    @Test
    void rejects_ambiguous_or_malformed_response_framing() throws Exception {
        assertFailure("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n"
                        + "Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nContent-Length: 2, 3\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + "1\r\na\r\n0\r\nmalformed-trailer\r\n\r\n",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\nContent-Length: 2\n\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\noversized",
                8, HttpNodeClient.FailureKind.RESPONSE_TOO_LARGE);
        assertFailure("HTTP/1.1 200 OK\r\nContent-Length: +1\r\n\r\na",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nContent-Length: 1,\r\n\r\na",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + "+1\r\na\r\n0\r\n\r\n",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + " 1\r\na\r\n0\r\n\r\n",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + "1\r\na\r\n0\r\nX-Trailer: safe\u000b\r\n\r\n",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                        + "ffffffffffffffff\r\n",
                64, HttpNodeClient.FailureKind.RESPONSE_TOO_LARGE);
        assertFailure("HTTP/1.1 +200 OK\r\nContent-Length: 2\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 0200 OK\r\nContent-Length: 2\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 ²00 OK\r\nContent-Length: 2\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 200 OK\r\nX-é: value\r\nContent-Length: 2\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
    }

    @Test
    void skips_bounded_informational_responses_but_rejects_protocol_upgrade() throws Exception {
        try (RawServer earlyHints = new RawServer(
                "HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n"
                        + "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                JdkHttpNodeClient client = client(earlyHints, null)) {
            HttpNodeClient.HttpResponse response = client.execute(
                    request(earlyHints.uri("http"), 64, Map.of())).get(2, TimeUnit.SECONDS);
            assertEquals(200, response.statusCode());
            assertEquals("ok", text(response.body()));
        }
        try (RawServer continueResponse = new RawServer(
                "HTTP/1.1 100 Continue\r\n\r\n"
                        + "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                JdkHttpNodeClient client = client(continueResponse, null)) {
            assertEquals(200, client.execute(request(continueResponse.uri("http"), 64, Map.of()))
                    .get(2, TimeUnit.SECONDS).statusCode());
        }
        try (RawServer processing = new RawServer(
                "HTTP/1.1 102 Processing\r\n\r\n"
                        + "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
                JdkHttpNodeClient client = client(processing, null)) {
            assertEquals(200, client.execute(request(processing.uri("http"), 64, Map.of()))
                    .get(2, TimeUnit.SECONDS).statusCode());
        }

        String interim = "HTTP/1.1 103 Early Hints\r\n\r\n";
        assertFailure(interim.repeat(9)
                        + "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        assertFailure("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\n"
                        + "Upgrade: websocket\r\n\r\n",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
        String largeInterimHeader = "X-Fill: " + "a".repeat(33_000) + "\r\n";
        assertFailure("HTTP/1.1 103 Early Hints\r\n" + largeInterimHeader + "\r\n"
                        + "HTTP/1.1 103 Early Hints\r\n" + largeInterimHeader + "\r\n"
                        + "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
                64, HttpNodeClient.FailureKind.INVALID_RESPONSE);
    }

    @Test
    void rejects_request_header_injection_before_opening_a_socket() throws Exception {
        try (RawServer server = new RawServer("HTTP/1.1 204 No Content\r\n\r\n");
                JdkHttpNodeClient client = client(server, null)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> client.execute(request(server.uri("http"), 64,
                            Map.of("X-Safe", "value\r\nInjected: yes")))
                            .get(2, TimeUnit.SECONDS));

            assertEquals(HttpNodeClient.FailureKind.INVALID_RESPONSE,
                    ((HttpNodeClient.HttpClientException) failure.getCause()).kind());
            assertEquals(0, server.hits.get());
        }
    }

    @Test
    void connection_reset_during_request_write_stays_possibly_sent_and_unresolved() throws Exception {
        InetAddress address = InetAddress.getLoopbackAddress();
        try (ServerSocket resetServer = new ServerSocket(0, 1, address)) {
            Thread resetter = Thread.ofVirtual().start(() -> {
                try (Socket accepted = resetServer.accept()) {
                    accepted.setSoLinger(true, 0);
                } catch (IOException ignored) {
                    // Client-side stable classification is asserted below.
                }
            });
            URI uri = new URI("http", null, address.getHostAddress(), resetServer.getLocalPort(),
                    "/reset", null, null);
            HttpTargetPolicy local = (target, purpose, cancellation, deadline) ->
                    new HttpTargetPolicy.ApprovedTarget(target, List.of(address), resetServer.getLocalPort());
            RecordingSendObserver observer = new RecordingSendObserver();
            byte[] body = new byte[4 * 1_024 * 1_024];
            HttpNodeClient.HttpRequest request = new HttpNodeClient.HttpRequest("POST", uri,
                    Map.of("Content-Type", "application/octet-stream"), body, Duration.ofSeconds(2),
                    Instant.now().plusSeconds(3), 64, ExecutionPurpose.TRIAL, NEVER_CANCELLED, observer);
            try (JdkHttpNodeClient client = new JdkHttpNodeClient(
                    local, local, 1, Duration.ofMillis(5))) {
                ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> client.execute(request).get(3, TimeUnit.SECONDS));

                assertEquals(HttpNodeClient.FailureKind.CONNECTION_LOST_AFTER_SEND,
                        ((HttpNodeClient.HttpClientException) failure.getCause()).kind());
                assertTrue(observer.isSentUnresolved());
            }
            resetter.join(2_000);
            assertFalse(resetter.isAlive());
        }
    }

    @Test
    void https_layers_on_the_approved_socket_and_keeps_original_host_for_sni_and_verification()
            throws Exception {
        try (RawServer server = new RawServer("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")) {
            RecordingSslSocketFactory tls = new RecordingSslSocketFactory();
            try (JdkHttpNodeClient client = client(server, tls)) {
                URI target = server.uri("https", "api.public.example");
                HttpNodeClient.HttpResponse response = client.execute(request(target, 64, Map.of()))
                        .get(2, TimeUnit.SECONDS);

                assertEquals("ok", text(response.body()));
                assertEquals("api.public.example", tls.peerHost);
                assertEquals("HTTPS", tls.socket.parameters.getEndpointIdentificationAlgorithm());
                assertEquals("api.public.example",
                        ((javax.net.ssl.SNIHostName) tls.socket.parameters.getServerNames().getFirst())
                                .getAsciiName());
                assertTrue(tls.socket.handshakeStarted.get());
                assertFalse(tls.socket.closedBeforeHandshake.get());
            }
        }
    }

    private static void assertFailure(String response, int maxBytes,
            HttpNodeClient.FailureKind expected) throws Exception {
        try (RawServer server = new RawServer(response); JdkHttpNodeClient client = client(server, null)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> client.execute(request(server.uri("http"), maxBytes, Map.of()))
                            .get(2, TimeUnit.SECONDS));
            assertEquals(expected, ((HttpNodeClient.HttpClientException) failure.getCause()).kind());
        }
    }

    private static JdkHttpNodeClient client(RawServer server, SSLSocketFactory tls) {
        HttpTargetPolicy local = (uri, purpose, cancellation, deadline) ->
                new HttpTargetPolicy.ApprovedTarget(
                        uri, List.of(server.address), server.socket.getLocalPort());
        return tls == null
                ? new JdkHttpNodeClient(local, local, 2, Duration.ofMillis(5))
                : new JdkHttpNodeClient(local, local, 2, Duration.ofMillis(5), tls);
    }

    private static HttpNodeClient.HttpRequest request(URI uri, int maxBytes,
            Map<String, String> headers) {
        return new HttpNodeClient.HttpRequest("GET", uri, headers, new byte[0],
                Duration.ofSeconds(2), Instant.now().plusSeconds(3), maxBytes,
                ExecutionPurpose.TRIAL, NEVER_CANCELLED);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static final class RawServer implements AutoCloseable {

        private final InetAddress address = InetAddress.getLoopbackAddress();
        private final ServerSocket socket;
        private final Thread worker;
        private final AtomicInteger hits = new AtomicInteger();

        private RawServer(String response) throws IOException {
            socket = new ServerSocket(0, 1, address);
            worker = Thread.ofVirtual().start(() -> serve(response));
        }

        private URI uri(String scheme) {
            return uri(scheme, address.getHostAddress());
        }

        private URI uri(String scheme, String host) {
            return URI.create(scheme + "://" + host + ":" + socket.getLocalPort() + "/resource");
        }

        private void serve(String response) {
            try (Socket accepted = socket.accept()) {
                hits.incrementAndGet();
                readRequest(accepted.getInputStream());
                accepted.getOutputStream().write(response.getBytes(StandardCharsets.ISO_8859_1));
                accepted.getOutputStream().flush();
            } catch (IOException ignored) {
                // Test assertions observe the client classification; close may interrupt accept/write.
            }
        }

        private static void readRequest(InputStream input) throws IOException {
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            int matched = 0;
            while (matched < 4) {
                int next = input.read();
                if (next < 0) {
                    return;
                }
                headers.write(next);
                byte[] marker = {'\r', '\n', '\r', '\n'};
                matched = next == marker[matched] ? matched + 1 : next == '\r' ? 1 : 0;
            }
        }

        @Override
        public void close() throws Exception {
            socket.close();
            worker.join(2_000);
        }
    }

    private static final class RecordingSslSocketFactory extends SSLSocketFactory {

        private volatile String peerHost;
        private volatile RecordingSslSocket socket;

        @Override
        public Socket createSocket(Socket connected, String host, int port, boolean autoClose) {
            peerHost = host;
            socket = new RecordingSslSocket(connected);
            return socket;
        }

        @Override public String[] getDefaultCipherSuites() { return new String[0]; }
        @Override public String[] getSupportedCipherSuites() { return new String[0]; }
        @Override public Socket createSocket(String host, int port) { throw unsupported(); }
        @Override public Socket createSocket(String host, int port, InetAddress local, int localPort) {
            throw unsupported();
        }
        @Override public Socket createSocket(InetAddress host, int port) { throw unsupported(); }
        @Override public Socket createSocket(InetAddress address, int port,
                InetAddress local, int localPort) { throw unsupported(); }

        private static UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("unexpected TLS connection path");
        }
    }

    private static final class RecordingSendObserver implements HttpNodeClient.SendObserver {

        private final AtomicBoolean unresolved = new AtomicBoolean();

        @Override
        public void possiblySent() {
            unresolved.set(true);
        }

        @Override
        public void resolved() {
            unresolved.set(false);
        }

        @Override
        public boolean isSentUnresolved() {
            return unresolved.get();
        }
    }

    private static final class RecordingSslSocket extends SSLSocket {

        private final Socket delegate;
        private final AtomicBoolean handshakeStarted = new AtomicBoolean();
        private final AtomicBoolean closedBeforeHandshake = new AtomicBoolean();
        private volatile SSLParameters parameters = new SSLParameters();

        private RecordingSslSocket(Socket delegate) {
            this.delegate = delegate;
        }

        @Override public InputStream getInputStream() throws IOException { return delegate.getInputStream(); }
        @Override public OutputStream getOutputStream() throws IOException { return delegate.getOutputStream(); }
        @Override public void close() throws IOException {
            if (!handshakeStarted.get()) {
                closedBeforeHandshake.set(true);
            }
            delegate.close();
        }
        @Override public void setSoTimeout(int timeout) throws java.net.SocketException {
            delegate.setSoTimeout(timeout);
        }
        @Override public SSLParameters getSSLParameters() { return parameters; }
        @Override public void setSSLParameters(SSLParameters value) { parameters = value; }
        @Override public void startHandshake() throws IOException {
            if (delegate.isClosed()) {
                closedBeforeHandshake.set(true);
                throw new IOException("approved socket was closed before TLS handshake");
            }
            handshakeStarted.set(true);
        }
        @Override public String[] getSupportedCipherSuites() { return new String[0]; }
        @Override public String[] getEnabledCipherSuites() { return new String[0]; }
        @Override public void setEnabledCipherSuites(String[] suites) { }
        @Override public String[] getSupportedProtocols() { return new String[0]; }
        @Override public String[] getEnabledProtocols() { return new String[0]; }
        @Override public void setEnabledProtocols(String[] protocols) { }
        @Override public SSLSession getSession() { return null; }
        @Override public void addHandshakeCompletedListener(HandshakeCompletedListener listener) { }
        @Override public void removeHandshakeCompletedListener(HandshakeCompletedListener listener) { }
        @Override public void setUseClientMode(boolean mode) { }
        @Override public boolean getUseClientMode() { return true; }
        @Override public void setNeedClientAuth(boolean need) { }
        @Override public boolean getNeedClientAuth() { return false; }
        @Override public void setWantClientAuth(boolean want) { }
        @Override public boolean getWantClientAuth() { return false; }
        @Override public void setEnableSessionCreation(boolean flag) { }
        @Override public boolean getEnableSessionCreation() { return true; }
    }
}
