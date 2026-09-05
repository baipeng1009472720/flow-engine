package com.playrewards.flowengine.server.demo;

import com.playrewards.flowengine.spi.HttpTargetPolicy;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Local static HTTP fixture for transport acceptance, never a proxy or production business service.
 */
public final class DemoHttpEndpoint implements AutoCloseable {
    private final HttpServer server;
    private final ThreadPoolExecutor workers;
    private final InetAddress loopback;

    /**
     * Binds an ephemeral loopback port with finite workers and queue only in explicitly enabled
     * demo mode.
     */
    public DemoHttpEndpoint() throws IOException {
        loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        server = HttpServer.create(new InetSocketAddress(loopback, 0), 8);
        workers =
                new ThreadPoolExecutor(
                        2,
                        2,
                        0,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(8),
                        Thread.ofPlatform().daemon(true).name("flow-demo-http-", 0).factory(),
                        new ThreadPoolExecutor.AbortPolicy());
        server.setExecutor(workers);
        server.createContext(
                "/demo",
                exchange -> {
                    try (exchange) {
                        if (!"/demo".equals(exchange.getRequestURI().getRawPath())
                                || !"GET".equals(exchange.getRequestMethod())) {
                            exchange.sendResponseHeaders(404, -1);
                            return;
                        }
                        byte[] body =
                                "{\"message\":\"demo-http\"}".getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, body.length);
                        exchange.getResponseBody().write(body);
                    }
                });
        server.start();
    }

    /** Returns the sole fixture address; callers cannot configure a different upstream. */
    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/demo";
    }

    /**
     * Approves only this exact socket and path, without DNS resolution or arbitrary private-target
     * access.
     */
    public HttpTargetPolicy policy() {
        URI endpoint = URI.create(url());
        return (uri, purpose, cancellation, deadline) -> {
            cancellation.throwIfCancellationRequested();
            if (!"http".equals(uri.getScheme())
                    || !"127.0.0.1".equals(uri.getHost())
                    || uri.getPort() != endpoint.getPort()
                    || !"/demo".equals(uri.getRawPath())
                    || uri.getRawUserInfo() != null
                    || uri.getRawFragment() != null) {
                throw new HttpTargetPolicy.TargetRejectedException(
                        "Only the local demo fixture is allowed");
            }
            return new HttpTargetPolicy.ApprovedTarget(uri, List.of(loopback), endpoint.getPort());
        };
    }

    /** Stops the fixture listener and its bounded workers on host shutdown. */
    @Override
    public void close() {
        server.stop(0);
        workers.shutdownNow();
    }
}
