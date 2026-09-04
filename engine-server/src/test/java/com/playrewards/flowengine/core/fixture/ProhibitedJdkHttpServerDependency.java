package com.playrewards.flowengine.core.fixture;

import com.sun.net.httpserver.HttpServer;

/** Test-only fixture proving protected layers reject JDK HTTP server dependencies. */
public final class ProhibitedJdkHttpServerDependency {
    private HttpServer server;
}
