package com.playrewards.flowengine.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts the standalone server; runtime flow behavior is deliberately out of scope for this bootstrap. */
@SpringBootApplication
public class FlowEngineApplication {
    /** Starts the server process. */
    public static void main(String[] args) {
        SpringApplication.run(FlowEngineApplication.class, args);
    }
}
