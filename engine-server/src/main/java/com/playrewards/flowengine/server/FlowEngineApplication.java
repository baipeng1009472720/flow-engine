package com.playrewards.flowengine.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Starts the generic distribution; reusable controllers and runtime assembly belong to the Starter. */
@SpringBootApplication
public class FlowEngineApplication {
    /** Starts the server process. */
    public static void main(String[] args) {
        SpringApplication.run(FlowEngineApplication.class, args);
    }
}
