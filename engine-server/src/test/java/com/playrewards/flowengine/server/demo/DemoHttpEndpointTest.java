package com.playrewards.flowengine.server.demo;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.spi.HttpTargetPolicy;

import org.junit.jupiter.api.Test;

import java.net.URI;

/** The demo's exception to private-target rejection must never open other local services. */
class DemoHttpEndpointTest {
    @Test
    void policy_accepts_only_its_owned_socket_and_exact_path() throws Exception {
        try (var endpoint = new DemoHttpEndpoint()) {
            var policy = endpoint.policy();
            policy.validate(URI.create(endpoint.url() + "?value=demo"), ExecutionPurpose.TRIAL);
            for (String target :
                    java.util.List.of(
                            "http://127.0.0.1:1/demo",
                            endpoint.url() + "/other",
                            endpoint.url().replace("127.0.0.1", "localhost"),
                            endpoint.url() + "#fragment",
                            endpoint.url().replace("127.0.0.1", "user@127.0.0.1"))) {
                assertThrows(
                        HttpTargetPolicy.TargetRejectedException.class,
                        () -> policy.validate(URI.create(target), ExecutionPurpose.TRIAL));
            }
        }
    }
}
