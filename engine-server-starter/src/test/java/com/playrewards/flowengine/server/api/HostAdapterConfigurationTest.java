package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.server.config.EngineAutoConfiguration;
import com.playrewards.flowengine.spi.ExecutionRecordStore;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** A host replacing record storage must deliberately provide the independent payload adapter. */
class HostAdapterConfigurationTest {
    @Test
    void rejects_a_custom_record_store_without_any_payload_adapter() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(EngineAutoConfiguration.class))
                .withBean(
                        ExecutionRecordStore.class,
                        () ->
                                (ExecutionRecordStore)
                                        java.lang.reflect.Proxy.newProxyInstance(
                                                getClass().getClassLoader(),
                                                new Class<?>[] {ExecutionRecordStore.class},
                                                (proxy, method, args) ->
                                                        method.invoke(
                                                                new com.playrewards.flowengine
                                                                        .adapter.memory
                                                                        .InMemoryObservationStore(),
                                                                args)))
                .run(
                        context -> {
                            assertNotNull(
                                    context.getStartupFailure(),
                                    "must not silently construct an unrelated empty payload"
                                            + " execution table");
                            Throwable cause = context.getStartupFailure();
                            while (cause.getCause() != null) cause = cause.getCause();
                            assertTrue(
                                    cause.getMessage().contains("requires separate PayloadStore"),
                                    cause.toString());
                        });
    }
}
