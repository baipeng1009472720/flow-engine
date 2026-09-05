package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.adapter.memory.InMemoryObservationStore;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.server.config.EngineAutoConfiguration;
import com.playrewards.flowengine.spi.*;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Instant;
import java.util.Map;

/** Host properties must bound actual storage, not merely document inactive defaults. */
class ObservationLimitsTest {
    @Test
    void diagnostic_kinds_expire_independently_after_completion_but_never_while_active() {
        var clock = new MutableClock();
        var hour = java.time.Duration.ofHours(1);
        var limits =
                new InMemoryObservationStore.Limits(
                        2,
                        16,
                        16,
                        16,
                        16,
                        16,
                        1024,
                        1024,
                        1024,
                        hour,
                        java.time.Duration.ofSeconds(3),
                        java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(2));
        var store = new InMemoryObservationStore(limits, clock);
        store.saveExecution(record("a", null));
        store.saveNode(
                new NodeExecutionRecord(
                        "node",
                        "a",
                        "node",
                        "node",
                        com.playrewards.flowengine.contract.component.ComponentType.START,
                        "root",
                        1,
                        ExecutionStatus.RUNNING,
                        clock.instant(),
                        clock.instant(),
                        null,
                        null,
                        null,
                        null,
                        null));
        store.appendLog(
                new ExecutionLogRecord(
                        "log",
                        "a",
                        "node",
                        clock.instant(),
                        ExecutionLogLevel.INFO,
                        "engine",
                        0,
                        "safe",
                        Map.of(),
                        null));
        store.append(
                new ExecutionEvent(
                        "a:0",
                        "a",
                        "STARTED",
                        ExecutionStatus.RUNNING,
                        null,
                        null,
                        null,
                        0,
                        0,
                        clock.instant(),
                        Map.of()));
        var payload = store.store("a", "value");
        clock.now = clock.now.plusSeconds(20);
        assertEquals(1, store.logs("node").size());
        assertTrue(store.find(payload).isPresent());
        assertEquals(1, store.readAfter("a", -1).size());
        store.saveExecution(record("a", clock.instant()));
        clock.now = clock.now.plusMillis(1500);
        assertTrue(store.logs("node").isEmpty());
        assertTrue(store.find(payload).isPresent());
        assertEquals(1, store.readAfter("a", -1).size());
        clock.now = clock.now.plusSeconds(1);
        assertTrue(store.find(payload).isEmpty());
        assertEquals(1, store.readAfter("a", -1).size());
        clock.now = clock.now.plusSeconds(1);
        assertThrows(IllegalArgumentException.class, () -> store.readAfter("a", -1));
        assertTrue(store.execution("a").isPresent());
    }

    private static final class MutableClock extends java.time.Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        public Instant instant() {
            return now;
        }

        public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        public java.time.Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    @Test
    void binds_independent_counts_and_payload_byte_limits() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(EngineAutoConfiguration.class))
                .withPropertyValues(
                        "flow-engine.observation.max-executions=1",
                        "flow-engine.observation.events.max-count=2",
                        "flow-engine.observation.logs.max-count=2",
                        "flow-engine.payload.max-bytes=256")
                .run(
                        context -> {
                            assertNull(context.getStartupFailure());
                            var store = context.getBean(InMemoryObservationStore.class);
                            store.saveExecution(record("a", null));
                            assertThrows(
                                    IllegalStateException.class,
                                    () -> store.saveExecution(record("b", null)));
                            assertThrows(
                                    IllegalArgumentException.class,
                                    () -> store.store("a", "x".repeat(1024)));
                            for (int n = 0; n < 4; n++)
                                store.append(
                                        new ExecutionEvent(
                                                "a:" + n,
                                                "a",
                                                "STARTED",
                                                ExecutionStatus.RUNNING,
                                                null,
                                                null,
                                                null,
                                                0,
                                                n,
                                                Instant.now(),
                                                Map.of()));
                            assertThrows(
                                    IllegalArgumentException.class, () -> store.readAfter("a", -1));
                            assertEquals(2, store.readAfter("a", 1).size());
                            store.saveNode(
                                    new NodeExecutionRecord(
                                            "node",
                                            "a",
                                            "node",
                                            "node",
                                            com.playrewards.flowengine.contract.component
                                                    .ComponentType.START,
                                            "root",
                                            1,
                                            ExecutionStatus.RUNNING,
                                            Instant.now(),
                                            Instant.now(),
                                            null,
                                            null,
                                            null,
                                            null,
                                            null));
                            for (int n = 0; n < 3; n++)
                                store.appendLog(
                                        new ExecutionLogRecord(
                                                "log" + n,
                                                "a",
                                                "node",
                                                Instant.now(),
                                                ExecutionLogLevel.INFO,
                                                "engine",
                                                n,
                                                "safe",
                                                Map.of(),
                                                null));
                            assertEquals(2, store.logs("node").size());
                        });
    }

    static ExecutionRecord record(String id, Instant finished) {
        return new ExecutionRecord(
                id,
                id,
                null,
                new FlowIdentity("tenant", "flow", "v1"),
                "hash",
                ExecutionMode.EPHEMERAL,
                ExecutionPurpose.TRIAL,
                finished == null ? ExecutionStatus.RUNNING : ExecutionStatus.SUCCEEDED,
                Instant.now(),
                finished,
                null,
                null,
                null,
                0);
    }
}
