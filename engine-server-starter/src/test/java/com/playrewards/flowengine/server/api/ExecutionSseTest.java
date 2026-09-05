package com.playrewards.flowengine.server.api;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.adapter.memory.InMemoryObservationStore;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.spi.ExecutionRecord;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

/**
 * Deterministic replay ordering, cursor ownership and slow subscriber tests without timing
 * assumptions.
 */
class ExecutionSseTest {
    @Test
    void reconnects_from_a_retained_cursor_after_earlier_events_expired() throws Exception {
        var store =
                new InMemoryObservationStore(
                        1, 16, java.time.Duration.ofHours(1), java.time.Clock.systemUTC());
        store.saveExecution(
                new ExecutionRecord(
                        "run",
                        "run",
                        null,
                        new FlowIdentity("tenant", "flow", "v1"),
                        "hash",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        Instant.now(),
                        null,
                        null,
                        null,
                        null,
                        -1));
        var bus = new BoundedExecutionEventBus(store, 1, 2);
        for (int n = 0; n < 20; n++) bus.publish(event(n));
        try (var subscription = bus.subscribe("run", "run:17")) {
            assertEquals(18, subscription.poll(1).sequence());
            assertEquals(19, subscription.poll(1).sequence());
        }
        assertThrows(IllegalArgumentException.class, () -> bus.subscribe("run", "run:1"));
    }

    @Test
    void replays_then_delivers_live_without_duplicates_and_detaches_slow_subscribers()
            throws Exception {
        var store = new InMemoryObservationStore();
        store.saveExecution(
                new ExecutionRecord(
                        "run",
                        "run",
                        null,
                        new FlowIdentity("tenant", "flow", "v1"),
                        "hash",
                        ExecutionMode.EPHEMERAL,
                        ExecutionPurpose.TRIAL,
                        ExecutionStatus.RUNNING,
                        Instant.now(),
                        null,
                        null,
                        null,
                        null,
                        -1));
        var bus = new BoundedExecutionEventBus(store, 2, 2);
        bus.publish(event(0));
        bus.publish(event(1));
        try (var subscription = bus.subscribe("run", "run:0")) {
            bus.publish(event(2));
            assertEquals(1, subscription.poll(1).sequence());
            assertEquals(2, subscription.poll(1).sequence());
            assertNull(subscription.poll(1));
            bus.publish(event(3));
            bus.publish(event(4));
            bus.publish(event(5));
            assertTrue(subscription.closed());
            assertEquals("SLOW_SUBSCRIBER", subscription.failure());
        }
        assertThrows(IllegalArgumentException.class, () -> bus.subscribe("run", "another:0"));
        assertThrows(IllegalArgumentException.class, () -> bus.subscribe("run", "run:999"));
    }

    private static ExecutionEvent event(long seq) {
        return new ExecutionEvent(
                "run:" + seq,
                "run",
                "STARTED",
                ExecutionStatus.RUNNING,
                null,
                null,
                null,
                0,
                seq,
                Instant.now(),
                Map.of());
    }
}
