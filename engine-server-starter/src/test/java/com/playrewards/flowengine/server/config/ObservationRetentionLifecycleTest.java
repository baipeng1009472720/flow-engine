package com.playrewards.flowengine.server.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** 周期清理在单个失败后继续调度，停止后不再执行，避免清理异常导致记录永久堆积。 */
class ObservationRetentionLifecycleTest {

    @Test
    void keepsPurgingAfterAFailureAndStopsCleanly() throws Exception {
        var calls = new AtomicInteger();
        var third = new CountDownLatch(3);
        var lifecycle =
                new ObservationRetentionLifecycle(
                        () -> {
                            third.countDown();
                            if (calls.incrementAndGet() == 1)
                                throw new IllegalStateException("RUNTIME_STORAGE_TRANSACTION_FAILED");
                        },
                        Duration.ofMillis(20));
        lifecycle.start();
        try {
            assertTrue(lifecycle.isRunning());
            assertTrue(third.await(5, TimeUnit.SECONDS));
        } finally {
            lifecycle.stop();
        }
        assertFalse(lifecycle.isRunning());
        int stopped = calls.get();
        Thread.sleep(100);
        assertEquals(stopped, calls.get());
    }
}
