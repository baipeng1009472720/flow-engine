package com.playrewards.flowengine.server.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.playrewards.flowengine.adapter.mysql.MySqlTenantStoreRouter;
import com.playrewards.flowengine.core.execution.FlowEngine;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Verifies that host lifecycle callbacks cannot leak duplicate recovery scanners. */
class DurableRecoveryLifecycleTest {

    @Test
    void repeatedStartKeepsTheOriginalRecoveryWorker() throws Exception {
        var lifecycle = new DurableRecoveryLifecycle(null, null);
        try {
            lifecycle.start();
            ScheduledExecutorService first = worker(lifecycle);
            lifecycle.start();
            assertSame(first, worker(lifecycle));
        } finally {
            lifecycle.stop();
        }
    }

    @Test
    void stoppedWorkerCannotResumeCandidatesAfterAReplacementStarts() throws Exception {
        var stores = mock(MySqlTenantStoreRouter.class);
        var engine = mock(FlowEngine.class);
        var firstScanEntered = new CountDownLatch(1);
        var releaseFirstScan = new CountDownLatch(1);
        var firstScanReturned = new CountDownLatch(1);
        var resumed = new CountDownLatch(1);
        var scans = new AtomicInteger();
        var candidate = new MySqlTenantStoreRouter.RecoveryCandidate("tenant", "execution");
        when(stores.recoverable(100))
                .thenAnswer(
                        ignored -> {
                            if (scans.incrementAndGet() == 1) {
                                firstScanEntered.countDown();
                                awaitIgnoringInterruption(releaseFirstScan);
                                firstScanReturned.countDown();
                            }
                            return List.of(candidate);
                        });
        when(engine.resume("execution", "tenant"))
                .thenAnswer(
                        ignored -> {
                            resumed.countDown();
                            return null;
                        });

        var lifecycle = new DurableRecoveryLifecycle(stores, engine);
        try {
            lifecycle.start();
            assertTrue(firstScanEntered.await(2, TimeUnit.SECONDS));
            lifecycle.stop();
            lifecycle.start();
            assertTrue(resumed.await(2, TimeUnit.SECONDS));
            releaseFirstScan.countDown();
            assertTrue(firstScanReturned.await(2, TimeUnit.SECONDS));
            Thread.sleep(100);
            verify(engine, times(1)).resume("execution", "tenant");
        } finally {
            releaseFirstScan.countDown();
            lifecycle.stop();
        }
    }

    private static void awaitIgnoringInterruption(CountDownLatch latch) {
        boolean interrupted = false;
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private static ScheduledExecutorService worker(DurableRecoveryLifecycle lifecycle)
            throws ReflectiveOperationException {
        Field worker = DurableRecoveryLifecycle.class.getDeclaredField("worker");
        worker.setAccessible(true);
        return (ScheduledExecutorService) worker.get(lifecycle);
    }
}
