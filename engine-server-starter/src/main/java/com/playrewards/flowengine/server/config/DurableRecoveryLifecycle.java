package com.playrewards.flowengine.server.config;

import com.playrewards.flowengine.adapter.mysql.MySqlTenantStoreRouter;
import com.playrewards.flowengine.core.execution.FlowEngine;

import org.springframework.context.SmartLifecycle;

import java.util.concurrent.*;

/** Bounded repeated strong-state scan allows takeover after a dead owner's lease expires. */
public final class DurableRecoveryLifecycle implements SmartLifecycle {
    private final MySqlTenantStoreRouter stores;
    private final FlowEngine engine;
    private ScheduledExecutorService worker;
    private volatile boolean running;
    private volatile long generation;

    /** Creates the same recovery scanner used by host auto-configuration and process probes. */
    public DurableRecoveryLifecycle(MySqlTenantStoreRouter stores, FlowEngine engine) {
        this.stores = stores;
        this.engine = engine;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        long workerGeneration = ++generation;
        worker =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "flow-durable-recovery");
                            t.setDaemon(true);
                            return t;
                        });
        worker.scheduleWithFixedDelay(
                () -> recover(workerGeneration), 0, 1, TimeUnit.SECONDS);
    }

    private void recover(long workerGeneration) {
        try {
            for (var candidate : stores.recoverable(100)) {
                if (!isCurrent(workerGeneration)) return;
                try {
                    engine.resume(candidate.executionId(), candidate.tenantId());
                } catch (IllegalStateException busy) {
                    // Live local/remote ownership is expected while scanning; no duplicate
                    // scheduler starts.
                    if (!"DURABLE_EXECUTION_LEASE_BUSY".equals(busy.getMessage())
                            && !"DURABLE_EXECUTION_ALREADY_ACTIVE".equals(busy.getMessage()))
                        System.getLogger(getClass().getName())
                                .log(
                                        System.Logger.Level.WARNING,
                                        "Durable recovery could not start");
                }
            }
        } catch (RuntimeException failure) {
            if (isCurrent(workerGeneration))
                System.getLogger(getClass().getName())
                        .log(System.Logger.Level.WARNING, "Durable recovery scan failed");
        }
    }

    private boolean isCurrent(long workerGeneration) {
        return running && generation == workerGeneration;
    }

    public synchronized void stop() {
        running = false;
        generation++;
        if (worker != null) {
            worker.shutdownNow();
            worker = null;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
