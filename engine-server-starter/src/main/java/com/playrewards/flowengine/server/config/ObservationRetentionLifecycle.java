package com.playrewards.flowengine.server.config;

import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * 观察记录保留清理的宿主生命周期：单个守护线程按固定间隔调用一次分批清理。
 *
 * <p>单次清理失败只记录无身份信息的告警并等待下一个周期，不中断调度；停止时取消后续周期。
 * 不负责：清理范围与批量大小（由 {@code MySqlObservationRetention} 决定），也不在关闭前强制清理。
 */
public final class ObservationRetentionLifecycle implements SmartLifecycle {
    private final Runnable purge;
    private final Duration interval;
    private ScheduledExecutorService worker;
    private volatile boolean running;

    /**
     * @param purge 一次有界清理动作
     * @param interval 两次清理之间的固定延迟，必须为正
     */
    public ObservationRetentionLifecycle(Runnable purge, Duration interval) {
        this.purge = Objects.requireNonNull(purge);
        if (interval.isZero() || interval.isNegative())
            throw new IllegalArgumentException("cleanup interval must be positive");
        this.interval = interval;
    }

    @Override
    public synchronized void start() {
        if (running) return;
        running = true;
        worker =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "flow-observation-retention");
                            t.setDaemon(true);
                            return t;
                        });
        worker.scheduleWithFixedDelay(
                this::purgeOnce, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void purgeOnce() {
        try {
            purge.run();
        } catch (RuntimeException failure) {
            // 不输出异常文本：存储异常可能包含库名等部署信息；下一周期会重试。
            System.getLogger(getClass().getName())
                    .log(System.Logger.Level.WARNING, "Observation retention cleanup failed");
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (worker != null) {
            worker.shutdownNow();
            worker = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
