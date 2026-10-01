package com.playrewards.flowengine.adapter.mysql;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * 观察记录写后缓冲：按执行划分有序通道，后台少量写线程把同一通道内积累的写入合并为一个事务提交。
 *
 * <p>顺序：同一执行的写入只进入同一通道，通道同一时刻只被一个线程排空（后台线程或读前刷新的调用线程），
 * 取出顺序即提交顺序；不同执行互不等待，不再按租户串行。
 *
 * <p>有界与背压：全局待写条数超过 {@code maxPending} 时，提交者在自身线程同步排空自己的通道，
 * 不丢弃任何写入，也不无限占用内存。
 *
 * <p>失败：整批失败后逐条重放以隔离失败项，首个失败按执行记录，由 {@link #flush} 抛给终态路径；
 * 非严格审计时记录器只告警，严格审计时执行转为 {@code OBSERVATION_AUDIT_FAILED}。
 *
 * <p>关闭：{@link #close} 停止接受异步写入（之后的提交在调用线程同步落库）并排空全部通道。
 * 进程被强制杀死时最多丢失尚未提交的诊断写入；终态记录与终态事件始终由调用方同步提交，不经过本缓冲。
 */
final class ObservationWriteBehind implements AutoCloseable {
    private final BiConsumer<String, List<ObservationWrite>> sink;
    private final int maxPending;
    private final int maxBatch;
    private final ConcurrentHashMap<String, Lane> lanes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RuntimeException> failures = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<Lane> ready = new LinkedBlockingQueue<>();
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicLong failed = new AtomicLong();
    private final List<Thread> writers = new ArrayList<>();
    private volatile boolean closed;

    /**
     * @param sink 在一个事务内应用同一租户、同一执行的一批写入；抛出异常表示整批回滚
     * @param writers 后台写线程数，同时也是后台占用的最大数据库连接数
     * @param maxPending 全局待写上限，超过后提交者同步排空
     * @param maxBatch 单个事务最多合并的写入条数
     */
    ObservationWriteBehind(
            BiConsumer<String, List<ObservationWrite>> sink,
            int writers,
            int maxPending,
            int maxBatch) {
        if (writers < 1 || maxPending < 1 || maxBatch < 1)
            throw new IllegalArgumentException("write-behind bounds invalid");
        this.sink = Objects.requireNonNull(sink);
        this.maxPending = maxPending;
        this.maxBatch = maxBatch;
        for (int index = 0; index < writers; index++) {
            Thread writer = new Thread(this::work, "flow-observation-writer-" + index);
            writer.setDaemon(true);
            writer.start();
            this.writers.add(writer);
        }
    }

    /**
     * 追加一条写入到所属执行的通道；关闭后在调用线程同步落库。
     *
     * @param tenant 已解析的所属租户
     * @param write 不可变写入
     */
    void submit(String tenant, ObservationWrite write) {
        String id = write.executionId();
        if (closed) {
            drain(id);
            apply(tenant, id, List.of(write));
            return;
        }
        Lane lane =
                lanes.compute(
                        id,
                        (key, current) -> {
                            Lane target = current == null ? new Lane(id, tenant) : current;
                            synchronized (target) {
                                target.queue.add(write);
                                if (!target.scheduled) {
                                    target.scheduled = true;
                                    ready.add(target);
                                }
                            }
                            return target;
                        });
        if (pending.incrementAndGet() > maxPending) drain(lane);
    }

    /** 同步排空一个执行的通道；失败保留到 {@link #flush}。读前刷新与终态提交前调用。 */
    void drain(String executionId) {
        Lane lane = lanes.get(executionId);
        if (lane != null) drain(lane);
    }

    /**
     * 排空并报告一个执行自上次 flush 以来的首个异步写入失败。
     *
     * @throws IllegalStateException 该执行有写入未能落库
     */
    void flush(String executionId) {
        drain(executionId);
        RuntimeException failure = failures.remove(executionId);
        if (failure != null) throw failure;
    }

    /** 终态已提交后释放该执行的失败记录。 */
    void complete(String executionId) {
        drain(executionId);
        failures.remove(executionId);
    }

    /** @return 当前全局待写条数（无身份信息，供监控采样） */
    int pendingWrites() {
        return pending.get();
    }

    /** @return 累计未能落库的写入条数 */
    long failedWrites() {
        return failed.get();
    }

    private void work() {
        while (!closed || !ready.isEmpty()) {
            try {
                Lane lane = ready.poll(200, TimeUnit.MILLISECONDS);
                if (lane != null) drain(lane);
            } catch (InterruptedException stop) {
                if (closed) return;
            } catch (RuntimeException unexpected) {
                failed.incrementAndGet();
            }
        }
    }

    private void drain(Lane lane) {
        lane.lock.lock();
        try {
            while (true) {
                List<ObservationWrite> batch = new ArrayList<>();
                synchronized (lane) {
                    while (batch.size() < maxBatch && !lane.queue.isEmpty())
                        batch.add(lane.queue.poll());
                    if (batch.isEmpty()) {
                        lane.scheduled = false;
                        break;
                    }
                }
                try {
                    apply(lane.tenant, lane.executionId, batch);
                } finally {
                    pending.addAndGet(-batch.size());
                }
            }
        } finally {
            lane.lock.unlock();
        }
        // 只移除确实空闲的通道；compute 与 submit 共用同一桶锁，避免移除后丢失并发追加的写入。
        lanes.computeIfPresent(
                lane.executionId,
                (key, current) -> {
                    if (current != lane) return current;
                    synchronized (current) {
                        return current.queue.isEmpty() && !current.scheduled ? null : current;
                    }
                });
    }

    private void apply(String tenant, String executionId, List<ObservationWrite> batch) {
        try {
            sink.accept(tenant, batch);
        } catch (RuntimeException batchFailure) {
            if (batch.size() == 1) {
                record(executionId, batchFailure);
                return;
            }
            // 整批回滚后逐条重放：只丢弃真正失败的写入，不让一条日志拖累同批的节点状态。
            for (var write : batch) {
                try {
                    sink.accept(tenant, List.of(write));
                } catch (RuntimeException failure) {
                    record(executionId, failure);
                }
            }
        }
    }

    private void record(String executionId, RuntimeException failure) {
        failed.incrementAndGet();
        failures.putIfAbsent(
                executionId,
                failure instanceof IllegalStateException || failure instanceof IllegalArgumentException
                        ? failure
                        : new IllegalStateException("OBSERVATION_WRITE_FAILED"));
    }

    @Override
    public void close() {
        closed = true;
        for (Lane lane : List.copyOf(lanes.values())) drain(lane);
        for (Thread writer : writers) writer.interrupt();
        for (Thread writer : writers) {
            try {
                writer.join(5000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 单个执行的有序写入通道。 */
    private static final class Lane {
        private final String executionId;
        private final String tenant;
        private final ArrayDeque<ObservationWrite> queue = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();
        private boolean scheduled;

        private Lane(String executionId, String tenant) {
            this.executionId = executionId;
            this.tenant = tenant;
        }
    }
}
