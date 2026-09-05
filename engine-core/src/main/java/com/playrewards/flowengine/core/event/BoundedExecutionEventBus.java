package com.playrewards.flowengine.core.event;

import com.playrewards.flowengine.contract.execution.ExecutionEvent;
import com.playrewards.flowengine.spi.ExecutionObservationStore;

import java.util.*;
import java.util.concurrent.*;

/** Ordered replay plus bounded mailboxes. Publishing never calls a subscriber's network code. */
public final class BoundedExecutionEventBus {
    private final ExecutionObservationStore store;
    private final int maxSubscribers, capacity;
    private final Set<Subscription> subscribers = new HashSet<>();

    public BoundedExecutionEventBus(
            ExecutionObservationStore store, int maxSubscribers, int capacity) {
        if (maxSubscribers < 1 || capacity < 1)
            throw new IllegalArgumentException("invalid event bus limits");
        this.store = store;
        this.maxSubscribers = maxSubscribers;
        this.capacity = capacity;
    }

    /**
     * Persists before publishing. Slow consumers are detached with an explicit reconnect reason.
     */
    public synchronized void publish(ExecutionEvent event) {
        store.append(event);
        deliverCommitted(event);
    }

    /** 仅投递已由原子终态事务提交的事件，不再追加重复事件。 */
    public synchronized void deliverCommitted(ExecutionEvent event) {
        for (var subscription : List.copyOf(subscribers)) {
            if (subscription.executionId.equals(event.executionId())
                    && !subscription.queue.offer(event)) {
                subscription.failure = "SLOW_SUBSCRIBER";
                subscription.close();
            }
        }
    }

    /** Atomically snapshots replay and registers live delivery, preventing gaps and duplicates. */
    public synchronized Subscription subscribe(String executionId, String lastEventId) {
        if (subscribers.size() >= maxSubscribers)
            throw new IllegalStateException("SUBSCRIBER_CAPACITY_EXCEEDED");
        long cursor = -1;
        if (lastEventId != null && !lastEventId.isBlank()) {
            String prefix = executionId + ":";
            if (!lastEventId.startsWith(prefix))
                throw new IllegalArgumentException("EVENT_CURSOR_INVALID");
            try {
                cursor = Long.parseLong(lastEventId.substring(prefix.length()));
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("EVENT_CURSOR_INVALID");
            }
            if (cursor < 0) throw new IllegalArgumentException("EVENT_CURSOR_INVALID");
        }
        var replay = store.readAfter(executionId, cursor < 0 ? -1 : cursor - 1);
        if (cursor >= 0 && (replay.isEmpty() || replay.getFirst().sequence() != cursor))
            throw new IllegalArgumentException("EVENT_CURSOR_INVALID");
        final long after = cursor;
        var subscription =
                new Subscription(
                        executionId, replay.stream().filter(e -> e.sequence() > after).toList());
        subscribers.add(subscription);
        return subscription;
    }

    /** Detached replay is finite; only subsequent live arrivals consume the mailbox budget. */
    public final class Subscription implements AutoCloseable {
        private final String executionId;
        private final ArrayDeque<ExecutionEvent> replay;
        private final ArrayBlockingQueue<ExecutionEvent> queue = new ArrayBlockingQueue<>(capacity);
        private volatile boolean closed;
        private volatile String failure;

        private Subscription(String executionId, List<ExecutionEvent> replay) {
            this.executionId = executionId;
            this.replay = new ArrayDeque<>(replay);
        }

        public ExecutionEvent poll(long millis) throws InterruptedException {
            if (closed) return null;
            var event = replay.poll();
            return event != null ? event : queue.poll(millis, TimeUnit.MILLISECONDS);
        }

        public boolean closed() {
            return closed;
        }

        public String failure() {
            return failure;
        }

        @Override
        public void close() {
            synchronized (BoundedExecutionEventBus.this) {
                closed = true;
                subscribers.remove(this);
                queue.clear();
            }
        }
    }
}
