package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.spi.CancellationToken;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Thread-safe cancellation source exposed as the narrower SPI token to running nodes. */
public final class CancellationHandle implements CancellationToken {

    /** Distinguishes explicit user cancellation from deadline-triggered child cancellation. */
    public enum Reason {
        USER,
        DEADLINE,
        FAILURE,
        TERMINAL
    }

    private final AtomicReference<Reason> reason = new AtomicReference<>();
    private final CopyOnWriteArrayList<Runnable> callbacks = new CopyOnWriteArrayList<>();

    /** Requests explicit cancellation once and invokes registered interruption callbacks. */
    public boolean cancel() {
        return cancel(Reason.USER);
    }

    /** @return the winning cancellation reason, or {@code null} while active */
    public Reason reason() {
        return reason.get();
    }

    @Override
    public boolean isCancellationRequested() {
        return reason.get() != null;
    }

    /** Requests deadline cancellation once so every child observes the same terminal cause. */
    public boolean cancelForDeadline() {
        return cancel(Reason.DEADLINE);
    }

    /** Requests sibling-task cancellation after a node failure without changing that failure's code. */
    public boolean cancelForFailure() {
        return cancel(Reason.FAILURE);
    }

    /** Stops sibling work after END has established the execution's terminal outcome. */
    boolean cancelForTerminal() {
        return cancel(Reason.TERMINAL);
    }

    /**
     * Registers an interruption callback, invoking it immediately after a prior stop.
     *
     * @return idempotent registration which removes the callback after normal child completion
     */
    CancellationRegistration onCancellation(Runnable callback) {
        if (isCancellationRequested()) {
            callback.run();
            return CancellationRegistration.closed();
        }
        callbacks.add(callback);
        if (isCancellationRequested() && callbacks.remove(callback)) {
            callback.run();
            return CancellationRegistration.closed();
        }
        return new CancellationRegistration(callbacks, callback);
    }

    private boolean cancel(Reason requestedReason) {
        if (!reason.compareAndSet(null, requestedReason)) {
            return false;
        }
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (RuntimeException ignored) {
                // One misbehaving adapter must not prevent cancellation from reaching its siblings.
            }
        }
        callbacks.clear();
        return true;
    }

    /** Idempotent callback registration owned by the task that created it. */
    static final class CancellationRegistration implements AutoCloseable {

        private final CopyOnWriteArrayList<Runnable> callbacks;
        private final Runnable callback;
        private final AtomicBoolean open;

        private CancellationRegistration(CopyOnWriteArrayList<Runnable> callbacks, Runnable callback) {
            this.callbacks = callbacks;
            this.callback = callback;
            this.open = new AtomicBoolean(true);
        }

        private CancellationRegistration() {
            this.callbacks = null;
            this.callback = null;
            this.open = new AtomicBoolean(false);
        }

        private static CancellationRegistration closed() {
            return new CancellationRegistration();
        }

        @Override
        public void close() {
            if (open.compareAndSet(true, false)) {
                callbacks.remove(callback);
            }
        }
    }
}
