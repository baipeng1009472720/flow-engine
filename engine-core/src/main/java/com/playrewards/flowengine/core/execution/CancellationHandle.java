package com.playrewards.flowengine.core.execution;

import com.playrewards.flowengine.spi.CancellationToken;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import com.playrewards.flowengine.spi.HttpNodeClient;

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
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final CopyOnWriteArrayList<Runnable> callbacks = new CopyOnWriteArrayList<>();
    private final CancellationHandle root;
    private final CancellationRegistration parentRegistration;
    private final ConcurrentHashMap<String, Boolean> unresolvedSideEffects;

    /** Creates one root cancellation source whose reason participates in public terminal arbitration. */
    public CancellationHandle() {
        this.root = this;
        this.parentRegistration = null;
        this.unresolvedSideEffects = new ConcurrentHashMap<>();
    }

    private CancellationHandle(CancellationHandle parent) {
        this.root = parent.root;
        this.parentRegistration = parent.onCancellation(this::requestStop);
        this.unresolvedSideEffects = root.unresolvedSideEffects;
    }

    /** Requests explicit cancellation once and invokes registered interruption callbacks. */
    public boolean cancel() {
        return cancel(Reason.USER);
    }

    /** @return the winning cancellation reason, or {@code null} while active */
    public Reason reason() {
        return root.reason.get();
    }

    /** Creates an execution-root marker used only for unprotected external side effects. */
    public HttpNodeClient.SendObserver sideEffectObserver(String identity) {
        java.util.Objects.requireNonNull(identity, "side effect identity must not be null");
        return new HttpNodeClient.SendObserver() {
            @Override
            public void sent() {
                unresolvedSideEffects.put(identity, Boolean.TRUE);
            }

            @Override
            public void resolved() {
                unresolvedSideEffects.remove(identity);
            }

            @Override
            public boolean isSentUnresolved() {
                return unresolvedSideEffects.containsKey(identity);
            }

            @Override
            public String toString() {
                return "SendObserver[identity=<redacted>]";
            }
        };
    }

    /** @return whether any unprotected request may have escaped without a definitive outcome */
    boolean hasUnresolvedSideEffect() {
        return !unresolvedSideEffects.isEmpty();
    }

    @Override
    public boolean isCancellationRequested() {
        return stopRequested.get();
    }

    /** Stops current child work before the controller arbitrates the public terminal reason. */
    boolean requestStopWithoutReason() {
        return requestStop();
    }

    /** Creates a region-local source: parent stop flows down, while local fail-fast stays local. */
    CancellationHandle linkedChild() {
        return new CancellationHandle(this);
    }

    /** Removes this child from its parent's callback set after the structured region has finished. */
    void detachFromParent() {
        if (parentRegistration != null) {
            parentRegistration.close();
        }
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
    @Override
    public CancellationRegistration onCancellation(Runnable callback) {
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
        if (root != this) {
            return root.cancel(requestedReason);
        }
        if (!reason.compareAndSet(null, requestedReason)) {
            return false;
        }
        requestStop();
        return true;
    }

    private boolean requestStop() {
        if (!stopRequested.compareAndSet(false, true)) {
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
    public static final class CancellationRegistration implements CancellationToken.CancellationRegistration {

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
