package com.playrewards.flowengine.core.node.structure;

import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.core.execution.CleanupBudget;
import com.playrewards.flowengine.core.execution.ExecutionDeadlineExceededException;
import com.playrewards.flowengine.core.node.NodeExecutionContext;
import com.playrewards.flowengine.core.node.NodeExecutionException;
import com.playrewards.flowengine.core.node.NodeExecutor;
import com.playrewards.flowengine.core.node.NodeInvocation;
import com.playrewards.flowengine.core.node.NodeResult;
import com.playrewards.flowengine.spi.DistributedLockProvider;
import com.playrewards.flowengine.spi.LockLease;
import com.playrewards.flowengine.spi.LockRequest;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Evaluates safe lock identities and performs finite, cancellable lease acquisition/release. */
public final class LockNodeExecutor implements NodeExecutor {

    private static final long DEFAULT_LEASE_MILLIS = 30_000;
    private static final long WAIT_POLL_MILLIS = 10;

    private final ScriptRuntime scripts;
    private final DistributedLockProvider locks;
    private final Clock clock;

    /** Creates a LOCK executor with the host's script sandbox, lease provider and time source. */
    public LockNodeExecutor(ScriptRuntime scripts, DistributedLockProvider locks, Clock clock) {
        this.scripts = Objects.requireNonNull(scripts, "script runtime must not be null");
        this.locks = Objects.requireNonNull(locks, "lock provider must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public ComponentType supports() {
        return ComponentType.LOCK;
    }

    /** LOCK scheduling requires scheduler-owned lease state and cannot execute as an isolated leaf. */
    @Override
    public NodeInvocation execute(NodeExecutionContext context) {
        throw new NodeExecutionException("LOCK_COORDINATOR_REQUIRED",
                "LOCK must execute through the scheduler lease coordinator");
    }

    /** Acquires one finite lease; the caller must register cleanup before scheduling guarded work. */
    public LockLease acquire(NodeExecutionContext context) {
        context.throwIfStopped();
        String expression = text(context.node().node().config().get("lockKey"));
        if (expression == null) {
            throw new NodeExecutionException("LOCK_CONFIG_INVALID", "LOCK lockKey must be non-blank");
        }
        Object rawKey = awaitScript(context, expression);
        if (!(rawKey instanceof String key) || key.isBlank()) {
            throw new NodeExecutionException("LOCK_KEY_INVALID", "LOCK key expression must return a string");
        }
        long waitMillis = nonNegative(context, "waitMillis", 0);
        long configuredLease = nonNegative(context, "leaseMillis", DEFAULT_LEASE_MILLIS);
        if (configuredLease == 0) {
            configuredLease = DEFAULT_LEASE_MILLIS;
        }
        String lockName = scopedName(context, key);
        String ownerId = context.executionId() + ":" + context.node().node().id() + ":" + context.scopePath().value();
        Instant waitDeadline = min(context.request().deadline(), clock.instant().plusMillis(waitMillis));
        boolean singleAttempt = waitMillis == 0;
        do {
            context.throwIfStopped();
            long leaseMillis = Math.min(configuredLease,
                    Math.max(1, remainingMillis(context.request().deadline())));
            Optional<LockLease> acquired = locks.tryAcquire(
                    new LockRequest(lockName, ownerId, Duration.ofMillis(leaseMillis)));
            if (acquired.isPresent()) {
                return acquired.get();
            }
            if (singleAttempt || !clock.instant().isBefore(waitDeadline)) {
                break;
            }
            try {
                Thread.sleep(Math.min(WAIT_POLL_MILLIS, Math.max(1, remainingMillis(waitDeadline))));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                context.throwIfStopped();
                throw new java.util.concurrent.CancellationException("lock wait interrupted");
            }
        } while (true);
        throw new NodeExecutionException("LOCK_ACQUIRE_TIMEOUT", "LOCK could not be acquired within its wait limit");
    }

    /** Releases and validates the exact active fencing capability. */
    public void release(LockLease lease) {
        boolean unexpired = lease.expiresAt().isAfter(clock.instant());
        boolean released = locks.release(lease);
        if (!unexpired || !released) {
            throw new NodeExecutionException("LOCK_LEASE_LOST",
                    "LOCK lease is expired or no longer owned by this execution");
        }
    }

    /** Best-effort exceptional cleanup that never exposes the original key. */
    public boolean cleanup(LockLease lease) {
        try {
            return locks.release(lease);
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    /** @return standard no-output continuation after acquisition or release */
    public NodeResult continued() {
        return NodeResult.continueWith(Map.of(), Set.of("NEXT"));
    }

    private Object awaitScript(NodeExecutionContext context, String expression) {
        CompletionStage<Object> source = scripts.execute(new ScriptRuntime.ScriptInvocation(expression,
                context.externalScopes(), context.ctxView(), context.executionLogger(),
                context.cancellationToken(), context.request().deadline()));
        CompletableFuture<Object> future = new CompletableFuture<>();
        java.util.concurrent.CountDownLatch sourceDone = new java.util.concurrent.CountDownLatch(1);
        source.whenComplete((value, failure) -> {
            sourceDone.countDown();
            if (failure == null) {
                future.complete(value);
            } else {
                future.completeExceptionally(failure);
            }
        });
        try {
            return future.get(Math.max(1, remainingMillis(context.request().deadline())), TimeUnit.MILLISECONDS);
        } catch (InterruptedException failure) {
            cancelSource(source, future);
            RuntimeException selected = stoppedFailure(context,
                    new java.util.concurrent.CancellationException("lock key evaluation interrupted"));
            awaitSourceCleanup(sourceDone, context, selected);
            Thread.currentThread().interrupt();
            throw selected;
        } catch (TimeoutException failure) {
            cancelSource(source, future);
            RuntimeException selected = stoppedFailure(context,
                    new ExecutionDeadlineExceededException("execution deadline exceeded", failure));
            awaitSourceCleanup(sourceDone, context, selected);
            throw selected;
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new NodeExecutionException("LOCK_KEY_EVALUATION_FAILED", "LOCK key evaluation failed");
        }
    }

    private void awaitSourceCleanup(java.util.concurrent.CountDownLatch sourceDone,
            NodeExecutionContext context, Throwable selectedFailure) {
        boolean interrupted = false;
        boolean cleaned = false;
        CleanupBudget budget = CleanupBudget.standard();
        try {
            while (budget.remainingNanos() > 0) {
                try {
                    cleaned = sourceDone.await(budget.remainingNanos(), TimeUnit.NANOSECONDS);
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                    // Cancellation can interrupt this worker repeatedly; the source still owns cleanup.
                }
            }
        } finally {
            if (!cleaned) {
                selectedFailure.addSuppressed(new IllegalStateException(
                        "LOCK key source cleanup did not finish before the deadline"));
                try {
                    context.executionLogger().error("LOCK key source cleanup timed out", selectedFailure,
                            Map.of("executionId", context.executionId(),
                                    "nodeId", context.node().node().id(),
                                    "scopePath", context.scopePath().value()));
                } catch (RuntimeException ignored) {
                    // Observation failure must not replace cancellation or deadline semantics.
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static RuntimeException stoppedFailure(
            NodeExecutionContext context, RuntimeException fallback) {
        try {
            context.throwIfStopped();
            return fallback;
        } catch (RuntimeException selected) {
            return selected;
        }
    }

    private static void cancelSource(CompletionStage<?> source, CompletableFuture<?> bridge) {
        if (source instanceof Future<?> cancellable) {
            cancellable.cancel(true);
        }
        bridge.cancel(true);
    }

    private long remainingMillis(Instant deadline) {
        return Math.max(0, clock.instant().until(deadline, ChronoUnit.MILLIS));
    }

    private static long nonNegative(NodeExecutionContext context, String field, long defaultValue) {
        Object value = context.node().node().config().get(field);
        if (value == null) {
            return defaultValue;
        }
        if (!(value instanceof Number number) || number.longValue() < 0
                || number.doubleValue() != Math.rint(number.doubleValue())) {
            throw new NodeExecutionException("LOCK_CONFIG_INVALID", "LOCK " + field + " must be non-negative");
        }
        return number.longValue();
    }

    private static String scopedName(NodeExecutionContext context, String rawKey) {
        return "tenant:" + encode(context.request().tenantId()) + ":flow:"
                + encode(context.flowIdentity().flowId()) + ":version:" + encode(context.flowIdentity().version())
                + ":key:" + digest(rawKey);
    }

    private static String encode(String value) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static Instant min(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private static String text(Object value) {
        return value instanceof String text && !text.isBlank() ? text.strip() : null;
    }
}
