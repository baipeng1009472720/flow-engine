package com.playrewards.flowengine.core.durable;

import com.alibaba.fastjson2.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.core.node.*;
import com.playrewards.flowengine.spi.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Owns strong recovery documents and fenced execution leases, not graph scheduling. DagScheduler
 * remains the only owner of readiness, loop/subflow boundaries and cancellation. An unfinished
 * unsafe call is deliberately not replayed; no observation log is consulted.
 */
public final class DurableExecutionCoordinator implements AutoCloseable {
    private final Function<String, DurableStores> stores;
    private final IdempotencyStore idempotency;
    private final Duration leaseDuration;
    private final Consumer<ExecutionState> afterCheckpoint;
    private final String owner = UUID.randomUUID().toString();
    private final Map<String, Session> active = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeat =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "flow-execution-lease");
                        t.setDaemon(true);
                        return t;
                    });
    private volatile boolean closed;

    public DurableExecutionCoordinator(
            Function<String, DurableStores> stores,
            IdempotencyStore idempotency,
            Duration leaseDuration) {
        this(stores, idempotency, leaseDuration, state -> {});
    }

    /** The observer runs after commit and may provide bounded host checkpoint instrumentation. */
    public DurableExecutionCoordinator(
            Function<String, DurableStores> stores,
            IdempotencyStore idempotency,
            Duration leaseDuration,
            Consumer<ExecutionState> afterCheckpoint) {
        this.stores = stores;
        this.idempotency = idempotency;
        this.leaseDuration = leaseDuration;
        this.afterCheckpoint = afterCheckpoint;
        long interval = Math.max(20, leaseDuration.toMillis() / 3);
        heartbeat.scheduleWithFixedDelay(this::renew, interval, interval, TimeUnit.MILLISECONDS);
    }

    /** Captures the full immutable plan/request once, before any node side effect. */
    public Session begin(ExecutionPlan plan, ExecutionRequest request) {
        if (!plan.flowIdentity().tenantId().equals(request.tenantId()))
            throw new SecurityException("EXECUTION_TENANT_MISMATCH");
        var key =
                new IdempotencyKey(
                        request.tenantId(),
                        plan.flowIdentity().flowId()
                                + "/"
                                + plan.flowIdentity().version()
                                + "/"
                                + request.idempotencyKey());
        var claim = idempotency.claim(new IdempotencyRecord(key, UUID.randomUUID().toString()));
        return open(claim.executionId(), request.tenantId(), plan, request);
    }

    /** Resumes only explicit strong state; the saved plan and request cannot drift on reload. */
    public Session resume(String id, String tenant) {
        return open(id, tenant, null, null);
    }

    private synchronized Session open(
            String id, String tenant, ExecutionPlan plan, ExecutionRequest request) {
        if (closed) throw new IllegalStateException("DURABLE_COORDINATOR_CLOSED");
        if (active.containsKey(id))
            throw new IllegalStateException("DURABLE_EXECUTION_ALREADY_ACTIVE");
        DurableStores selected = stores.apply(tenant);
        var lease =
                selected.leases()
                        .acquire(id, owner, leaseDuration)
                        .orElseThrow(
                                () -> new IllegalStateException("DURABLE_EXECUTION_LEASE_BUSY"));
        try {
            // Ownership must precede the read: the previous owner may have committed while
            // acquiring.
            var state = selected.states().find(id);
            JSONObject document;
            if (state.isPresent()) {
                Object value =
                        selected.payloads()
                                .find(
                                        new PayloadReference(
                                                (String) state.get().state().get("document")))
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "RECOVERY_PAYLOAD_MISSING"));
                document = JSON.parseObject(JSON.toJSONString(value));
                ExecutionPlan persisted = document.getObject("plan", ExecutionPlan.class);
                if (!tenant.equals(persisted.flowIdentity().tenantId())
                        || (plan != null && !plan.bundleHash().equals(persisted.bundleHash())))
                    throw new SecurityException("RECOVERY_IDENTITY_MISMATCH");
                plan = persisted;
                request = document.getObject("request", ExecutionRequest.class);
            } else {
                if (plan == null) throw new IllegalArgumentException("EXECUTION_NOT_FOUND");
                document = new JSONObject();
                document.put("plan", plan);
                document.put("request", request);
                document.put("completed", new JSONObject());
                document.put("inFlight", new JSONObject());
                document.put("attempts", new JSONObject());
                document.put("contexts", new JSONObject());
                document.put("frames", new JSONObject());
            }
            var session =
                    new Session(
                            id,
                            plan,
                            request,
                            selected,
                            lease,
                            document,
                            state.map(ExecutionState::revision).orElse(0L));
            session.committedStatus =
                    state.map(ExecutionState::status).orElse(ExecutionStatus.RUNNING);
            active.put(id, session);
            if (state.isEmpty()) session.persist(ExecutionStatus.RUNNING, null);
            return session;
        } catch (RuntimeException failure) {
            active.remove(id);
            selected.leases().release(lease);
            throw failure;
        }
    }

    /** Aliases stable child execution identities to the owning root lease/document. */
    public void child(String parent, String child) {
        active.put(child, required(parent));
    }

    public Session session(String id) {
        return active.get(id);
    }

    private Session required(String id) {
        return Objects.requireNonNull(active.get(id), "durable session missing");
    }

    private void renew() {
        for (Session session : new HashSet<>(active.values())) {
            if (session.suspended) continue;
            try {
                if (!session.stores.leases().heartbeat(session.lease, leaseDuration))
                    session.suspended = true;
            } catch (RuntimeException failure) {
                session.suspended = true;
            }
        }
    }

    /**
     * Stops renewal before scheduler interruption; shutdown must not persist cancellation as
     * business outcome.
     */
    public void suspend() {
        closed = true;
        new HashSet<>(active.values()).forEach(s -> s.suspended = true);
        heartbeat.shutdownNow();
    }

    public void close() {
        suspend();
        new HashSet<>(active.values()).forEach(s -> s.stores.leases().release(s.lease));
        active.clear();
    }

    /**
     * One serial checkpoint writer coordinates concurrent branch completions under one root lease.
     */
    public final class Session {
        private final String id;
        private final ExecutionPlan plan;
        private final ExecutionRequest request;
        private final DurableStores stores;
        private final ExecutionLease lease;
        private final JSONObject document;
        private long revision;
        private ExecutionStatus committedStatus = ExecutionStatus.RUNNING;
        private volatile boolean suspended;

        Session(
                String id,
                ExecutionPlan plan,
                ExecutionRequest request,
                DurableStores stores,
                ExecutionLease lease,
                JSONObject document,
                long revision) {
            this.id = id;
            this.plan = plan;
            this.request = request;
            this.stores = stores;
            this.lease = lease;
            this.document = document;
            this.revision = revision;
        }

        public String executionId() {
            return id;
        }

        public ExecutionPlan plan() {
            return plan;
        }

        public ExecutionRequest request() {
            return request;
        }

        public boolean suspended() {
            return suspended;
        }

        public ExecutionLease lease() {
            return lease;
        }

        public synchronized ExecutionResult terminal() {
            return document.getObject("terminal", ExecutionResult.class);
        }

        private JSONObject object(String key) {
            Object value = document.get(key);
            if (value instanceof JSONObject json) return json;
            JSONObject result = JSON.parseObject(JSON.toJSONString(value));
            document.put(key, result);
            return result;
        }

        private String address(String executionId, CompiledNode node, ScopePath scope) {
            return executionId + "/" + node.node().id() + "/" + scope.value();
        }

        public synchronized NodeResult completed(
                String executionId, CompiledNode node, ScopePath scope) {
            if (node.componentContract().componentType() == ComponentType.LOCK) return null;
            return object("completed")
                    .getObject(address(executionId, node, scope), NodeResult.class);
        }

        /**
         * Persists intent before invocation. Unknown SIDE_EFFECT must stop before calling the
         * executor.
         */
        public synchronized void before(String executionId, CompiledNode node, ScopePath scope) {
            ensureActive();
            String key = address(executionId, node, scope);
            String previous = object("inFlight").getString(key);
            if (ComponentEffect.SIDE_EFFECT.name().equals(previous))
                throw new NodeExecutionException(
                        "SIDE_EFFECT_RESULT_UNCERTAIN",
                        "manual confirmation is required",
                        ExecutionStatus.WAITING_MANUAL);
            object("inFlight").put(key, node.componentContract().capabilities().effect().name());
            object("attempts").put(key, object("attempts").getIntValue(key) + 1);
            persist(ExecutionStatus.RUNNING, null);
        }

        /**
         * Commits the exact result before it can activate successors; no script/condition
         * recomputation.
         */
        public synchronized void completed(
                String executionId,
                CompiledNode node,
                ScopePath scope,
                NodeResult result,
                ExecutionContext context) {
            ensureActive();
            String key = address(executionId, node, scope);
            object("completed").put(key, result);
            object("inFlight").remove(key);
            object("contexts").put(executionId, context.snapshot());
            persist(ExecutionStatus.RUNNING, key);
        }

        public synchronized void restore(String executionId, ExecutionContext context) {
            JSONObject snapshot = object("contexts").getJSONObject(executionId);
            if (snapshot != null) context.restore(snapshot);
        }

        /** Persist the next attempt before a retry delay or another external call can begin. */
        public synchronized void retry(
                String executionId,
                CompiledNode node,
                ScopePath scope,
                String code,
                ExecutionContext context) {
            ensureActive();
            String key = address(executionId, node, scope);
            object("attempts").put(key, object("attempts").getIntValue(key) + 1);
            object("contexts").put(executionId, context.snapshot());
            document.put(
                    "retry",
                    Map.of("address", key, "code", code, "recordedAt", Instant.now().toString()));
            persist(ExecutionStatus.WAITING_RETRY, null);
        }

        /**
         * Locks are acquired afresh after recovery; this checkpoint never asserts old lease
         * validity.
         */
        public synchronized void waitingForLock(
                String executionId, CompiledNode node, ScopePath scope) {
            document.put(
                    "lockWait",
                    Map.of(
                            "address",
                            address(executionId, node, scope),
                            "fencingRevalidationRequired",
                            true));
            persist(ExecutionStatus.WAITING_RETRY, null);
        }

        /**
         * Pin loop collection once so an unfinished loop never re-evaluates its collection script.
         */
        public synchronized Object frame(String key) {
            return object("frames").get(key);
        }

        public synchronized void frame(String key, Object value) {
            if (!object("frames").containsKey(key)) {
                object("frames").put(key, value);
                persist(ExecutionStatus.RUNNING, null);
            }
        }

        /**
         * State terminal is independent from audit terminal, whose own atomic store commit follows.
         */
        public synchronized void prepareTerminal(ExecutionResult result) {
            if (suspended) return;
            if (committedStatus == ExecutionStatus.SUCCEEDED
                    || committedStatus == ExecutionStatus.FAILED
                    || committedStatus == ExecutionStatus.CANCELLED) return;
            document.put("terminal", result);
            persist(ExecutionStatus.RUNNING, null);
        }

        public synchronized void finish(ExecutionResult result) {
            if (suspended) return;
            document.put("terminal", result);
            persist(result.status(), null);
            stores.leases().release(lease);
            active.entrySet().removeIf(e -> e.getValue() == this);
        }

        /**
         * Drops local ownership after an audit commit; its prepared terminal remains recoverable.
         */
        public void relinquish() {
            suspended = true;
            active.entrySet().removeIf(e -> e.getValue() == this);
            try {
                stores.leases().release(lease);
            } catch (RuntimeException unavailable) {
                // Failure to release is safe: another owner waits for database lease expiry.
            }
        }

        private void ensureActive() {
            if (suspended || closed)
                throw new NodeExecutionException(
                        "DURABLE_EXECUTION_SUSPENDED", "execution ownership ended");
        }

        private void persist(ExecutionStatus status, String completed) {
            ensureActive();
            PayloadReference payload =
                    stores.payloads()
                            .storeCheckpoint(id, JSON.parseObject(JSON.toJSONString(document)));
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("document", payload.value());
            header.put("formatVersion", 1);
            if (completed != null) header.put("lastCompleted", completed);
            ExecutionState saved =
                    stores.states()
                            .save(
                                    new ExecutionState(id, revision, status, header),
                                    revision,
                                    lease);
            revision = saved.revision();
            committedStatus = status;
            afterCheckpoint.accept(saved);
        }
    }
}
