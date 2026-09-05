package com.playrewards.flowengine.core.observation;

import com.playrewards.flowengine.contract.execution.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.core.execution.ScopePath;
import com.playrewards.flowengine.core.node.*;
import com.playrewards.flowengine.spi.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production observation boundary called by real scheduling transitions, never reconstructed from
 * ctx.
 */
public final class ExecutionRecorder {
    private final ExecutionRecordStore store;
    private final PayloadStore payloads;
    private final BoundedExecutionEventBus bus;
    private final Clock clock;
    private final boolean strictAudit;
    private final Map<String, Run> active = new HashMap<>();
    private final AtomicLong warnings = new AtomicLong();
    private java.util.function.Function<String, ExecutionLease> leases = id -> null;

    /**
     * Bound by the composition root; durable terminal writes require the owning root's live fence.
     */
    public void executionLeases(java.util.function.Function<String, ExecutionLease> leases) {
        this.leases = leases;
    }

    private void commitTerminal(ExecutionRecord record, ExecutionEvent event) {
        if (record.mode() == ExecutionMode.DURABLE) {
            ExecutionLease lease = leases.apply(record.executionId());
            if (lease == null) throw new IllegalStateException("DURABLE_EXECUTION_LEASE_REQUIRED");
            store.commitTerminal(record, event, lease);
        } else store.commitTerminal(record, event);
    }

    /**
     * Creates a host-owned observation boundary; strict audit promotes sink failures to execution
     * failure.
     */
    public ExecutionRecorder(
            ExecutionRecordStore store,
            PayloadStore payloads,
            BoundedExecutionEventBus bus,
            Clock clock,
            boolean strictAudit) {
        this.store = store;
        this.payloads = payloads;
        this.bus = bus;
        this.clock = clock;
        this.strictAudit = strictAudit;
    }

    /**
     * Reserves finite observation capacity before a controller or business side effect can start.
     */
    public synchronized void created(
            String id, ExecutionPlan plan, ExecutionRequest request, String parent) {
        if (active.containsKey(id) || store.execution(id).isPresent())
            throw new IllegalStateException("OBSERVATION_EXECUTION_ALREADY_EXISTS");
        String root = parent == null ? id : required(parent).record.rootExecutionId();
        var record =
                new ExecutionRecord(
                        id,
                        root,
                        parent,
                        plan.flowIdentity(),
                        plan.bundleHash(),
                        request.mode(),
                        request.purpose(),
                        ExecutionStatus.CREATED,
                        clock.instant(),
                        null,
                        null,
                        null,
                        null,
                        -1);
        active.put(id, new Run(record));
        try {
            store.saveExecution(record);
            emit(id, null, "CREATED", ExecutionStatus.CREATED, Map.of());
            if (parent != null)
                emit(
                        parent,
                        null,
                        "CHILD_CREATED",
                        ExecutionStatus.RUNNING,
                        Map.of(
                                "childExecutionId",
                                id,
                                "rootExecutionId",
                                root,
                                "flowId",
                                plan.flowIdentity().flowId()));
        } catch (RuntimeException failure) {
            try {
                failClosed(required(id), null);
            } finally {
                active.remove(id);
            }
            throw failure;
        }
    }

    /** A strong pending terminal can complete a missing audit or reuse an already committed one. */
    public synchronized void recoverForTerminal(String id) {
        if (store.execution(id).filter(r -> r.finishedAt() != null).isEmpty()) recover(id);
    }

    /** Records controller admission independently of the first node's queue and permit wait. */
    public synchronized void started(String id) {
        emit(id, null, "STARTED", ExecutionStatus.RUNNING, Map.of());
    }

    /**
     * Reattaches audit identity after strong-state ownership is acquired; events never drive
     * recovery.
     */
    public synchronized boolean recover(String id) {
        if (active.containsKey(id)) return true;
        var record = store.execution(id);
        if (record.isEmpty()) return false;
        var run = new Run(record.get());
        run.sequence =
                Math.max(
                        record.get().eventCursor(),
                        store.readAfter(id, -1).stream()
                                .mapToLong(ExecutionEvent::sequence)
                                .max()
                                .orElse(-1));
        active.put(id, run);
        return true;
    }

    /**
     * Binds immutable runtime identity before input binding, permit acquisition or executor
     * invocation.
     */
    public synchronized Attempt queued(String executionId, CompiledNode node, ScopePath scope) {
        var attempt = new Attempt(executionId, node, scope);
        int number =
                store.nodes(executionId).stream()
                                .filter(
                                        r ->
                                                r.nodeId().equals(node.node().id())
                                                        && r.scopePath().equals(scope.value()))
                                .mapToInt(NodeExecutionRecord::attempt)
                                .max()
                                .orElse(0)
                        + 1;
        attempt.newRecord(number);
        return attempt;
    }

    /**
     * Selects the public root error once and flushes critical records before the completion future.
     */
    public synchronized ExecutionResult finished(ExecutionResult result) {
        Run run = active.get(result.executionId());
        if (run == null) {
            var terminal =
                    store.execution(result.executionId()).filter(r -> r.finishedAt() != null);
            if (terminal.isEmpty())
                throw new IllegalStateException("OBSERVATION_EXECUTION_NOT_ACTIVE");
            var record = terminal.get();
            return new ExecutionResult(
                    record.executionId(),
                    record.status(),
                    record.status() == ExecutionStatus.SUCCEEDED
                                    && record.resultPayloadRef() != null
                            ? payloads.find(new PayloadReference(record.resultPayloadRef()))
                                    .orElse(null)
                            : null,
                    record.errorCode(),
                    record.errorCode() == null ? null : "execution did not complete successfully",
                    record.errorId(),
                    record.startedAt(),
                    record.finishedAt());
        }
        try {
            if (strictAudit && run.auditFailed)
                throw new IllegalStateException("OBSERVATION_AUDIT_FAILED");
            String errorId = result.errorCode() == null ? null : result.errorId();
            if (result.errorCode() != null && errorId == null)
                errorId = UUID.randomUUID().toString();
            var value =
                    new ExecutionResult(
                            result.executionId(),
                            result.status(),
                            result.result(),
                            result.errorCode(),
                            result.errorCode() == null
                                    ? null
                                    : "execution did not complete successfully",
                            errorId,
                            result.startedAt(),
                            result.finishedAt());
            String payload = savePayload(result.executionId(), result.result());
            var r = run.record;
            run.record =
                    new ExecutionRecord(
                            r.executionId(),
                            r.rootExecutionId(),
                            r.parentExecutionId(),
                            r.flowIdentity(),
                            r.bundleHash(),
                            r.mode(),
                            r.purpose(),
                            result.status(),
                            r.startedAt(),
                            result.finishedAt(),
                            payload,
                            errorId,
                            result.errorCode(),
                            r.eventCursor());
            try {
                store.flush(result.executionId());
            } catch (RuntimeException failure) {
                warn(failure);
            }
            var terminal =
                    new ExecutionEvent(
                            result.executionId() + ":" + (++run.sequence),
                            result.executionId(),
                            switch (result.status()) {
                                case SUCCEEDED -> "COMPLETED";
                                case CANCELLED -> "CANCELLED";
                                case WAITING_MANUAL -> "WAITING";
                                default -> "FAILED";
                            },
                            result.status(),
                            null,
                            null,
                            null,
                            0,
                            run.sequence,
                            clock.instant(),
                            errorId == null
                                    ? Map.of()
                                    : Map.of("errorId", errorId, "code", result.errorCode()));
            run.record = terminalRecord(run.record, value, payload, run.sequence);
            synchronized (bus) {
                commitTerminal(run.record, terminal);
                bus.deliverCommitted(terminal);
            }
            forwardCommitted(run.record, terminal);
            return value;
        } catch (RuntimeException failure) {
            return failClosed(run, result);
        } finally {
            active.remove(result.executionId());
        }
    }

    private ExecutionRecord terminalRecord(
            ExecutionRecord record, ExecutionResult result, String payload, long cursor) {
        return new ExecutionRecord(
                record.executionId(),
                record.rootExecutionId(),
                record.parentExecutionId(),
                record.flowIdentity(),
                record.bundleHash(),
                record.mode(),
                record.purpose(),
                result.status(),
                record.startedAt(),
                result.finishedAt(),
                payload,
                result.errorId(),
                result.errorCode(),
                cursor);
    }

    /** 补偿记录带 finishedAt，可被宿主保留策略回收；清理不依赖补偿写入是否成功。 */
    private ExecutionResult failClosed(Run run, ExecutionResult selected) {
        boolean preserveFailure = selected != null && selected.errorCode() != null;
        var result =
                new ExecutionResult(
                        run.record.executionId(),
                        preserveFailure ? selected.status() : ExecutionStatus.FAILED,
                        null,
                        preserveFailure ? selected.errorCode() : "OBSERVATION_AUDIT_FAILED",
                        "critical observation could not be committed",
                        preserveFailure && selected.errorId() != null
                                ? selected.errorId()
                                : UUID.randomUUID().toString(),
                        run.record.startedAt(),
                        clock.instant());
        var record = terminalRecord(run.record, result, null, ++run.sequence);
        var event =
                new ExecutionEvent(
                        record.executionId() + ":" + run.sequence,
                        record.executionId(),
                        "FAILED",
                        result.status(),
                        null,
                        null,
                        null,
                        0,
                        run.sequence,
                        clock.instant(),
                        Map.of("errorId", result.errorId(), "code", result.errorCode()));
        try {
            synchronized (bus) {
                commitTerminal(record, event);
                bus.deliverCommitted(event);
            }
            forwardCommitted(record, event);
        } catch (RuntimeException unavailable) {
            try {
                synchronized (bus) {
                    store.saveExecution(record);
                    bus.publish(event);
                }
                forwardCommitted(record, event);
            } catch (RuntimeException ignored) {
                warnings.incrementAndGet();
            }
        }
        return result;
    }

    /** 子终态已提交后不可回滚；转发失败只污染父观察边界，不修改已提交的子结果。 */
    private void forwardCommitted(ExecutionRecord record, ExecutionEvent event) {
        try {
            forward(
                    record,
                    null,
                    event.eventType(),
                    event.status(),
                    event.sequence(),
                    event.attributes());
        } catch (RuntimeException failure) {
            warnings.incrementAndGet();
            Run parent = active.get(record.parentExecutionId());
            if (parent != null) parent.auditFailed = true;
        }
    }

    /** Returns a monotonic, payload-free count suitable for a host monitoring gauge. */
    public long warningCount() {
        return warnings.get();
    }

    /** 将调度器实际仲裁的根因绑定到结果，不按错误码或观察到达先后猜测失败节点。 */
    public synchronized ExecutionResult identify(ExecutionResult result, Object cause) {
        if (result.errorCode() == null || cause == null) return result;
        return new ExecutionResult(
                result.executionId(),
                result.status(),
                result.result(),
                result.errorCode(),
                result.errorMessage(),
                errorIdentity(result.executionId(), cause),
                result.startedAt(),
                result.finishedAt());
    }

    private String errorIdentity(String executionId, Object cause) {
        Run run = required(executionId);
        Run root = active.getOrDefault(run.record.rootExecutionId(), run);
        return root.causes.computeIfAbsent(cause, ignored -> UUID.randomUUID().toString());
    }

    /** 子 END 转为调用节点异常时显式保留根因，不能因新异常对象而断开错误关联。 */
    public synchronized void inheritCause(String executionId, Object original, Object propagated) {
        var run = required(executionId);
        var root = active.getOrDefault(run.record.rootExecutionId(), run);
        root.causes.put(
                propagated,
                original instanceof ExecutionResult result && result.errorId() != null
                        ? result.errorId()
                        : errorIdentity(executionId, original));
    }

    private Run required(String id) {
        var run = active.get(id);
        if (run == null) throw new IllegalStateException("OBSERVATION_EXECUTION_NOT_ACTIVE");
        return run;
    }

    private void emit(
            String id,
            NodeExecutionRecord node,
            String type,
            ExecutionStatus status,
            Map<String, Object> attributes) {
        Run run = required(id);
        var r = run.record;
        long sequence = ++run.sequence;
        Map<String, Object> attrs = new LinkedHashMap<>(attributes);
        if (node != null) attrs.put("nodeExecutionId", node.nodeExecutionId());
        bus.publish(
                new ExecutionEvent(
                        id + ":" + sequence,
                        id,
                        type,
                        status,
                        node == null ? null : node.nodeId(),
                        node == null ? null : node.nodeCode(),
                        node == null ? null : node.scopePath(),
                        node == null ? 0 : node.attempt(),
                        sequence,
                        clock.instant(),
                        attrs));
        run.record =
                new ExecutionRecord(
                        r.executionId(),
                        r.rootExecutionId(),
                        r.parentExecutionId(),
                        r.flowIdentity(),
                        r.bundleHash(),
                        r.mode(),
                        r.purpose(),
                        node == null ? status : r.status(),
                        r.startedAt(),
                        r.finishedAt(),
                        r.resultPayloadRef(),
                        r.errorId(),
                        r.errorCode(),
                        sequence);
        store.saveExecution(run.record);
        forward(r, node, type, status, sequence, attrs);
    }

    private void forward(
            ExecutionRecord r,
            NodeExecutionRecord node,
            String type,
            ExecutionStatus status,
            long sequence,
            Map<String, Object> attrs) {
        String id = r.executionId();
        // Forward child observations using the parent's cursor without falsifying child identity or
        // status.
        // Nested children remain discoverable in one root stream while retaining their own event
        // tables.
        if (r.parentExecutionId() != null && active.containsKey(r.parentExecutionId())) {
            Map<String, Object> forwarded = new LinkedHashMap<>(attrs);
            forwarded.putIfAbsent("originExecutionId", id);
            forwarded.putIfAbsent("originEventId", id + ":" + sequence);
            forwarded.putIfAbsent("originEventType", type);
            forwarded.putIfAbsent("childExecutionId", id);
            forwarded.putIfAbsent("childEventId", id + ":" + sequence);
            forwarded.putIfAbsent("childEventType", type);
            forwarded.putIfAbsent("childStatus", status.name());
            var path = new ArrayList<Object>();
            if (attrs.get("forwardingPath") instanceof List<?> previous) path.addAll(previous);
            path.add(id);
            forwarded.put("forwardingPath", path);
            emit(
                    r.parentExecutionId(),
                    node,
                    "CHILD_EVENT",
                    required(r.parentExecutionId()).record.status(),
                    forwarded);
        }
    }

    private String savePayload(String id, Object value) {
        if (value == null) return null;
        try {
            return payloads.store(id, value).value();
        } catch (RuntimeException failure) {
            warn(failure);
            return null;
        }
    }

    private void warn(RuntimeException failure) {
        long count = warnings.incrementAndGet();
        // Rate-bounded safe alert: no adapter exception, payload or private message enters stderr.
        if (count == 1 || (count & (count - 1)) == 0)
            System.getLogger(ExecutionRecorder.class.getName())
                    .log(System.Logger.Level.WARNING, "Observation write failed; count=" + count);
        if (strictAudit) throw new IllegalStateException("OBSERVATION_AUDIT_FAILED");
    }

    private static final class Run {
        private ExecutionRecord record;
        private long sequence = -1;
        private final Map<Object, String> causes = new IdentityHashMap<>();
        private boolean auditFailed;

        private Run(ExecutionRecord record) {
            this.record = record;
        }
    }

    /**
     * Mutable attempt cursor belongs to one invocation; retries allocate new identities instead of
     * overwriting history.
     */
    public final class Attempt implements ExecutionLogger {
        private final String executionId;
        private final CompiledNode node;
        private final ScopePath scope;
        private NodeExecutionRecord record;
        private long logSequence, contentSequence;

        private Attempt(String id, CompiledNode node, ScopePath scope) {
            this.executionId = id;
            this.node = node;
            this.scope = scope;
        }

        private void newRecord(int number) {
            record =
                    new NodeExecutionRecord(
                            UUID.randomUUID().toString(),
                            executionId,
                            node.node().id(),
                            node.node().nodeCode(),
                            node.componentContract().componentType(),
                            scope.value(),
                            number,
                            ExecutionStatus.QUEUED,
                            clock.instant(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            logSequence = 0;
            contentSequence = 0;
            store.saveNode(record);
            emit(executionId, record, "NODE_QUEUED", ExecutionStatus.QUEUED, Map.of());
        }

        /** Marks actual executor readiness after concurrency permits have been acquired. */
        public void started() {
            synchronized (ExecutionRecorder.this) {
                transition(ExecutionStatus.RUNNING, null, null, null, null);
                diagnostic("node started", ExecutionLogLevel.INFO, null);
            }
        }

        /** Stores bound inputs without generating a second lifecycle transition. */
        public void inputs(Object value) {
            synchronized (ExecutionRecorder.this) {
                record =
                        new NodeExecutionRecord(
                                record.nodeExecutionId(),
                                executionId,
                                record.nodeId(),
                                record.nodeCode(),
                                record.componentType(),
                                record.scopePath(),
                                record.attempt(),
                                record.status(),
                                record.queuedAt(),
                                record.startedAt(),
                                record.finishedAt(),
                                savePayload(executionId, value),
                                record.outputPayloadRef(),
                                record.errorId(),
                                record.errorCode());
                store.saveNode(record);
            }
        }

        /**
         * Records success only after output protocol validation; failing END results remain
         * failures.
         */
        public void succeeded(NodeResult result) {
            synchronized (ExecutionRecorder.this) {
                if (result.terminalOutcome() != null
                        && result.terminalOutcome().status() != ExecutionStatus.SUCCEEDED) {
                    failed(
                            new NodeExecutionException(
                                    result.terminalOutcome().errorCode(),
                                    "END failure",
                                    result.terminalOutcome().status()),
                            result.terminalOutcome());
                    return;
                }
                transition(
                        ExecutionStatus.SUCCEEDED,
                        null,
                        savePayload(executionId, result.outputs()),
                        null,
                        null);
                diagnostic(
                        "node completed; output protocol validated", ExecutionLogLevel.INFO, null);
            }
        }

        /**
         * Associates a safe error identity with this attempt and the owning execution's root cause.
         */
        public void failed(Throwable failure) {
            failed(failure, failure);
        }

        private void failed(Throwable failure, Object cause) {
            synchronized (ExecutionRecorder.this) {
                if (record.finishedAt() != null) return;
                boolean cancelled =
                        failure instanceof java.util.concurrent.CancellationException
                                || failure instanceof InterruptedException;
                String code =
                        failure instanceof NodeExecutionException n
                                ? n.code()
                                : cancelled ? "EXECUTION_CANCELLED" : "INTERNAL";
                String id = errorIdentity(executionId, cause);
                transition(
                        cancelled
                                ? ExecutionStatus.CANCELLED
                                : failure instanceof NodeExecutionException n
                                        ? n.terminalStatus()
                                        : ExecutionStatus.FAILED,
                        null,
                        null,
                        id,
                        code);
                try {
                    diagnostic("node failed", ExecutionLogLevel.ERROR, id);
                } catch (RuntimeException auditFailure) {
                    // 原始失败已经确定；诊断异常不能将调度器的仲裁根因替换为日志故障。
                    required(executionId).auditFailed = true;
                }
            }
        }

        /** Called by HTTP only when its retry policy actually chooses another wire attempt. */
        public void retry(String code) {
            synchronized (ExecutionRecorder.this) {
                transition(
                        ExecutionStatus.RETRYING, null, null, UUID.randomUUID().toString(), code);
                diagnostic("HTTP retry scheduled", ExecutionLogLevel.WARN, record.errorId());
                newRecord(record.attempt() + 1);
                started();
            }
        }

        /**
         * Rebinds producer identity and stores content separately from its replayable reference.
         */
        public void content(ContentChunk incoming) {
            incoming.validateResourceLimits();
            synchronized (ExecutionRecorder.this) {
                if (record.finishedAt() != null || !active.containsKey(executionId)) return;
                var chunk =
                        new ContentChunk(
                                executionId,
                                record.nodeCode(),
                                record.scopePath(),
                                contentSequence++,
                                incoming.contentType(),
                                incoming.payload());
                store.appendContent(record.nodeExecutionId(), chunk);
                emit(
                        executionId,
                        record,
                        "CONTENT",
                        ExecutionStatus.RUNNING,
                        Map.of(
                                "contentType",
                                chunk.contentType(),
                                "contentPayloadRef",
                                payloads.store(executionId, chunk.payload()).value(),
                                "contentSequence",
                                chunk.sequence()));
            }
        }

        @Override
        public void write(ExecutionLogEntry entry) {
            synchronized (ExecutionRecorder.this) {
                if (!active.containsKey(executionId)) return;
                // Arbitrary business strings and attributes are protected payloads. Default queries
                // expose only safe diagnostics.
                String reference =
                        savePayload(
                                executionId,
                                Map.of(
                                        "message",
                                        entry.message(),
                                        "attributes",
                                        entry.attributes()));
                appendLog(
                        entry.level(),
                        "business diagnostic",
                        reference == null ? Map.of() : Map.of("diagnosticPayloadRef", reference),
                        null);
            }
        }

        private void diagnostic(String message, ExecutionLogLevel level, String errorId) {
            appendLog(level, message, Map.of(), errorId);
        }

        private void appendLog(
                ExecutionLogLevel level,
                String message,
                Map<String, Object> attrs,
                String errorId) {
            try {
                var log =
                        new ExecutionLogRecord(
                                UUID.randomUUID().toString(),
                                executionId,
                                record.nodeExecutionId(),
                                clock.instant(),
                                level,
                                message.equals("business diagnostic")
                                        ? record.componentType().name().toLowerCase(Locale.ROOT)
                                        : "engine",
                                logSequence++,
                                message,
                                attrs,
                                errorId);
                store.appendLog(log);
                if (active.containsKey(executionId))
                    emit(
                            executionId,
                            record,
                            "LOG",
                            record.status(),
                            errorId == null
                                    ? Map.of("logId", log.logId(), "level", level.name())
                                    : Map.of(
                                            "logId",
                                            log.logId(),
                                            "level",
                                            level.name(),
                                            "errorId",
                                            errorId));
            } catch (RuntimeException failure) {
                ExecutionRecorder.this.warn(failure);
            }
        }

        private void transition(
                ExecutionStatus status, String input, String output, String errorId, String code) {
            Instant started = record.startedAt();
            if (status == ExecutionStatus.RUNNING && started == null) started = clock.instant();
            boolean ended = status != ExecutionStatus.RUNNING && status != ExecutionStatus.QUEUED;
            record =
                    new NodeExecutionRecord(
                            record.nodeExecutionId(),
                            executionId,
                            record.nodeId(),
                            record.nodeCode(),
                            record.componentType(),
                            record.scopePath(),
                            record.attempt(),
                            status,
                            record.queuedAt(),
                            started,
                            ended ? clock.instant() : null,
                            input == null ? record.inputPayloadRef() : input,
                            output == null ? record.outputPayloadRef() : output,
                            errorId,
                            code);
            store.saveNode(record);
            if (active.containsKey(executionId))
                emit(
                        executionId,
                        record,
                        switch (status) {
                            case RUNNING -> "NODE_STARTED";
                            case SUCCEEDED -> "NODE_COMPLETED";
                            case RETRYING -> "RETRYING";
                            case CANCELLED -> "NODE_CANCELLED";
                            case SKIPPED -> "NODE_SKIPPED";
                            default -> "NODE_FAILED";
                        },
                        status,
                        errorId == null ? Map.of() : Map.of("errorId", errorId, "code", code));
        }

        /** Resolves an inactive incoming branch without claiming the executor ran. */
        public void skipped() {
            synchronized (ExecutionRecorder.this) {
                transition(ExecutionStatus.SKIPPED, null, null, null, null);
            }
        }
    }
}
