package com.playrewards.flowengine.adapter.mysql;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.spi.*;

import java.util.*;

/**
 * Strong checkpoints never read observation tables; CAS and lease fence use one locked transaction.
 */
public final class MySqlExecutionStateStore implements FencedExecutionStateStore {
    private final MySqlRuntimeDatabase db;
    private final String tenant;

    public MySqlExecutionStateStore(MySqlRuntimeDatabase db, String tenant) {
        this.db = db;
        this.tenant = tenant;
    }

    /** Reads only this tenant's complete checkpoint, never a diagnostic projection. */
    public Optional<ExecutionState> find(String id) {
        return db.transaction(tx -> read(tx, id));
    }

    private Optional<ExecutionState> read(MySqlRuntimeDatabase.Tx tx, String id) {
        return tx.row(
                        "SELECT revision,status,checkpoint FROM engine_execution_state WHERE"
                                + " tenant_id=#{args[0]} AND execution_id=#{args[1]}",
                        tenant,
                        id)
                .map(
                        r ->
                                new ExecutionState(
                                        id,
                                        MySqlRuntimeDatabase.number(r, "revision"),
                                        ExecutionStatus.valueOf(
                                                MySqlRuntimeDatabase.text(r, "status")),
                                        JSON.parseObject(
                                                MySqlRuntimeDatabase.text(r, "checkpoint"))));
    }

    /**
     * Persists revision, status, checkpoint history and reference cleanup in one fenced
     * transaction.
     */
    public ExecutionState save(ExecutionState value, long revision, ExecutionLease lease) {
        return db.capacityTransaction(tenant, tx -> save(tx, value, revision, lease));
    }

    private ExecutionState save(
            MySqlRuntimeDatabase.Tx tx, ExecutionState value, long revision, ExecutionLease lease) {
        MySqlLeaseRepository.validate(tx, tenant, value.executionId(), lease);
        var current = read(tx, value.executionId());
        if (current.isPresent()
                && terminal(current.get().status())
                && current.get().status() != value.status())
            throw new IllegalStateException("TERMINAL_STATE_IMMUTABLE");
        String json = JSON.toJSONString(value.state());
        if (json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 262144)
            throw new IllegalArgumentException("CHECKPOINT_TOO_LARGE");
        int changed =
                revision == 0
                        ? tx.update(
                                "INSERT IGNORE INTO"
                                    + " engine_execution_state(tenant_id,execution_id,revision,status,checkpoint)"
                                    + " VALUES(#{args[0]},#{args[1]},1,#{args[2]},#{args[3]})",
                                tenant,
                                value.executionId(),
                                value.status().name(),
                                json)
                        : tx.update(
                                "UPDATE engine_execution_state SET"
                                    + " revision=revision+1,status=#{args[2]},checkpoint=#{args[3]},updated_at=CURRENT_TIMESTAMP(6)"
                                    + " WHERE tenant_id=#{args[0]} AND execution_id=#{args[1]} AND"
                                    + " revision=#{args[4]}",
                                tenant,
                                value.executionId(),
                                value.status().name(),
                                json,
                                revision);
        if (changed != 1) throw new IllegalStateException("CHECKPOINT_REVISION_CONFLICT");
        tx.update(
                "INSERT INTO engine_checkpoint VALUES(#{args[0]},#{args[1]},#{args[2]},#{args[3]})",
                tenant,
                value.executionId(),
                revision + 1,
                json);
        // Keep the latest complete revision and one predecessor; remove only unreferenced recovery
        // values.
        tx.update(
                "DELETE FROM engine_checkpoint WHERE tenant_id=#{args[0]} AND"
                        + " execution_id=#{args[1]} AND revision<#{args[2]}",
                tenant,
                value.executionId(),
                revision);
        // Payload writes and both retention paths must enter through one tenant mutex. Otherwise
        // expiry cleanup and checkpoint cleanup can lock the expiry and execution indexes in
        // opposite order and deadlock even though they target different executions.
        MySqlCapacity.lock(tx, tenant);
        tx.update(
                "DELETE p FROM engine_payload p LEFT JOIN engine_checkpoint c ON"
                    + " c.tenant_id=p.tenant_id AND c.execution_id=p.execution_id AND"
                    + " JSON_UNQUOTE(JSON_EXTRACT(c.checkpoint,'$.document'))=p.payload_id WHERE"
                    + " p.tenant_id=#{args[0]} AND p.execution_id=#{args[1]} AND p.pinned=TRUE AND"
                    + " c.revision IS NULL",
                tenant,
                value.executionId());
        return new ExecutionState(value.executionId(), revision + 1, value.status(), value.state());
    }

    /** Terminal states cannot reopen, even when the caller owns the lease and matching revision. */
    public boolean transition(
            String id,
            long revision,
            ExecutionStatus from,
            ExecutionStatus to,
            Map<String, Object> checkpoint,
            ExecutionLease lease) {
        return db.capacityTransaction(
                tenant,
                tx -> {
                    MySqlLeaseRepository.validate(tx, tenant, id, lease);
                    var current = read(tx, id);
                    if (current.isEmpty()
                            || current.get().revision() != revision
                            || current.get().status() != from) return false;
                    if (terminal(from) && from != to) return false;
                    save(tx, new ExecutionState(id, revision, to, checkpoint), revision, lease);
                    return true;
                });
    }

    public List<ExecutionState> recoverable(int limit) {
        if (limit < 1 || limit > 1000)
            throw new IllegalArgumentException("recovery batch out of bounds");
        return db.transaction(
                tx ->
                        tx
                                .rows(
                                        "SELECT execution_id FROM engine_execution_state WHERE"
                                                + " tenant_id=#{args[0]} AND status IN"
                                                + " ('CREATED','RUNNING','WAITING_RETRY') ORDER BY"
                                                + " updated_at LIMIT #{args[1]}",
                                        tenant,
                                        limit)
                                .stream()
                                .map(
                                        r ->
                                                read(
                                                                tx,
                                                                MySqlRuntimeDatabase.text(
                                                                        r, "execution_id"))
                                                        .orElseThrow())
                                .toList());
    }

    private static boolean terminal(ExecutionStatus status) {
        return status == ExecutionStatus.SUCCEEDED
                || status == ExecutionStatus.FAILED
                || status == ExecutionStatus.CANCELLED;
    }
}
