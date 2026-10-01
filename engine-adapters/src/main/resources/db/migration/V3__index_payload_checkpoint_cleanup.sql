-- Checkpoint retention deletes only one execution's pinned payloads. Keep that range isolated so
-- concurrent executions do not scan and gap-lock the tenant's whole payload primary-key range.
CREATE INDEX ix_payload_execution
 ON engine_payload(tenant_id, execution_id, pinned, payload_id);
