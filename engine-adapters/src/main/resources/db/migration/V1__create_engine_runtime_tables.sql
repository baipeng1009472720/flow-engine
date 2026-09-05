-- Tenant is the leading key everywhere. Strong recovery and diagnostic data remain separate.
CREATE TABLE engine_execution_state (
 tenant_id VARCHAR(128) NOT NULL, execution_id VARCHAR(64) NOT NULL,
 revision BIGINT NOT NULL, status VARCHAR(32) NOT NULL, checkpoint JSON NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(tenant_id,execution_id), KEY ix_recovery(tenant_id,status,updated_at)
);
CREATE TABLE engine_checkpoint (
 tenant_id VARCHAR(128) NOT NULL, execution_id VARCHAR(64) NOT NULL,
 revision BIGINT NOT NULL, checkpoint JSON NOT NULL,
 PRIMARY KEY(tenant_id,execution_id,revision)
);
CREATE TABLE engine_lease (
 tenant_id VARCHAR(128) NOT NULL, execution_id VARCHAR(64) NOT NULL,
 owner_id VARCHAR(64) NOT NULL, fencing_token BIGINT NOT NULL, expires_at TIMESTAMP(6) NOT NULL,
 PRIMARY KEY(tenant_id,execution_id), KEY ix_expiry(tenant_id,expires_at)
);
CREATE TABLE engine_idempotency (
 tenant_id VARCHAR(128) NOT NULL, key_digest CHAR(64) NOT NULL, execution_id VARCHAR(64) NOT NULL,
 PRIMARY KEY(tenant_id,key_digest)
);
CREATE TABLE engine_execution (
 tenant_id VARCHAR(128) NOT NULL, execution_id VARCHAR(64) NOT NULL,
 status VARCHAR(32) NOT NULL, event_cursor BIGINT NOT NULL DEFAULT -1, body JSON NOT NULL,
 updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(tenant_id,execution_id)
);
CREATE TABLE engine_node_attempt (
 tenant_id VARCHAR(128) NOT NULL, node_execution_id VARCHAR(64) NOT NULL,
 execution_id VARCHAR(64) NOT NULL, node_id VARCHAR(128) NOT NULL,
 scope_digest CHAR(64) NOT NULL, attempt INT NOT NULL, body JSON NOT NULL,
 PRIMARY KEY(tenant_id,node_execution_id),
 UNIQUE KEY uq_attempt(tenant_id,execution_id,node_id,scope_digest,attempt)
);
CREATE TABLE engine_event (
 tenant_id VARCHAR(128) NOT NULL, execution_id VARCHAR(64) NOT NULL,
 sequence BIGINT NOT NULL, body JSON NOT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(tenant_id,execution_id,sequence)
);
CREATE TABLE engine_log (
 tenant_id VARCHAR(128) NOT NULL, node_execution_id VARCHAR(64) NOT NULL,
 sequence BIGINT NOT NULL, body JSON NOT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(tenant_id,node_execution_id,sequence)
);
CREATE TABLE engine_content (
 tenant_id VARCHAR(128) NOT NULL, node_execution_id VARCHAR(64) NOT NULL,
 sequence BIGINT NOT NULL, body JSON NOT NULL,
 PRIMARY KEY(tenant_id,node_execution_id,sequence)
);
CREATE TABLE engine_payload (
 tenant_id VARCHAR(128) NOT NULL, payload_id VARCHAR(64) NOT NULL,
 execution_id VARCHAR(64) NOT NULL, digest CHAR(64) NOT NULL, size_bytes INT NOT NULL,
 body MEDIUMTEXT NOT NULL, expires_at TIMESTAMP(6) NOT NULL, pinned BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(tenant_id,payload_id), KEY ix_payload_expiry(tenant_id,pinned,expires_at)
);
