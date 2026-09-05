-- Business LOCK resources are deliberately separate from execution ownership leases.
CREATE TABLE engine_business_lock (
 tenant_id VARCHAR(128) NOT NULL, resource_digest CHAR(64) NOT NULL,
 owner_digest CHAR(64) NOT NULL, lease_id VARCHAR(64) NOT NULL,
 fencing_token BIGINT NOT NULL, expires_at TIMESTAMP(6) NOT NULL,
 PRIMARY KEY(tenant_id,resource_digest), KEY ix_lock_expiry(tenant_id,expires_at)
);
CREATE TABLE engine_capacity (
 tenant_id VARCHAR(128) NOT NULL, PRIMARY KEY(tenant_id)
);
ALTER TABLE engine_content ADD COLUMN created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE engine_node_attempt ADD COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);
