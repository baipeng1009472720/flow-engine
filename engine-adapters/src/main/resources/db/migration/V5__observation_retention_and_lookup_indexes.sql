-- 执行记录进入可过期状态时写入 expires_at；NULL 表示活动 DURABLE/WAITING_MANUAL，永不按保留期删除。
ALTER TABLE engine_execution
 ADD COLUMN expires_at TIMESTAMP(6) NULL,
 ADD KEY ix_execution_expiry(tenant_id, expires_at),
 ADD KEY ix_execution_lookup(execution_id);
-- 升级前已结束的记录按默认 7 天保留期补齐；WAITING_MANUAL 需要人工处置，保持不过期。
UPDATE engine_execution SET expires_at=TIMESTAMPADD(DAY, 7, updated_at)
 WHERE status IN ('SUCCEEDED','FAILED','CANCELLED');
-- 按创建时间分批清理的范围索引；没有它们时每次清理都会扫描并锁住整个租户主键范围。
ALTER TABLE engine_event ADD KEY ix_event_expiry(tenant_id, created_at);
ALTER TABLE engine_log ADD KEY ix_log_expiry(tenant_id, created_at);
ALTER TABLE engine_content ADD KEY ix_content_expiry(tenant_id, created_at);
-- 不带租户的不透明 ID 路由查询（execution/node/payload）此前是全表扫描。
ALTER TABLE engine_node_attempt ADD KEY ix_node_lookup(node_execution_id);
ALTER TABLE engine_payload ADD KEY ix_payload_lookup(payload_id);
