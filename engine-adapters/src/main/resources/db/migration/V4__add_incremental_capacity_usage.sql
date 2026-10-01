-- 容量上限改为按 (tenant, table) 增量计数：写入与计数在同一事务内条件递增，删除按实际删除行数递减，
-- 因此提交点上 used 始终等于真实行数，且不再需要每次写入执行 COUNT(*)。
CREATE TABLE engine_capacity_usage (
 tenant_id VARCHAR(128) NOT NULL, table_name VARCHAR(64) NOT NULL, used BIGINT NOT NULL,
 PRIMARY KEY(tenant_id, table_name)
);
-- 升级时一次性用真实行数播种；之后由写入/清理路径在事务内维护。
INSERT INTO engine_capacity_usage SELECT tenant_id,'engine_execution',COUNT(*) FROM engine_execution GROUP BY tenant_id;
INSERT INTO engine_capacity_usage SELECT tenant_id,'engine_node_attempt',COUNT(*) FROM engine_node_attempt GROUP BY tenant_id;
INSERT INTO engine_capacity_usage SELECT tenant_id,'engine_event',COUNT(*) FROM engine_event GROUP BY tenant_id;
INSERT INTO engine_capacity_usage SELECT tenant_id,'engine_log',COUNT(*) FROM engine_log GROUP BY tenant_id;
INSERT INTO engine_capacity_usage SELECT tenant_id,'engine_content',COUNT(*) FROM engine_content GROUP BY tenant_id;
INSERT INTO engine_capacity_usage SELECT tenant_id,'engine_payload',COUNT(*) FROM engine_payload GROUP BY tenant_id;
