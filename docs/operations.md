# Flow Engine 运维基线

本文描述独立 `engine-server` 和复用 `engine-server-starter` 的首期运行边界。容量数字是工程门禁，不是生产 SLA；上线前仍需按实例 CPU、内存、MySQL 和下游限额重新压测。

## 1. 启动前提

- Java 21；启用虚拟线程。
- DURABLE 模式使用 MySQL 8，运行身份只授予引擎 schema 的 DDL/DML 权限。
- 宿主必须显式提供可信 `FlowBundleLoader`、`ExecutionAccessPolicy`，需要外部 scope 时提供 `ExecutionContextLoader`；默认实现全部拒绝访问。
- 业务组件由宿主启动期注册，组件 Bean 必须无状态、线程安全；引擎不加载 Bundle 中声明的类名。
- 数据库凭据、签名密钥和业务 Secret 只从运行环境或密钥服务注入，不进入源码、Bundle、checkpoint 或日志。

通用发行版只在显式开启 demo 且绑定回环地址时用于本地验证：

```sh
java -jar engine-server/target/engine-server-0.1.0-SNAPSHOT.jar \
  --flow-engine.demo.enabled=true \
  --server.address=127.0.0.1 \
  --server.port=8099
```

生产宿主启用 DURABLE 所需属性：

```text
flow-engine.durable.enabled=true
flow-engine.durable.jdbc-url=<runtime-owned JDBC URL>
flow-engine.durable.username=<runtime identity>
flow-engine.durable.password=<secret reference or injected value>
```

健康检查使用 `GET /actuator/health`。健康只证明进程已启动；发布验收还必须执行一条真实流程并查询终态、节点尝试和授权 Payload。

## 2. 资源边界

默认并发限制：

| 资源 | 属性 | 默认值 |
| --- | --- | ---: |
| 全局节点调用 | `flow-engine.concurrency.global` | 1024 |
| 单租户节点调用 | `flow-engine.concurrency.per-tenant` | 256 |
| 单流程节点调用 | `flow-engine.concurrency.per-flow` | 256 |
| 单组件节点调用 | `flow-engine.concurrency.per-component` | 128 |
| 单外部资源调用 | `flow-engine.concurrency.per-resource` | 64 |
| SSE 订阅 | `flow-engine.events.max-subscribers` | 512 |
| 单执行 SSE 邮箱 | `flow-engine.events.buffer-per-execution` | 256 |
| 单 Payload | `flow-engine.payload.max-bytes` | 1 MiB |
| 请求截止时间上限 | `flow-engine.execution.max-duration` | 5 分钟 |

`ConcurrencyGovernor.snapshot()` 提供不含租户、流程和 Payload 身份的进程内 active/waiting 计数，宿主可采样为高水位指标。不得仅扩大队列或 permit；任何上调都要同时验证拒绝、取消、deadline、heap 和平台线程上限。

DURABLE 默认工作池最多 16 个活动连接并保留最多 16 个 idle 连接，避免高频 checkpoint 反复建连。lease heartbeat 使用单线程控制通道；工作池饱和时最多额外建立 1 个非池化控制连接。业务副作用资源应使用宿主自己的有界连接池，不与引擎状态池共享阻塞队列。

本机首期门禁结果：

| 场景 | 结果 |
| --- | --- |
| 1000 并发轻量 EPHEMERAL（真实 REST WAIT） | 1000/1000；wall 2658ms；p95 2566ms；p99 2629ms；permit in-flight high-water 1000，active/waiting high-water 515/488 |
| 500 并发 SSE | 500/500；事件顺序正确；wall 6221ms；p95 6213ms；p99 6214ms；permit in-flight high-water 423 |
| 200 并发真实回环 HTTP I/O | 200/200；wall 4281ms；p95 4266ms；p99 4278ms；permit in-flight high-water 200，平台线程增长 14 |
| 100 并发 DURABLE 强制重启后自动扫描恢复 | 100/100；wall 15751ms；p95 15353ms；p99 15507ms；permit/storage in-flight 最终均为 0；三项副作用各 100 次 |

上述数据来自 2026-09-13 的 fresh `-Pengine-benchmark verify` 和最终独立恢复复验。EPHEMERAL、SSE 与 I/O 场景均经过真实 Server HTTP 端口；轻量 EPHEMERAL 在业务节点统一阻塞，直到服务端单一原子许可计数 `inFlightAcquisitions` 达到 1000 才释放，防止只证明客户端同时发起请求。I/O 业务组件访问测试拥有的真实回环 HTTP 服务。容量采样记录 heap、平台线程和调度许可 in-flight/active/waiting 高水位；本轮 EPHEMERAL、SSE、I/O 的 heap high-water 分别为 394,646,656、444,596,224、467,412,320 bytes，平台线程增长分别为 83、5、14。SSE sender 使用 512 上限和零缓存同步移交，HTTP 客户端 admission 也保持有界。DURABLE 使用一次性 MySQL、首 JVM 真实 `SIGKILL`，第二 JVM 启动生产同类 `DurableRecoveryLifecycle` 自动扫描接管；最终恢复复验 heap high-water 73,425,280 bytes、增长 45,717,032 bytes，平台线程高水位 27、增长 19，node permit/storage capacity in-flight 高水位分别为 90/12，终态均为 0。以上是工程门禁，不是生产 SLA，也不能替代部署环境的连接池和下游容量监控。

## 3. DURABLE 与恢复

- Flyway 启动时执行只追加 migration。V3 为 `engine_payload(tenant_id, execution_id, pinned, payload_id)` 增加 checkpoint 清理索引；禁止改写 V1-V3。
- 执行租约为 15 秒，约每 5 秒续租。checkpoint 写入验证 owner、fencing token 和 revision CAS。
- 同租户 Payload、状态和观察容量写入先经过固定 64 stripe 的进程内队列，再获取数据库容量行锁；数据库锁仍负责跨 JVM 正确性。进程内同时 admission 上限固定为 256，可通过 `capacitySnapshot()` 读取无身份信息的当前 in-flight/limit；上限已满返回 `RUNTIME_STORAGE_CAPACITY_BUSY`，等待 stripe 锁时被中断返回 `RUNTIME_STORAGE_CAPACITY_INTERRUPTED`，两种情况都不会借用数据库连接。
- 每执行仅保留最近两个完整 checkpoint；正在使用的 pinned Payload 不参与普通到期清理。
- 自动恢复每秒扫描最多 100 条可恢复记录。旧 owner 被 `SIGKILL` 后，新实例必须等待数据库租约过期，随后从已提交的不可变 plan/request 恢复。
- 已提交节点结果不重放；未知非幂等副作用进入 `WAITING_MANUAL`，不得自动猜测成功或失败。

发布或重启流程：

1. 停止接收新流量，观察 governor waiting、活动执行和 MySQL 连接占用。
2. 正常发布使用 graceful shutdown；强制终止场景按故障处理，不手工删除 lease。
3. 新实例健康后等待至少一个 lease 周期，查询恢复候选的确定终态。
4. 对 `WAITING_MANUAL` 检查业务侧幂等账本，由授权人员决定后续；当前 API 不提供自动确认。
5. 核对每个 execution 的终态事件唯一、节点 attempt 完整、副作用幂等键无重复。

## 4. 观察与安全

- 执行、节点 attempt、生命周期事件、结构化日志、内容流和 Payload 分开保存；普通查询只返回摘要。
- 完整 Payload 需要独立 `PAYLOAD` 授权。跨租户 execution、node、log、event 和 Payload 访问必须 fail closed。
- SSE 新执行只允许 POST；重连使用已授权 execution 的 GET 和 `Last-Event-ID`，不得通过重发 POST 制造第二次副作用。
- SCRIPT 运行在沙箱中；HTTP adapter 必须限制 scheme、DNS 解析结果、私网/回环目标、重定向和响应体大小。TRIAL 的副作用目标由宿主提供独立 allowlist。
- 请求体上限 256 KiB；超限返回 413。日志和错误不得包含 Secret、认证头、签名、原始敏感字段或 Throwable 文本。

建议至少告警：未知终态、lease heartbeat 失败、恢复反复失败、`WAITING_MANUAL`、SSE 慢订阅者、容量拒绝、审计 warning、MySQL 活动连接接近 16、governor waiting 持续增长，以及 Payload/观察容量接近上限。

## 5. 验证命令

聚焦 MySQL、恢复和安全验证需要可用 Docker：

```sh
DOCKER_HOST=unix:///path/to/docker.sock \
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
mvn -q -pl engine-adapters,engine-server-starter -am \
  -Dtest=MySqlStoreContractTest,MySqlMigrationUpgradeTest,DurableRecoveryTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

容量与安全 profile：

```sh
mvn -q -Pengine-benchmark -pl engine-server-starter -am verify
```

真实独立 JVM恢复：

```sh
mvn -q -pl engine-server -am package
node engine-server/scripts/verify-durable-process.mjs \
  --server-jar engine-server/target/engine-server-0.1.0-SNAPSHOT.jar
node engine-server/scripts/verify-task14-durable-capacity.mjs
```

两个脚本只删除自己创建且标签匹配的一次性 MySQL 容器。代码测试、独立进程恢复、PlayRewards 双进程 BFF 和浏览器验收必须分别记录，不能互相替代。

## 6. 故障定位

| 现象/错误码 | 核对项 |
| --- | --- |
| `DURABLE_EXECUTION_LEASE_BUSY` | 旧 owner 是否仍存活；不要手工抢占未过期 lease |
| `EXECUTION_LEASE_LOST` | heartbeat、控制连接、数据库时钟、长事务和 fencing token |
| `EXECUTION_CAPACITY_EXCEEDED` | active/waiting 高水位、单租户/流程/组件限制；不要直接扩容队列 |
| `RECOVERY_PAYLOAD_MISSING` | pinned Payload 是否被误清理、V3 migration 是否生效 |
| `WAITING_MANUAL` | 未完成 SIDE_EFFECT 的外部幂等账本和人工处置记录 |
| SSE 断线 | 使用最后确认的 `Last-Event-ID` GET 重连；不要重发 POST |
| 连接池卡顿 | 引擎状态池与业务池是否隔离、idle 是否为 16、MySQL/Docker 建连延迟 |

诊断记录只保存 executionId、flow/version、Bundle Hash、错误码和脱敏指标；不得采集原始 Payload、数据库密码或信任密钥。
