# 通用执行 REST/SSE Starter

宿主只依赖 `engine-server-starter`。Boot 从 AutoConfiguration imports 装配 Controller、固定业务组件注册表和完整节点引擎，无需依赖 `FlowEngineApplication`。

## 接入与身份

宿主注册 `FlowBundleLoader`、`FlowBusinessComponent` 与 `ExecutionAccessPolicy` Bean；需要外部上下文或子流程时分别注册 `ExecutionContextLoader`、`RelatedFlowResolver`。所有注册均在启动阶段完成。

默认身份策略拒绝执行与查询，health 独立开放。`ExecutionAccessPolicy` 从可信会话/认证组件解析租户，并按 `FlowIdentity` 分别授权执行、读取、日志、完整 Payload 和取消。请求中的 tenantId 不构成身份；与可信租户冲突时拒绝。公网接口只接受引用，不接受 Bundle、executorRef 或 Java 类。

## 请求

```json
{"flowReference":"demo:main","responseMode":"WAIT","mode":"EPHEMERAL","purpose":"TRIAL","startInputs":{},"externalScopes":{},"idempotencyKey":"caller-key"}
```

`POST /v1/executions`：WAIT 返回终态观察摘要；ASYNC 返回 202 和 executionId。deadline 默认 30 秒，最大 5 分钟，请求 JSON 最大 256 KiB。开始输入仍按 START 协议验证；externalScopes 必须与 Bundle 声明一致。contextReference 与直接外部上下文不能同时提供。

`POST /v1/executions/stream` 新建一次执行并输出 SSE。断线必须使用 `GET /v1/executions/{executionId}/stream` 并携带 `Last-Event-ID`；带游标的 POST 会被拒绝，防止重连触发第二次副作用。游标格式为 `executionId:sequence`；只接受该已授权执行的保留游标。过期游标明确失败，不从头重跑业务。

查询：

- `GET /v1/executions/{id}`、`/{id}/events?afterSequence=-1&limit=100`、`/{id}/nodes?offset=0&limit=100`
- `GET /v1/node-executions/{id}`、`/{id}/logs?offset=0&limit=100`
- `GET /v1/payloads/{id}`：额外 PAYLOAD 授权后才返回完整值
- `POST /v1/executions/{id}/cancel`：显式取消，断开 SSE 不取消执行
- `POST /v1/executions/{id}/resume`：当前返回 409 / EPHEMERAL_RESUME_UNSUPPORTED

分页最多 500 条。DURABLE 请求明确返回 409，WAITING_MANUAL 可以查询但不自动重放副作用。

## 观察与资源边界

每次实际 node/scope/attempt 分配唯一 nodeExecutionId；子流程有独立 executionId 与 root/parent 关系。生命周期事件、结构化诊断日志、ContentChunk 与 Payload 分表保存。业务/脚本诊断原文和属性保存在单独授权的 diagnosticPayloadRef，默认日志只展示安全摘要，Throwable 不进入 JSON。HTTP 重试记录每次实际尝试，失败节点 ERROR、事件与终态共享 errorId。

内存实现默认最多保留 1024 次执行，每次最多 16384 节点/事件/载荷；每节点日志最多 1024 条、内容最多 4096 块，每个 Payload 最大 64 KiB，每块内容最大 16 KiB。执行完成后一小时清理，活跃执行不会因过期清理被移除。事件到上限淘汰最早游标，后续有效游标仍能恢复。超过执行或节点/载荷容量会明确拒绝或产生安全告警。

SSE 最多 512 个订阅，单订阅实时邮箱 256 条，发送任务上限 512；发布线程只写邮箱，不执行网络发送。慢订阅者断开后可以从最后确认游标 GET 重连。长连接最多 310 秒。

关键状态和 ERROR 在最终响应前同步写入并 flush。普通诊断写入故障产生限频安全告警及 recorder.warningCount；`flow-engine.observation.strict-audit=true` 可将审计故障设为 fail closed。存储可由宿主替换为外部实现，但这不等于 DURABLE 恢复能力。

## 本地演示

存储配置均在启动时生效：`flow-engine.observation.max-executions`、`max-nodes`；`observation.events.max-count/max-bytes/retention`；`observation.logs.max-count/max-bytes/retention`；`observation.payloads.max-count/retention`；`observation.executions.retention`；`observation.content.max-count`。完整值大小使用 `flow-engine.payload.max-bytes`，通用发行版配置为 1 MiB。各类保留期从执行完成开始分别计时，活跃执行的记录不被清理。

SSE 邮箱和订阅数分别由 `flow-engine.events.buffer-per-execution`、`flow-engine.events.max-subscribers` 控制；发送工作数另有 512 的硬上限。五层 governor 绑定 `flow-engine.concurrency.global/per-tenant/per-flow/per-component/per-resource`；`flow-engine.execution.max-duration` 控制请求截止时间上限（不能高于五分钟的服务硬限制）。

自定义 `ExecutionRecordStore` 必须同时提供 `PayloadStore`（也可以由同一个实例实现两者），缺少时启动失败。内容原文以 `contentPayloadRef` 存储，普通事件查询不会返回原文；SSE 在每个分片的实际所属流程通过 PAYLOAD 授权后才补入原文，子流同样独立判断。

通用发行版显式开启演示并绑定回环地址：

```sh
java -jar engine-server/target/engine-server-0.1.0-SNAPSHOT.jar --flow-engine.demo.enabled=true --server.address=127.0.0.1 --server.port=8099
```

默认演示租户固定为 demo。示例业务与 Loader 位于 `engine-server`，不会带入私有宿主。`demo:main` 提供真实流式、并行、两轮循环和子流程；`demo:slow` 用于取消；`demo:failure` 用于协议错误关联；`demo:all` 补全 HTTP、锁、IF、VARIABLE，HTTP 仅访问精确允许的本地验收目标。

这里的内存观察不能跨进程恢复；生产宿主必须提供实际认证、业务加载和相应共享资源适配器。
