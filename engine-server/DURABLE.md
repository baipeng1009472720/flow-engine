# MySQL DURABLE 运行

在宿主已有鉴权、可信 FlowBundleLoader 和业务组件装配基础上启用以下配置，连接参数由运行环境注入：

    flow-engine.durable.enabled=true
    flow-engine.durable.jdbc-url
    flow-engine.durable.username
    flow-engine.durable.password

凭据通过运行环境提供，不写入源码、检查点或日志。Starter 使用最多16个活动数据库连接；启动执行Flyway迁移，已有V1使用新增V2升级。每个请求使用宿主认证的tenant，所有存储按真实owner路由。

POST /v1/executions 选择 mode=DURABLE，WAIT/ASYNC/SSE沿用同一契约。每秒扫描至多100个可恢复执行，执行租约15秒，每5秒续租；节点/检查点提交都验证版本与fencing。进程退出后新实例等待租约过期自动接续，也可在授权后POST /v1/executions/{id}/resume。WAITING_MANUAL不会自动重跑；该接口不提供猜测副作用成功/失败的确认操作。

恢复使用保存的不可漂移计划、请求、完整scope上下文、loop collection帧、稳定child identity和已提交NodeResult。原DagScheduler重建就绪关系；已完成节点不调用执行器。业务LOCK使用独立MySQL锁表并在恢复时重新获取fence。未完成的非幂等副作用进入WAITING_MANUAL。

终态先保存可恢复的强终态意图，再事务提交关键记录和终态事件。关键审计完成后若最终强状态标记暂时失败，对外保留已提交的相同终态并释放调度owner；恢复只补齐finalize，不重复业务节点。

观察事件、日志、内容分别支持现有 flow-engine.observation.<events|logs|content>.max-count/retention；执行/节点数量用 observation.max-executions/max-nodes。默认每tenant每观察表上限10000。普通Payload默认1MiB、1小时，使用 payload.max-bytes 与 observation.payloads.retention。恢复Payload受相同单值大小上限约束，但不随观察保留期过期；每执行仅保留最近两个完整checkpoint及其引用。容量耗尽时拒绝新写，不淘汰活跃恢复数据。

验证：

    mvn -q -pl engine-adapters,engine-server-starter,engine-server -am package
    node engine-server/scripts/verify-durable-process.mjs --server-jar engine-server/target/engine-server-0.1.0-SNAPSHOT.jar

Testcontainers需要可用Docker。独立进程脚本创建专用临时MySQL容器，执行三个检查点SIGKILL/新JVM恢复和真实Server WAIT/SSE重启验收，结束后清理测试容器与卷。以上不替代Task14的100并发DURABLE容量基准。
