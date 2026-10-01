package com.playrewards.flowengine.server.config;

import com.playrewards.flowengine.adapter.mysql.*;
import com.playrewards.flowengine.core.durable.DurableExecutionCoordinator;
import com.playrewards.flowengine.core.execution.FlowEngine;

import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.springframework.boot.autoconfigure.*;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.time.Duration;

/**
 * Optional MySQL runtime composition. Credentials come from host properties; no secrets enter
 * checkpoints.
 */
@AutoConfiguration(before = EngineAutoConfiguration.class)
@ConditionalOnProperty(name = "flow-engine.durable.enabled", havingValue = "true")
public class DurableAutoConfiguration {
    @Bean(destroyMethod = "forceCloseAll")
    @ConditionalOnMissingBean(MySqlRuntimeDatabase.class)
    PooledDataSource engineDurableDataSource(Environment env) {
        var source =
                new PooledDataSource(
                        "com.mysql.cj.jdbc.Driver",
                        env.getRequiredProperty("flow-engine.durable.jdbc-url"),
                        env.getRequiredProperty("flow-engine.durable.username"),
                        env.getRequiredProperty("flow-engine.durable.password"));
        source.setPoolMaximumActiveConnections(16);
        source.setPoolMaximumIdleConnections(16);
        source.setPoolTimeToWait(5000);
        return source;
    }

    @Bean
    @ConditionalOnMissingBean
    MySqlRuntimeDatabase engineRuntimeDatabase(PooledDataSource engineDurableDataSource) {
        var db = new MySqlRuntimeDatabase(engineDurableDataSource);
        db.migrate();
        return db;
    }

    @Bean
    @ConditionalOnMissingBean(com.playrewards.flowengine.spi.ExecutionRecordStore.class)
    MySqlTenantStoreRouter durableObservationStore(MySqlRuntimeDatabase db, Environment env) {
        var capacities = new java.util.HashMap<String, Integer>();
        capacities.put(
                "engine_execution",
                env.getProperty("flow-engine.observation.max-executions", Integer.class, 10000));
        capacities.put(
                "engine_node_attempt",
                env.getProperty("flow-engine.observation.max-nodes", Integer.class, 10000));
        for (var entry :
                java.util.Map.of(
                                "engine_event",
                                "events",
                                "engine_log",
                                "logs",
                                "engine_content",
                                "content")
                        .entrySet())
            capacities.put(
                    entry.getKey(),
                    env.getProperty(
                            "flow-engine.observation." + entry.getValue() + ".max-count",
                            Integer.class,
                            10000));
        return new MySqlTenantStoreRouter(
                db,
                capacities,
                retentionPolicy(env),
                env.getProperty("flow-engine.payload.max-bytes", Integer.class, 1048576),
                duration(env, "flow-engine.observation.payloads.retention", "1h"));
    }

    /**
     * 观察记录保留策略：DURABLE 终态默认 7 天，EPHEMERAL 终态与孤儿非终态默认 1 天，
     * 事件/日志/内容沿用 1 小时默认值；全部可由 {@code flow-engine.observation.*} 覆盖。
     */
    static MySqlRetentionPolicy retentionPolicy(Environment env) {
        return new MySqlRetentionPolicy(
                duration(env, "flow-engine.observation.executions.retention", "7d"),
                duration(env, "flow-engine.observation.executions.ephemeral-retention", "1d"),
                duration(env, "flow-engine.observation.executions.stale-retention", "1d"),
                duration(env, "flow-engine.observation.events.retention", "1h"),
                duration(env, "flow-engine.observation.logs.retention", "1h"),
                duration(env, "flow-engine.observation.content.retention", "1h"),
                env.getProperty("flow-engine.observation.cleanup.batch-size", Integer.class, 500));
    }

    private static Duration duration(Environment env, String key, String fallback) {
        return org.springframework.boot.convert.DurationStyle.detectAndParse(
                env.getProperty(key, fallback));
    }

    /** 周期分批清理过期观察记录；每租户每类每周期最多 max-batches 批，避免长时间占用连接。 */
    @Bean
    @ConditionalOnMissingBean
    ObservationRetentionLifecycle observationRetentionLifecycle(
            MySqlRuntimeDatabase db, Environment env) {
        var retention = new MySqlObservationRetention(db, retentionPolicy(env));
        int batches = env.getProperty("flow-engine.observation.cleanup.max-batches", Integer.class, 20);
        return new ObservationRetentionLifecycle(
                () -> retention.purgeExpired(batches),
                duration(env, "flow-engine.observation.cleanup.interval", "1m"));
    }

    @Bean
    @ConditionalOnMissingBean
    DurableExecutionCoordinator durableExecutionCoordinator(
            MySqlRuntimeDatabase db, MySqlTenantStoreRouter router) {
        return new DurableExecutionCoordinator(
                router::durableStores, new MySqlIdempotencyStore(db), Duration.ofSeconds(15));
    }

    @Bean
    @ConditionalOnMissingBean(com.playrewards.flowengine.spi.DistributedLockProvider.class)
    MySqlDistributedLockProvider durableBusinessLocks(MySqlRuntimeDatabase db) {
        return new MySqlDistributedLockProvider(db);
    }

    @Bean
    DurableRecoveryLifecycle durableRecoveryLifecycle(
            MySqlTenantStoreRouter router, FlowEngine engine) {
        return new DurableRecoveryLifecycle(router, engine);
    }
}
