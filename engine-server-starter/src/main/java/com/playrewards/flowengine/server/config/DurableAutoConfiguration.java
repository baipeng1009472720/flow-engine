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
        source.setPoolMaximumIdleConnections(4);
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
        var retentions = new java.util.HashMap<String, Duration>();
        for (var entry :
                java.util.Map.of(
                                "engine_event",
                                "events",
                                "engine_log",
                                "logs",
                                "engine_content",
                                "content")
                        .entrySet()) {
            capacities.put(
                    entry.getKey(),
                    env.getProperty(
                            "flow-engine.observation." + entry.getValue() + ".max-count",
                            Integer.class,
                            10000));
            retentions.put(
                    entry.getKey(),
                    org.springframework.boot.convert.DurationStyle.detectAndParse(
                            env.getProperty(
                                    "flow-engine.observation." + entry.getValue() + ".retention",
                                    "1h")));
        }
        return new MySqlTenantStoreRouter(
                db,
                capacities,
                retentions,
                env.getProperty("flow-engine.payload.max-bytes", Integer.class, 1048576),
                org.springframework.boot.convert.DurationStyle.detectAndParse(
                        env.getProperty("flow-engine.observation.payloads.retention", "1h")));
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
