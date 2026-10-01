package com.playrewards.flowengine.server.config;

import com.playrewards.flowengine.adapter.groovy.SandboxedGroovyRuntime;
import com.playrewards.flowengine.adapter.http.JdkHttpNodeClient;
import com.playrewards.flowengine.adapter.memory.*;
import com.playrewards.flowengine.core.compile.*;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.event.BoundedExecutionEventBus;
import com.playrewards.flowengine.core.execution.*;
import com.playrewards.flowengine.core.node.NodeExecutorRegistry;
import com.playrewards.flowengine.core.observation.ExecutionRecorder;
import com.playrewards.flowengine.server.api.*;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;
import com.playrewards.flowengine.spi.*;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.time.Clock;
import java.util.*;

/**
 * Reusable runtime assembly, independently discovered by any Boot host through imports metadata.
 */
@AutoConfiguration
@Import({
    ExecutionController.class,
    ExecutionQueryController.class,
    ExecutionSseController.class,
    ExecutionErrors.class
})
public class EngineAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    Clock engineClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean(ExecutionRecordStore.class)
    InMemoryObservationStore observationStore(
            org.springframework.core.env.Environment environment, Clock clock) {
        var d = InMemoryObservationStore.Limits.defaults();
        return new InMemoryObservationStore(
                new InMemoryObservationStore.Limits(
                        limit(environment, "observation.max-executions", d.maxExecutions()),
                        limit(environment, "observation.max-nodes", d.maxNodes()),
                        limit(environment, "observation.events.max-count", d.maxEvents()),
                        limit(environment, "observation.logs.max-count", d.maxLogs()),
                        limit(environment, "observation.content.max-count", d.maxContentChunks()),
                        limit(environment, "observation.payloads.max-count", d.maxPayloads()),
                        limit(environment, "payload.max-bytes", d.maxPayloadBytes()),
                        limit(environment, "observation.events.max-bytes", d.maxEventBytes()),
                        limit(environment, "observation.logs.max-bytes", d.maxLogBytes()),
                        retention(environment, "executions"),
                        retention(environment, "events"),
                        retention(environment, "logs"),
                        retention(environment, "payloads")),
                clock);
    }

    @Bean
    @ConditionalOnMissingBean(PayloadStore.class)
    PayloadStore payloadStore(ExecutionRecordStore records) {
        if (records instanceof PayloadStore payloads) return payloads;
        throw new IllegalStateException(
                "custom ExecutionRecordStore requires separate PayloadStore");
    }

    @Bean
    @ConditionalOnMissingBean
    ExecutionAccessPolicy executionAccessPolicy() {
        return new ExecutionAccessPolicy() {
            public CapturedAuthorization capture(
                    jakarta.servlet.http.HttpServletRequest request) {
                throw new SecurityException();
            }

            public void authorize(
                    CapturedAuthorization caller,
                    com.playrewards.flowengine.contract.bundle.FlowIdentity flow,
                    Action action) {
                throw new SecurityException();
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    ExecutionContextLoader executionContextLoader() {
        return request -> {
            throw new SecurityException();
        };
    }

    @Bean
    @ConditionalOnMissingBean
    FlowBusinessComponentRegistry businessComponents(List<FlowBusinessComponent> beans) {
        return new FlowBusinessComponentRegistry(beans);
    }

    @Bean
    @ConditionalOnMissingBean
    FlowCompiler flowCompiler(Optional<RelatedFlowResolver> resolver) {
        return new FlowCompiler(resolver.orElse(null));
    }

    @Bean
    @ConditionalOnMissingBean
    ScriptRuntime scriptRuntime() {
        return new SandboxedGroovyRuntime();
    }

    @Bean
    @ConditionalOnMissingBean
    DistributedLockProvider distributedLockProvider() {
        return new InMemoryLockProvider();
    }

    @Bean
    @ConditionalOnMissingBean
    HttpNodeClient httpNodeClient() {
        return new JdkHttpNodeClient();
    }

    @Bean
    @ConditionalOnMissingBean
    ConcurrencyGovernor concurrencyGovernor(
            org.springframework.core.env.Environment environment, Clock clock) {
        return new ConcurrencyGovernor(
                limit(environment, "concurrency.global", 1024),
                limit(environment, "concurrency.per-tenant", 256),
                limit(environment, "concurrency.per-flow", 256),
                limit(environment, "concurrency.per-component", 128),
                limit(environment, "concurrency.per-resource", 64),
                clock);
    }

    @Bean
    @ConditionalOnMissingBean
    BoundedExecutionEventBus executionEventBus(
            ExecutionRecordStore store, org.springframework.core.env.Environment environment) {
        return new BoundedExecutionEventBus(
                store,
                limit(environment, "events.max-subscribers", 512),
                limit(environment, "events.buffer-per-execution", 256));
    }

    @Bean
    @ConditionalOnMissingBean
    ExecutionRecorder executionRecorder(
            ExecutionRecordStore store,
            PayloadStore payloads,
            BoundedExecutionEventBus bus,
            Clock clock,
            @org.springframework.beans.factory.annotation.Value(
                            "${flow-engine.observation.strict-audit:false}")
                    boolean strictAudit) {
        return new ExecutionRecorder(store, payloads, bus, clock, strictAudit);
    }

    @Bean
    @ConditionalOnMissingBean
    FlowEngine flowEngine(
            ScriptRuntime scripts,
            FlowBusinessComponentRegistry components,
            DistributedLockProvider locks,
            HttpNodeClient http,
            Clock clock,
            ConcurrencyGovernor governor,
            ExecutionRecorder recorder,
            Optional<com.playrewards.flowengine.core.durable.DurableExecutionCoordinator> durable) {
        return new FlowEngine(
                NodeExecutorRegistry.runtimeBuiltIns(scripts, components, locks, http, clock),
                governor,
                clock,
                recorder,
                durable.orElse(null));
    }

    @Bean
    @ConditionalOnMissingBean
    ExecutionService executionService(
            FlowEngine engine,
            FlowCompiler compiler,
            FlowBusinessComponentRegistry components,
            List<FlowBundleLoader> loaders,
            ExecutionContextLoader contexts,
            ExecutionAccessPolicy access,
            ExecutionRecordStore store,
            PayloadStore payloads,
            org.springframework.core.env.Environment environment) {
        return new ExecutionService(
                engine,
                compiler,
                components,
                loaders,
                contexts,
                access,
                store,
                payloads,
                org.springframework.boot.convert.DurationStyle.detectAndParse(
                        environment.getProperty("flow-engine.execution.max-duration", "5m")));
    }

    private static int limit(
            org.springframework.core.env.Environment environment, String path, int fallback) {
        return environment.getProperty("flow-engine." + path, Integer.class, fallback);
    }

    private static java.time.Duration retention(
            org.springframework.core.env.Environment environment, String kind) {
        return org.springframework.boot.convert.DurationStyle.detectAndParse(
                environment.getProperty("flow-engine.observation." + kind + ".retention", "1h"));
    }
}
