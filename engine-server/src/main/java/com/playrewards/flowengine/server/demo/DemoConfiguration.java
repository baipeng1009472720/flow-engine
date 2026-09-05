package com.playrewards.flowengine.server.demo;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.core.compile.RelatedFlowResolver;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Set;

/**
 * Explicit local-only examples; production hosts must supply their own trusted access policy and
 * loaders.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "flow-engine.demo.enabled", havingValue = "true")
public class DemoConfiguration {
    private static final Set<String> LOOPBACK =
            Set.of("127.0.0.1", "::1", "[::1]", "0:0:0:0:0:0:0:1");

    /** Rejects wildcard/default binding before publishing any unauthenticated demo adapters. */
    public DemoConfiguration(Environment environment) {
        if (!LOOPBACK.contains(environment.getProperty("server.address", ""))) {
            throw new IllegalStateException("Demo mode requires explicit loopback server.address");
        }
    }

    /** Provides only the finite immutable demo catalog, not filesystem or network loading. */
    @Bean
    public DemoBundleLoader demoBundleLoader(DemoHttpEndpoint endpoint) {
        return new DemoBundleLoader(endpoint.url());
    }

    /** Starts a static local acceptance target, with no forwarding or external dependency. */
    @Bean
    public DemoHttpEndpoint demoHttpEndpoint() throws java.io.IOException {
        return new DemoHttpEndpoint();
    }

    /**
     * Uses real address-pinned transport while allowing only the fixture owned by this demo
     * context.
     */
    @Bean
    public com.playrewards.flowengine.adapter.http.JdkHttpNodeClient demoHttpClient(
            DemoHttpEndpoint endpoint) {
        return new com.playrewards.flowengine.adapter.http.JdkHttpNodeClient(
                endpoint.policy(), endpoint.policy(), 8, java.time.Duration.ofMillis(10));
    }

    /** Keeps child resolution inside the fixed demo catalog and tenant. */
    @Bean
    public RelatedFlowResolver demoRelatedFlowResolver(DemoBundleLoader loader) {
        return loader::resolve;
    }

    /** Registers a demand-driven example with no external side effects. */
    @Bean
    public DemoStreamComponent demoStreamComponent() {
        return new DemoStreamComponent();
    }

    /** Registers a cooperatively cancellable example without an extra worker pool. */
    @Bean
    public DemoSlowComponent demoSlowComponent() {
        return new DemoSlowComponent();
    }

    /** Local demonstration access is intentionally separate from production authentication. */
    @Bean
    public ExecutionAccessPolicy demoExecutionAccessPolicy() {
        return new ExecutionAccessPolicy() {
            @Override
            public String tenantId(HttpServletRequest request) {
                // Ignore forwarded/tenant headers: only the actual socket endpoints establish local
                // access.
                if (!LOOPBACK.contains(request.getLocalAddr())
                        || !LOOPBACK.contains(request.getRemoteAddr())) {
                    throw new SecurityException("Local demo access required");
                }
                return "demo";
            }

            @Override
            public void authorize(String tenantId, FlowIdentity flow, Action action) {
                if (!"demo".equals(tenantId)
                        || flow == null
                        || !"demo".equals(flow.tenantId())
                        || !"v1".equals(flow.version())
                        || action == null) {
                    throw new SecurityException("Demo flow access denied");
                }
            }
        };
    }
}
