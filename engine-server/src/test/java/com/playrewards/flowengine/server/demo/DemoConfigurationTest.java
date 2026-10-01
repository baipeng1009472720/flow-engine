package com.playrewards.flowengine.server.demo;

import static org.junit.jupiter.api.Assertions.*;

import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy;
import com.playrewards.flowengine.spi.FlowBundleLoader;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.web.MockHttpServletRequest;

/** The optional demo is closed by default and never accepts a caller-supplied tenant. */
class DemoConfigurationTest {
    private final ApplicationContextRunner contexts =
            new ApplicationContextRunner().withUserConfiguration(DemoConfiguration.class);

    @Test
    void demo_requires_explicit_flag_and_loopback_server_binding() {
        contexts.run(
                context -> assertTrue(context.getBeansOfType(FlowBundleLoader.class).isEmpty()));
        contexts.withPropertyValues("flow-engine.demo.enabled=true")
                .run(context -> assertNotNull(context.getStartupFailure()));
        contexts.withPropertyValues("flow-engine.demo.enabled=true", "server.address=0.0.0.0")
                .run(context -> assertNotNull(context.getStartupFailure()));
    }

    @Test
    void local_demo_policy_rejects_remote_requests_and_other_tenants() {
        contexts.withPropertyValues("flow-engine.demo.enabled=true", "server.address=127.0.0.1")
                .run(
                        context -> {
                            var policy = context.getBean(ExecutionAccessPolicy.class);
                            var request = new MockHttpServletRequest();
                            request.setLocalAddr("127.0.0.1");
                            request.setRemoteAddr("127.0.0.1");
                            request.addHeader("X-Tenant-Id", "forged");
                            var caller = policy.capture(request);
                            assertEquals("demo", caller.tenantId());
                            policy.authorize(
                                    caller,
                                    new FlowIdentity("demo", "main", "v1"),
                                    ExecutionAccessPolicy.Action.EXECUTE);
                            assertThrows(
                                    SecurityException.class,
                                    () ->
                                            policy.authorize(
                                                    caller,
                                                    new FlowIdentity("other", "main", "v1"),
                                                    ExecutionAccessPolicy.Action.READ));
                            assertThrows(
                                    SecurityException.class,
                                    () ->
                                            policy.authorize(
                                                    () -> "demo",
                                                    new FlowIdentity("demo", "main", "v1"),
                                                    ExecutionAccessPolicy.Action.READ));
                            request.setRemoteAddr("192.0.2.1");
                            assertThrows(SecurityException.class, () -> policy.capture(request));
                        });
    }
}
