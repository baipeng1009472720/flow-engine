package com.playrewards.flowengine.server.http;

import com.playrewards.flowengine.adapter.http.DefaultHttpTargetPolicy;
import com.playrewards.flowengine.adapter.http.JdkHttpNodeClient;
import com.playrewards.flowengine.spi.HttpNodeClient;
import com.playrewards.flowengine.spi.HttpTargetPolicy;
import java.time.Duration;

/**
 * Starter-owned factory for host HTTP composition. Hosts provide policy through SPI without
 * importing adapter implementation classes.
 */
public final class HttpNodeClients {

    private HttpNodeClients() {}

    /**
     * Creates the standard public-network transport with a host-specific trial target policy.
     *
     * @param trialPolicy target policy used only for trial executions
     * @param maxConcurrentRequests maximum active requests and bounded waiters
     * @param admissionPoll interval used while waiting for transport admission
     * @return an SPI client whose concrete implementation is owned by the Starter
     */
    public static HttpNodeClient createDefault(
            HttpTargetPolicy trialPolicy, int maxConcurrentRequests, Duration admissionPoll) {
        return new JdkHttpNodeClient(
                new DefaultHttpTargetPolicy(),
                trialPolicy,
                maxConcurrentRequests,
                admissionPoll);
    }
}
