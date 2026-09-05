package com.playrewards.flowengine.server.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Independent host uses imports metadata and must deny execution when no trusted identity adapter
 * exists.
 */
@SpringBootTest(classes = ExecutionApiTest.Host.class)
@AutoConfigureMockMvc
class ExecutionApiTest {
    @Configuration
    @EnableAutoConfiguration
    static class Host {}

    @Autowired MockMvc mvc;

    @Test
    void refuses_execution_without_a_host_identity_adapter() throws Exception {
        mvc.perform(post("/v1/executions").contentType("application/json").content("{}"))
                .andExpect(status().isForbidden())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("FORBIDDEN")));
    }

    @Test
    void health_does_not_require_execution_identity() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }
}
