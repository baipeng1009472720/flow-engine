package com.playrewards.flowengine.server.api;

import com.playrewards.flowengine.server.TestMySql;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.alibaba.fastjson2.JSON;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/** Real MySQL behind the authenticated Task 9 HTTP boundary; mode does not bypass tenant checks. */
@SpringBootTest(
        classes = ExecutionRequestContractTest.Host.class,
        properties = "flow-engine.durable.enabled=true")
@AutoConfigureMockMvc
class DurableApiTest {
    static final TestMySql MYSQL = TestMySql.start();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("flow-engine.durable.jdbc-url", MYSQL::getJdbcUrl);
        registry.add("flow-engine.durable.username", MYSQL::getUsername);
        registry.add("flow-engine.durable.password", MYSQL::getPassword);
    }

    @Autowired MockMvc mvc;

    @Test
    void durableWaitResumeAndTenantAuthorizationUseSamePublicContract() throws Exception {
        String body =
                mvc.perform(
                                post("/v1/executions")
                                        .principal(() -> "tenant")
                                        .contentType("application/json")
                                        .content(
                                                "{\"flowReference\":\"test:flow\",\"mode\":\"DURABLE\"}"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                        .andReturn()
                        .getResponse()
                        .getContentAsString();
        String id = JSON.parseObject(body).getString("executionId");
        mvc.perform(post("/v1/executions/" + id + "/resume").principal(() -> "other"))
                .andExpect(status().isForbidden());
        mvc.perform(post("/v1/executions/" + id + "/resume").principal(() -> "tenant"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.executionId").value(id));
        mvc.perform(get("/v1/executions/" + id).principal(() -> "tenant"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));
    }
}
