package com.playrewards.flowengine.server.api;

import com.alibaba.fastjson2.JSON;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/** Standard start/cancel/resume contract shared by generic and private hosts. */
@RestController
public class ExecutionController {
    private final ExecutionService service;

    /** Uses the authenticated service boundary for every mutating endpoint. */
    public ExecutionController(ExecutionService service) {
        this.service = service;
    }

    /**
     * Starts one execution; WAIT exposes the terminal summary and ASYNC returns its stable
     * identity.
     */
    @PostMapping(value = "/v1/executions", produces = "application/json")
    public ResponseEntity<String> start(HttpServletRequest request) throws Exception {
        var started = service.start(request);
        if (started.responseMode().equals("ASYNC"))
            return ResponseEntity.accepted()
                    .body(
                            JSON.toJSONString(
                                    Map.of(
                                            "executionId",
                                            started.handle().executionId(),
                                            "status",
                                            "ACCEPTED")));
        return ResponseEntity.ok(JSON.toJSONString(service.summary(service.waitFor(started))));
    }

    /**
     * Requests explicit cancellation after per-flow authorization; repeated cancellation is
     * harmless.
     */
    @PostMapping(value = "/v1/executions/{id}/cancel", produces = "application/json")
    public String cancel(HttpServletRequest request, @PathVariable("id") String id) {
        return JSON.toJSONString(service.cancel(request, id));
    }

    /**
     * Resumes authenticated durable state; EPHEMERAL and unconfirmed side effects cannot be
     * replayed.
     */
    @PostMapping(value = "/v1/executions/{id}/resume", produces = "application/json")
    public String resume(HttpServletRequest request, @PathVariable("id") String id) {
        return JSON.toJSONString(service.resume(request, id));
    }
}
