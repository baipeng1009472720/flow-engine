package com.playrewards.flowengine.server.api;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.Action;
import com.playrewards.flowengine.server.security.ExecutionAccessPolicy.CapturedAuthorization;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.*;

import java.util.*;

/** Read APIs keep lifecycle, diagnostics and separately authorized full payloads distinct. */
@RestController
public class ExecutionQueryController {
    private final ExecutionService service;

    /** All queries share the same trusted identity and per-flow permission service. */
    public ExecutionQueryController(ExecutionService service) {
        this.service = service;
    }

    /** Returns a safe execution summary, never its full result value. */
    @GetMapping(value = "/v1/executions/{id}", produces = "application/json")
    public String execution(HttpServletRequest http, @PathVariable("id") String id) {
        return JSON.toJSONString(service.summary(service.execution(http, id, Action.READ)));
    }

    /** Pages actual attempt records with their owning flow identity and timing. */
    @GetMapping(value = "/v1/executions/{id}/nodes", produces = "application/json")
    public String nodes(
            HttpServletRequest http,
            @PathVariable("id") String id,
            @RequestParam(value = "offset", defaultValue = "0") int offset,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        service.execution(http, id, Action.READ);
        return JSON.toJSONString(
                page(service.store().nodes(id), offset, limit).stream()
                        .map(service::nodeSummary)
                        .toList());
    }

    /**
     * Reads a bounded ordered event page; content stays behind independently authorized references.
     */
    @GetMapping(value = "/v1/executions/{id}/events", produces = "application/json")
    public String events(
            HttpServletRequest http,
            @PathVariable("id") String id,
            @RequestParam(value = "afterSequence", defaultValue = "-1") long after,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        CapturedAuthorization caller = service.capture(http);
        service.execution(caller, id, Action.READ);
        return JSON.toJSONString(
                page(
                        service.store().readAfter(id, after).stream()
                                .filter(event -> service.eventVisible(caller, event))
                                .toList(),
                        0,
                        limit));
    }

    /** Resolves one opaque attempt identity after checking its execution permission. */
    @GetMapping(value = "/v1/node-executions/{id}", produces = "application/json")
    public String node(HttpServletRequest http, @PathVariable("id") String id) {
        return JSON.toJSONString(service.nodeSummary(service.node(http, id, Action.READ)));
    }

    /** Applies LOGS permission and pagination independently from full diagnostic payload access. */
    @GetMapping(value = "/v1/node-executions/{id}/logs", produces = "application/json")
    public String logs(
            HttpServletRequest http,
            @PathVariable("id") String id,
            @RequestParam(value = "offset", defaultValue = "0") int offset,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        service.node(http, id, Action.LOGS);
        return JSON.toJSONString(page(service.store().logs(id), offset, limit));
    }

    /** Requires PAYLOAD permission on the actual owner, including child-execution payloads. */
    @GetMapping(value = "/v1/payloads/{id}", produces = "application/json")
    public String payload(HttpServletRequest http, @PathVariable("id") String id) {
        return JSON.toJSONString(service.payload(http, id));
    }

    private static <T> List<T> page(List<T> rows, int offset, int limit) {
        if (offset < 0 || offset > 100000 || limit < 1 || limit > 500)
            throw new IllegalArgumentException("invalid pagination");
        return rows.stream().skip(offset).limit(limit).toList();
    }
}
