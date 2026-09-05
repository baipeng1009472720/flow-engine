package com.playrewards.flowengine.server.api;

import com.alibaba.fastjson2.JSON;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * One controlled Fastjson2 error envelope; arbitrary exception text and stack traces never reach
 * clients.
 */
@RestControllerAdvice(basePackages = "com.playrewards.flowengine.server.api")
public class ExecutionErrors {
    /** Classifies only recognized failures and suppresses unknown exception messages and stack traces. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handle(Exception failure) {
        int status = 500;
        String code = "INTERNAL", category = "INTERNAL";
        if (failure instanceof ApiException api) {
            status = api.status();
            code = api.code();
            category = api.category();
        } else if (failure instanceof SecurityException) {
            status = 403;
            code = "FORBIDDEN";
            category = "AUTHORIZATION";
        } else if (failure instanceof IllegalArgumentException
                || failure instanceof com.alibaba.fastjson2.JSONException) {
            status = 400;
            code = "INVALID_REQUEST";
            category = "CONTRACT";
        } else if (failure instanceof IllegalStateException
                && failure.getMessage() != null
                && failure.getMessage().matches("[A-Z_]*CAPACITY_EXCEEDED")) {
            status = 429;
            code = "RESOURCE_LIMIT";
            category = "RESOURCE_LIMIT";
        }
        var body = new LinkedHashMap<String, Object>();
        body.put("code", code);
        body.put("category", category);
        body.put("message", code);
        body.put("retryable", status == 429);
        body.put("executionId", null);
        body.put("nodeCode", null);
        body.put("scopePath", null);
        body.put("attempt", 0);
        body.put("details", Map.of());
        return ResponseEntity.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .body(JSON.toJSONString(body, com.alibaba.fastjson2.JSONWriter.Feature.WriteNulls));
    }
}
