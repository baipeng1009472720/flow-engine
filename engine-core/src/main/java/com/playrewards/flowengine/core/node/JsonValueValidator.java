package com.playrewards.flowengine.core.node;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.schema.JSONSchema;
import com.alibaba.fastjson2.schema.ValidateResult;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.util.Objects;

/** Runtime JSON Schema validator shared by START input and declared node-output checks. */
public final class JsonValueValidator {

    private JsonValueValidator() {
    }

    /**
     * Validates a concrete JSON value against a compiled protocol snapshot.
     *
     * @param value JSON value, including null
     * @param protocol immutable target protocol
     * @param errorCode stable error code used on rejection
     * @param field diagnostic field path
     */
    public static void validate(Object value, ProtocolSchema protocol, String errorCode, String field) {
        Objects.requireNonNull(protocol, "protocol must not be null");
        JSONObject schemaObject = JSON.parseObject(JSON.toJSONString(protocol.schema()));
        JSONSchema schema = JSONSchema.of(schemaObject);
        ValidateResult result = schema.validate(JSON.toJSON(value));
        if (!result.isSuccess()) {
            throw new NodeExecutionException(errorCode,
                    field + " does not satisfy protocol " + protocol.code() + ": " + result.getMessage());
        }
    }
}
