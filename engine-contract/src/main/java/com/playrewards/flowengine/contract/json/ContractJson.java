package com.playrewards.flowengine.contract.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.playrewards.flowengine.contract.bundle.BundleTrust;
import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * JSON codec and canonical SHA-256 utility for the versioned bundle boundary.
 *
 * <p>Only this codec applies the public top-level unknown-field policy. Nested configuration and JSON
 * Schema objects deliberately remain extensible JSON documents. Hashing serializes a recursively sorted
 * object graph in UTF-8 while preserving array order.</p>
 */
public final class ContractJson {

    private static final Set<String> BUNDLE_FIELDS = Set.of("contractVersion", "flowIdentity", "flowDefinition",
            "componentContracts", "protocolSchemas", "externalScopeSchemas", "bundleHash", "trust");
    private static final Set<String> FLOW_IDENTITY_FIELDS = Set.of("tenantId", "flowId", "version");
    private static final Set<String> FLOW_DEFINITION_FIELDS = Set.of("schemaVersion", "nodes", "edges");
    private static final Set<String> FLOW_NODE_FIELDS = Set.of("id", "componentCode", "nodeCode", "nodeName", "config");
    private static final Set<String> FLOW_EDGE_FIELDS = Set.of("id", "sourceNodeId", "targetNodeId", "sourcePort", "targetPort");
    private static final Set<String> COMPONENT_FIELDS = Set.of("componentCode", "componentType", "executorRef",
            "configSchema", "inputSlots", "outputSlots", "capabilities");
    private static final Set<String> CAPABILITY_FIELDS = Set.of("effect", "supportsTrial", "streaming", "retryable",
            "maxConcurrency");
    private static final Set<String> PROTOCOL_SCHEMA_FIELDS = Set.of("code", "version", "schema");
    private static final Set<String> TRUST_FIELDS = Set.of("issuer", "keyId", "signature");

    private ContractJson() {
    }

    /**
     * Parses an executable bundle while rejecting unknown top-level fields.
     *
     * @param json UTF-8 JSON object supplied by a trusted host
     * @return immutable validated bundle
     * @throws IllegalArgumentException when the document is malformed, unknown or version-incompatible
     */
    public static ExecutableFlowBundle fromBundleJson(String json) {
        JSONObject object = JSON.parseObject(JsonValues.requireText(json, "bundle JSON"));
        if (object == null) {
            throw new IllegalArgumentException("bundle JSON must be an object");
        }
        validateBundleStructure(object);
        try {
            return object.toJavaObject(ExecutableFlowBundle.class);
        } catch (RuntimeException exception) {
            IllegalArgumentException contractFailure = findContractFailure(exception);
            if (contractFailure != null) {
                throw contractFailure;
            }
            throw new IllegalArgumentException("invalid executable bundle JSON", exception);
        }
    }

    /** Serializes a bundle using its public JSON field names. */
    public static String toBundleJson(ExecutableFlowBundle bundle) {
        return JSON.toJSONString(bundle);
    }

    /**
     * Calculates a SHA-256 lowercase hexadecimal digest from the canonical bundle body.
     *
     * @param bundle static runtime data to fingerprint
     * @return 64-character lowercase hexadecimal SHA-256 digest
     */
    public static String bundleHash(ExecutableFlowBundle bundle) {
        byte[] canonicalJson = JSON.toJSONString(canonicalBundleBody(bundle)).getBytes(StandardCharsets.UTF_8);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonicalJson);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format("%02x", value));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
        }
    }

    private static Map<String, Object> canonicalBundleBody(ExecutableFlowBundle bundle) {
        Map<String, Object> body = new TreeMap<>();
        body.put("contractVersion", bundle.contractVersion());
        body.put("flowIdentity", flowIdentity(bundle.flowIdentity()));
        body.put("flowDefinition", flowDefinition(bundle.flowDefinition()));
        body.put("componentContracts", contracts(bundle.componentContracts()));
        body.put("protocolSchemas", schemas(bundle.protocolSchemas()));
        body.put("externalScopeSchemas", schemas(bundle.externalScopeSchemas()));
        body.put("trust", trust(bundle.trust()));
        return body;
    }

    private static Map<String, Object> flowIdentity(FlowIdentity identity) {
        return sorted(Map.of("tenantId", identity.tenantId(), "flowId", identity.flowId(), "version", identity.version()));
    }

    private static Map<String, Object> flowDefinition(FlowDefinition definition) {
        List<Object> nodes = new ArrayList<>(definition.nodes().stream().map(ContractJson::flowNode).toList());
        List<Object> edges = new ArrayList<>(definition.edges().stream().map(ContractJson::flowEdge).toList());
        return sorted(Map.of("schemaVersion", definition.schemaVersion(), "nodes", nodes, "edges", edges));
    }

    private static Map<String, Object> flowNode(FlowNode node) {
        return sorted(Map.of("id", node.id(), "componentCode", node.componentCode(), "nodeCode", node.nodeCode(),
                "nodeName", node.nodeName(), "config", node.config()));
    }

    private static Map<String, Object> flowEdge(FlowEdge edge) {
        return sorted(Map.of("id", edge.id(), "sourceNodeId", edge.sourceNodeId(), "targetNodeId", edge.targetNodeId(),
                "sourcePort", edge.sourcePort(), "targetPort", edge.targetPort()));
    }

    private static Map<String, Object> contracts(Map<String, CompiledComponentContract> values) {
        Map<String, Object> result = new TreeMap<>();
        values.forEach((code, contract) -> {
            Map<String, Object> contractJson = new LinkedHashMap<>();
            contractJson.put("componentCode", contract.componentCode());
            contractJson.put("componentType", contract.componentType().name());
            contractJson.put("executorRef", contract.executorRef());
            contractJson.put("configSchema", contract.configSchema());
            contractJson.put("inputSlots", schemas(contract.inputSlots()));
            contractJson.put("outputSlots", schemas(contract.outputSlots()));
            contractJson.put("capabilities", capabilities(contract.capabilities()));
            result.put(code, sorted(contractJson));
        });
        return result;
    }

    private static Map<String, Object> schemas(Map<String, ProtocolSchema> values) {
        Map<String, Object> result = new TreeMap<>();
        values.forEach((code, schema) -> result.put(code,
                sorted(Map.of("code", schema.code(), "version", schema.version(), "schema", schema.schema()))));
        return result;
    }

    private static Map<String, Object> capabilities(ComponentCapabilities capabilities) {
        return sorted(Map.of("effect", capabilities.effect().name(), "supportsTrial", capabilities.supportsTrial(),
                "streaming", capabilities.streaming(), "retryable", capabilities.retryable(), "maxConcurrency",
                capabilities.maxConcurrency()));
    }

    private static Map<String, Object> trust(BundleTrust trust) {
        if (trust == null) {
            return Map.of();
        }
        Map<String, Object> result = new TreeMap<>();
        if (trust.issuer() != null) {
            result.put("issuer", trust.issuer());
        }
        if (trust.keyId() != null) {
            result.put("keyId", trust.keyId());
        }
        return result;
    }

    private static Map<String, Object> sorted(Map<String, ?> values) {
        return canonicalize(values);
    }

    @SuppressWarnings("unchecked")
    private static <T> T canonicalize(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new TreeMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("canonical JSON object keys must be strings");
                }
                result.put(key, canonicalize(entry.getValue()));
            }
            return (T) result;
        }
        if (value instanceof Collection<?> values) {
            List<Object> result = new ArrayList<>(values.size());
            for (Object item : values) {
                result.add(canonicalize(item));
            }
            return (T) result;
        }
        return (T) value;
    }

    private static IllegalArgumentException findContractFailure(Throwable exception) {
        for (Throwable current = exception; current != null; current = current.getCause()) {
            if (current instanceof IllegalArgumentException failure) {
                return failure;
            }
        }
        return null;
    }

    private static void validateBundleStructure(JSONObject bundle) {
        validateFields(bundle, BUNDLE_FIELDS, "executable bundle");
        validateFields(object(bundle, "flowIdentity"), FLOW_IDENTITY_FIELDS, "flowIdentity");
        JSONObject definition = object(bundle, "flowDefinition");
        validateFields(definition, FLOW_DEFINITION_FIELDS, "flowDefinition");
        validateNodes(array(definition, "nodes"));
        validateEdges(array(definition, "edges"));
        validateComponentContracts(object(bundle, "componentContracts"));
        validateSchemas(object(bundle, "protocolSchemas"), "protocolSchemas");
        validateSchemas(object(bundle, "externalScopeSchemas"), "externalScopeSchemas");
        Object trust = bundle.get("trust");
        if (trust != null) {
            validateFields(asObject(trust, "trust"), TRUST_FIELDS, "trust");
        }
    }

    private static void validateNodes(JSONArray nodes) {
        for (int index = 0; index < nodes.size(); index++) {
            validateFields(asObject(nodes.get(index), "flowDefinition.nodes[" + index + "]"), FLOW_NODE_FIELDS,
                    "flowDefinition.nodes[" + index + "]");
        }
    }

    private static void validateEdges(JSONArray edges) {
        for (int index = 0; index < edges.size(); index++) {
            validateFields(asObject(edges.get(index), "flowDefinition.edges[" + index + "]"), FLOW_EDGE_FIELDS,
                    "flowDefinition.edges[" + index + "]");
        }
    }

    private static void validateComponentContracts(JSONObject contracts) {
        for (Map.Entry<String, Object> entry : contracts.entrySet()) {
            JSONObject contract = asObject(entry.getValue(), "componentContracts." + entry.getKey());
            validateFields(contract, COMPONENT_FIELDS, "componentContracts." + entry.getKey());
            Object capabilities = contract.get("capabilities");
            if (capabilities != null) {
                validateFields(asObject(capabilities, "component capabilities"), CAPABILITY_FIELDS,
                        "component capabilities");
            }
            validateSchemas(object(contract, "inputSlots"), "component inputSlots");
            validateSchemas(object(contract, "outputSlots"), "component outputSlots");
        }
    }

    private static void validateSchemas(JSONObject schemas, String fieldName) {
        for (Map.Entry<String, Object> entry : schemas.entrySet()) {
            validateFields(asObject(entry.getValue(), fieldName + "." + entry.getKey()), PROTOCOL_SCHEMA_FIELDS,
                    fieldName + "." + entry.getKey());
        }
    }

    private static void validateFields(JSONObject object, Set<String> fields, String location) {
        for (String field : object.keySet()) {
            if (!fields.contains(field)) {
                throw new IllegalArgumentException("unknown " + location + " field: " + field);
            }
        }
    }

    private static JSONObject object(JSONObject parent, String fieldName) {
        return asObject(parent.get(fieldName), fieldName);
    }

    private static JSONObject asObject(Object value, String location) {
        if (value instanceof JSONObject object) {
            return object;
        }
        throw new IllegalArgumentException(location + " must be an object");
    }

    private static JSONArray array(JSONObject parent, String fieldName) {
        Object value = parent.get(fieldName);
        if (value instanceof JSONArray array) {
            return array;
        }
        throw new IllegalArgumentException(fieldName + " must be an array");
    }
}
