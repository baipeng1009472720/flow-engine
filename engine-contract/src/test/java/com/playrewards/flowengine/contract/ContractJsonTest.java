package com.playrewards.flowengine.contract;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.playrewards.flowengine.contract.bundle.BundleTrust;
import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.json.ContractJson;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Verifies the versioned JSON boundary exposed by the standalone runtime engine. */
class ContractJsonTest {

    @Test
    void slot_contract_round_trip_preserves_names_required_flag_protocol_identity_and_schema() {
        ProtocolSchema protocol = new ProtocolSchema("order", "1.0", Map.of("type", "object"));
        CompiledComponentContract component = new CompiledComponentContract(
                "business.reward", ComponentType.BUSINESS, "rewardExecutor", Map.of(),
                Map.of("order", new InputSlotContract("订单", true, protocol)),
                Map.of("result", new OutputSlotContract("结果", protocol)),
                new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 1));
        ExecutableFlowBundle original = new ExecutableFlowBundle(
                "1.0", new FlowIdentity("tenant", "flow", "v1"),
                new FlowDefinition("2.0", List.of(), List.of()), Map.of("business.reward", component),
                Map.of("order", protocol), Map.of(), null, null);

        String json = ContractJson.toBundleJson(original);
        ExecutableFlowBundle restored = ContractJson.fromBundleJson(json);

        InputSlotContract input = restored.componentContracts().get("business.reward").inputSlots().get("order");
        OutputSlotContract output = restored.componentContracts().get("business.reward").outputSlots().get("result");
        assertEquals("订单", input.name());
        assertTrue(input.required());
        assertEquals("order", input.protocol().code());
        assertEquals(Map.of("type", "object"), input.protocol().schema());
        assertEquals("结果", output.name());
        assertEquals("order", output.protocol().code());
        assertFalse(json.contains("\"outputSlots\":{\"result\":{\"required\""));
        JSONObject optionalInput = JSON.parseObject(json);
        optionalInput.getJSONObject("componentContracts").getJSONObject("business.reward")
                .getJSONObject("inputSlots").getJSONObject("order").put("required", false);
        assertNotEquals(original.calculateBundleHash(),
                ContractJson.fromBundleJson(optionalInput.toJSONString()).calculateBundleHash());
        JSONObject missingRequired = JSON.parseObject(json);
        missingRequired.getJSONObject("componentContracts").getJSONObject("business.reward")
                .getJSONObject("inputSlots").getJSONObject("order").remove("required");
        assertThrows(IllegalArgumentException.class,
                () -> ContractJson.fromBundleJson(missingRequired.toJSONString()));
    }

    @Test
    void round_trips_every_current_and_historical_node_type_with_standard_fields() throws IOException {
        ExecutableFlowBundle bundle = ContractJson.fromBundleJson(resource("contracts/full-node-bundle.json"));

        assertEquals("1.0", bundle.contractVersion());
        assertEquals("2.0", bundle.flowDefinition().schemaVersion());
        assertEquals(15, bundle.flowDefinition().nodes().size());
        assertEquals("start", bundle.flowDefinition().nodes().getFirst().nodeCode());
        assertEquals("builtin.start", bundle.flowDefinition().nodes().getFirst().componentCode());
        assertEquals("NEXT", bundle.flowDefinition().edges().getFirst().sourcePort());
        assertEquals("IN", bundle.flowDefinition().edges().getFirst().targetPort());
        assertEquals(ComponentType.MERGE, bundle.componentContracts().get("legacy.merge").componentType());
        assertEquals(ComponentType.SWITCH, bundle.componentContracts().get("legacy.switch").componentType());
        assertRole(bundle, "loopStart", ComponentType.LOOP, "START");
        assertRole(bundle, "loopEnd", ComponentType.LOOP, "END");
        assertRole(bundle, "lockAcquire", ComponentType.LOCK, "ACQUIRE");
        assertRole(bundle, "lockRelease", ComponentType.LOCK, "RELEASE");
        assertDoesNotThrow(() -> ContractJson.fromBundleJson(ContractJson.toBundleJson(bundle)));
    }

    @Test
    void defensively_copies_nested_json_values() {
        List<Object> originalList = new ArrayList<>(List.of("one"));
        Map<String, Object> nested = new LinkedHashMap<>(Map.of("items", originalList));
        Map<String, Object> config = new LinkedHashMap<>(Map.of("nested", nested));

        FlowNode node = new FlowNode("n1", "builtin.script", "script", "脚本", config);
        originalList.add("two");
        nested.put("newField", "mutated");
        config.put("changed", true);

        @SuppressWarnings("unchecked")
        Map<String, Object> copiedNested = (Map<String, Object>) node.config().get("nested");
        @SuppressWarnings("unchecked")
        List<Object> copiedList = (List<Object>) copiedNested.get("items");
        assertEquals(List.of("one"), copiedList);
        assertFalse(copiedNested.containsKey("newField"));
        assertThrows(UnsupportedOperationException.class, () -> copiedList.add("blocked"));
    }

    @Test
    void calculates_a_deterministic_hash_from_canonical_json_without_hash_or_signature() {
        ExecutableFlowBundle first = bundleWithConfig(Map.of("z", List.of(2, 1), "a", Map.of("b", true)));
        ExecutableFlowBundle second = bundleWithConfig(orderedConfig());
        ExecutableFlowBundle changed = bundleWithConfig(Map.of("z", List.of(1, 2), "a", Map.of("b", true)));

        assertEquals(first.calculateBundleHash(), second.calculateBundleHash());
        assertNotEquals(first.calculateBundleHash(), changed.calculateBundleHash());
        ExecutableFlowBundle excludedFieldsChanged = new ExecutableFlowBundle(
                first.contractVersion(), first.flowIdentity(), first.flowDefinition(), first.componentContracts(),
                first.protocolSchemas(), first.externalScopeSchemas(), "f".repeat(64),
                new BundleTrust("issuer", "key", "different-signature"));
        assertEquals(first.calculateBundleHash(), excludedFieldsChanged.calculateBundleHash());
        assertEquals(first.calculateBundleHash(), first.withCalculatedHash().bundleHash());
    }

    @Test
    void rejects_unknown_major_contract_and_flow_definition_versions() {
        assertThrows(IllegalArgumentException.class, () -> ContractJson.fromBundleJson("""
                {"contractVersion":"2.0","flowIdentity":{"tenantId":"tenant","flowId":"flow","version":"v1"},
                 "flowDefinition":{"schemaVersion":"2.0","nodes":[],"edges":[]},
                 "componentContracts":{},"protocolSchemas":{},"externalScopeSchemas":{},"trust":{}}
                """));
        assertThrows(IllegalArgumentException.class, () -> new FlowDefinition("3.0", List.of(), List.of()));
    }

    @Test
    void rejects_unknown_structural_fields_but_keeps_config_extensible() {
        String json = """
                {"contractVersion":"1.0","flowIdentity":{"tenantId":"tenant","flowId":"flow","version":"v1"},
                 "flowDefinition":{"schemaVersion":"2.0","nodes":[{"id":"n1","componentCode":"builtin.script","nodeCode":"script","nodeName":"脚本","config":{"futureConfig":true},"componentSnapshot":{}}],"edges":[]},
                 "componentContracts":{},"protocolSchemas":{},"externalScopeSchemas":{},"trust":{}}
                """;

        assertThrows(IllegalArgumentException.class, () -> ContractJson.fromBundleJson(json));
    }

    @Test
    void request_to_string_does_not_expose_input_or_scope_values() {
        ExecutionRequest request = new ExecutionRequest(
                "inline:flow", ExecutionMode.EPHEMERAL, ExecutionPurpose.TRIAL,
                Map.of("password", "top-secret"), Map.of("customer", Map.of("token", "scope-secret")),
                "vault:context", "idempotency", "tenant", Instant.parse("2026-09-04T00:00:00Z"),
                Map.of("trace", "abc"));

        String rendered = request.toString();
        assertFalse(rendered.contains("top-secret"));
        assertFalse(rendered.contains("scope-secret"));
        assertFalse(rendered.contains("vault:context"));
    }

    @Test
    void rejects_non_finite_float_and_double_values_before_json_serialization_or_hashing() {
        assertNonFiniteNumberRejected(Double.NaN, "Double");
        assertNonFiniteNumberRejected(Double.POSITIVE_INFINITY, "Double");
        assertNonFiniteNumberRejected(Double.NEGATIVE_INFINITY, "Double");
        assertNonFiniteNumberRejected(Float.NaN, "Float");
        assertNonFiniteNumberRejected(Float.POSITIVE_INFINITY, "Float");
        assertNonFiniteNumberRejected(Float.NEGATIVE_INFINITY, "Float");

        assertThrows(IllegalArgumentException.class,
                () -> bundleWithConfig(Map.of("limit", Double.POSITIVE_INFINITY)).calculateBundleHash());
    }

    @Test
    void schema_non_blank_constraints_match_required_java_text_fields() throws IOException {
        JSONObject schema = JSON.parseObject(resource("schema/executable-flow-bundle-1.0.schema.json"));
        JSONObject definitions = schema.getJSONObject("$defs");
        assertEquals(1, definitions.getJSONObject("nonBlankString").getIntValue("minLength"));
        assertEquals("\\S", definitions.getJSONObject("nonBlankString").getString("pattern"));

        assertNonBlankReference(definitions.getJSONObject("flowIdentity"),
                "tenantId", "flowId", "version");
        assertNonBlankReference(definitions.getJSONObject("flowNode"), "id", "componentCode", "nodeCode", "nodeName");
        assertNonBlankReference(definitions.getJSONObject("flowEdge"), "id", "sourceNodeId", "targetNodeId",
                "sourcePort", "targetPort");
        assertNonBlankReference(definitions.getJSONObject("componentContract"), "componentCode", "executorRef");
        assertNonBlankReference(definitions.getJSONObject("protocolSchema"), "code", "version");
        assertNonBlankReference(definitions.getJSONObject("inputSlot"), "name");
        assertNonBlankReference(definitions.getJSONObject("outputSlot"), "name");
        assertTrue(definitions.getJSONObject("inputSlot").getJSONArray("required").contains("required"));
        assertFalse(definitions.getJSONObject("outputSlot").getJSONArray("required").contains("required"));
    }

    private static ExecutableFlowBundle bundleWithConfig(Map<String, Object> config) {
        return new ExecutableFlowBundle(
                "1.0", new FlowIdentity("tenant", "flow", "v1"),
                new FlowDefinition("2.0", List.of(new FlowNode("n1", "builtin.script", "script", "脚本", config)), List.of()),
                Map.of(), Map.of(), Map.of(), "0".repeat(64), new BundleTrust("issuer", "key", "signature"));
    }

    private static Map<String, Object> orderedConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("z", List.of(2, 1));
        config.put("a", Map.of("b", true));
        return config;
    }

    private static void assertRole(ExecutableFlowBundle bundle, String nodeCode, ComponentType type, String role) {
        FlowNode node = bundle.flowDefinition().nodes().stream()
                .filter(candidate -> nodeCode.equals(candidate.nodeCode()))
                .findFirst()
                .orElseThrow();
        assertEquals(type, bundle.componentContracts().get(node.componentCode()).componentType());
        assertEquals(role, node.config().get("role"));
    }

    private static void assertNonBlankReference(JSONObject object, String... propertyNames) {
        JSONObject properties = object.getJSONObject("properties");
        for (String propertyName : propertyNames) {
            assertEquals("#/$defs/nonBlankString", properties.getJSONObject(propertyName).getString("$ref"));
        }
    }

    private static void assertNonFiniteNumberRejected(Number value, String valueType) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new FlowNode("n1", "builtin.script", "script", "脚本", Map.of("value", value)));
        assertTrue(failure.getMessage().contains("node config.value"));
        assertTrue(failure.getMessage().contains(valueType));
    }

    private static String resource(String name) throws IOException {
        try (var stream = ContractJsonTest.class.getClassLoader().getResourceAsStream(name)) {
            if (stream == null) {
                throw new IOException("Missing test resource: " + name);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
