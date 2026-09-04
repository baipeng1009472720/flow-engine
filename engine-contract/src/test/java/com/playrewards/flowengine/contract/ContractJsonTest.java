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
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.json.ContractJson;
import com.playrewards.flowengine.contract.json.JsonValues;
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
    void execution_result_carries_optional_error_message_without_rendering_it() {
        Instant startedAt = Instant.parse("2026-09-04T00:00:00Z");
        ExecutionResult result = new ExecutionResult("execution", ExecutionStatus.FAILED, null,
                "DECLINED", "sensitive business reason", "error-id", startedAt, startedAt.plusSeconds(1));
        ExecutionResult decoded = JSON.parseObject(JSON.toJSONString(result), ExecutionResult.class);

        assertEquals("sensitive business reason", result.errorMessage());
        assertEquals(result, decoded);
        assertFalse(result.toString().contains("sensitive business reason"));
        assertTrue(result.toString().contains("errorMessagePresent=true"));
        assertThrows(IllegalArgumentException.class,
                () -> new ExecutionResult("execution", ExecutionStatus.FAILED, null,
                        "DECLINED", "  ", "error-id", startedAt, startedAt.plusSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new ExecutionResult("execution", ExecutionStatus.SUCCEEDED, "result",
                        "ERROR", null, "error-id", startedAt, startedAt.plusSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                () -> new ExecutionResult("execution", ExecutionStatus.FAILED, "forbidden-result",
                        "DECLINED", null, "error-id", startedAt, startedAt.plusSeconds(1)));
    }

    @Test
    void execution_result_accepts_only_terminal_statuses_and_requires_safe_failure_shapes() {
        Instant now = Instant.parse("2026-09-04T00:00:00Z");
        for (ExecutionStatus status : ExecutionStatus.values()) {
            if (status != ExecutionStatus.SUCCEEDED
                    && status != ExecutionStatus.FAILED
                    && status != ExecutionStatus.CANCELLED
                    && status != ExecutionStatus.WAITING_MANUAL) {
                assertThrows(IllegalArgumentException.class,
                        () -> new ExecutionResult("execution", status, null,
                                "NOT_TERMINAL", null, null, now, now));
            }
        }

        assertThrows(IllegalArgumentException.class,
                () -> new ExecutionResult("execution", ExecutionStatus.CANCELLED,
                        Map.of("secret", "value"), "EXECUTION_CANCELLED", null, null, now, now));
        assertThrows(IllegalArgumentException.class,
                () -> new ExecutionResult("execution", ExecutionStatus.CANCELLED,
                        null, null, "cancelled", null, now, now));
        assertThrows(RuntimeException.class, () -> JSON.parseObject("""
                {"executionId":"execution","status":"RUNNING","errorCode":"NOT_TERMINAL",
                 "startedAt":"2026-09-04T00:00:00Z","finishedAt":"2026-09-04T00:00:00Z"}
                """, ExecutionResult.class));
        assertThrows(RuntimeException.class, () -> JSON.parseObject("""
                {"executionId":"execution","status":"CANCELLED","result":{"secret":"value"},
                 "errorCode":"EXECUTION_CANCELLED","startedAt":"2026-09-04T00:00:00Z",
                 "finishedAt":"2026-09-04T00:00:00Z"}
                """, ExecutionResult.class));

        ExecutionResult cancelled = new ExecutionResult("execution", ExecutionStatus.CANCELLED,
                null, "EXECUTION_CANCELLED", "cancelled", null, now, now);
        assertEquals(cancelled, JSON.parseObject(JSON.toJSONString(cancelled), ExecutionResult.class));
        ExecutionResult waitingManual = new ExecutionResult("execution", ExecutionStatus.WAITING_MANUAL,
                null, "HTTP_SIDE_EFFECT_UNCERTAIN", "manual confirmation required", null, now, now);
        assertEquals(waitingManual,
                JSON.parseObject(JSON.toJSONString(waitingManual), ExecutionResult.class));
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
    void shared_resource_walker_matches_fastjson_for_escaped_strings_keys_and_unicode() {
        List<Object> roots = List.of(
                "\n", "\"", "\\", "😀", "\ud800",
                Map.of("key\n\"\\😀\ud800",
                        java.util.Arrays.asList("value\n\"\\😀\ud800", true, null)));
        long expectedBytes = roots.stream().mapToLong(value -> JSON.toJSONBytes(value).length).sum();
        JsonValues.JsonResourceLimits limits = new JsonValues.JsonResourceLimits(
                expectedBytes, 1_024, 16, 100);

        JsonValues.JsonResourceUsage usage = JsonValues.measureJsonResources(roots, limits);

        assertEquals(expectedBytes, usage.encodedBytes());
        assertEquals(10, usage.nodes());
        for (String value : List.of("\n", "\"", "\\", "😀", "\ud800")) {
            assertEquals(JSON.toJSONBytes(value).length, JsonValues.jsonEncodedStringBytes(value));
        }
        assertEquals(JSON.toJSONBytes("😀").length - 2,
                JsonValues.jsonStringContentBytes(List.of("\ud83d", "\ude00")));
        assertEquals(1, JsonValues.measureJsonString(List.of("\ud83d", "\ude00")).codePoints());
        assertEquals(JSON.toJSONBytes("\ude00\ud83d\ude00\ud83d").length - 2,
                JsonValues.jsonRepeatedStringContentBytes("\ude00\ud83d", 2));
    }

    @Test
    void shared_resource_walker_accepts_exact_boundaries_and_rejects_one_byte_beyond_them() {
        Object value = Map.of("escaped", "\n".repeat(10), "emoji", "😀");
        long exactBytes = JSON.toJSONBytes(value).length;
        long exactStringBytes = JSON.toJSONBytes("\n".repeat(10)).length;

        assertEquals(exactBytes, JsonValues.measureJsonResources(List.of(value),
                new JsonValues.JsonResourceLimits(exactBytes, exactStringBytes, 4, 3)).encodedBytes());
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(value),
                        new JsonValues.JsonResourceLimits(exactBytes - 1, exactStringBytes, 4, 3)));
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(value),
                        new JsonValues.JsonResourceLimits(exactBytes, exactStringBytes - 1, 4, 3)));
    }

    @Test
    void shared_resource_walker_matches_fastjson_big_decimal_encoding_without_plain_text_allocation() {
        for (java.math.BigDecimal value : List.of(
                new java.math.BigDecimal("0.0000001"),
                new java.math.BigDecimal("-0.00120"),
                new java.math.BigDecimal("1E+7"),
                new java.math.BigDecimal("1234567890123456789E-30"))) {
            assertEquals(JSON.toJSONBytes(value).length, JsonValues.jsonEncodedNumberBytes(value));
        }

        java.math.BigDecimal hugePlain = new java.math.BigDecimal(java.math.BigInteger.ONE, 1_000_000);
        assertEquals(1_000_002, JsonValues.jsonEncodedNumberBytes(hugePlain));
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(hugePlain),
                        new JsonValues.JsonResourceLimits(1_000_001, 8, 1, 1)));
    }

    @Test
    void admission_resource_walker_counts_every_null_map_key_separator_and_value() {
        Map<String, Object> nullable = new LinkedHashMap<>();
        nullable.put("a", null);
        nullable.put("bbbb", null);
        long admittedBytes = 22;

        JsonValues.JsonResourceUsage usage = JsonValues.measureJsonResources(List.of(nullable),
                new JsonValues.JsonResourceLimits(admittedBytes, 8, 2, 3));

        assertEquals(admittedBytes, usage.encodedBytes());
        assertEquals(3, usage.nodes());
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(nullable),
                        new JsonValues.JsonResourceLimits(admittedBytes - 1, 8, 2, 3)));

        Map<String, Object> longNullKey = new LinkedHashMap<>();
        longNullKey.put("1234567", null);
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(longNullKey),
                        new JsonValues.JsonResourceLimits(64, 8, 2, 2)));
    }

    @Test
    void rejects_uncached_inflated_numbers_before_large_precision_or_text_workspace_allocation() {
        BigInteger inflatedInteger = BigInteger.ONE.shiftLeft(2_000_000).subtract(BigInteger.ONE);
        BigDecimal uncachedPrecision = new BigDecimal(inflatedInteger, 0);
        com.sun.management.ThreadMXBean memory =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertTrue(memory.isThreadAllocatedMemorySupported());
        memory.setThreadAllocatedMemoryEnabled(true);
        JsonValues.measureJsonResources(List.of(BigInteger.ONE),
                new JsonValues.JsonResourceLimits(64, 8, 1, 1));

        long before = memory.getThreadAllocatedBytes(Thread.currentThread().threadId());
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(uncachedPrecision),
                        new JsonValues.JsonResourceLimits(64, 8, 1, 1)));
        long allocated = memory.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

        assertTrue(allocated < 131_072,
                () -> "number preflight allocated " + allocated + " bytes before rejection");
    }

    @Test
    void big_integer_admission_preserves_a_proven_near_boundary() {
        BigInteger oneThousandDigits = BigInteger.TEN.pow(999);
        BigInteger uncertainOneByteBoundary = BigInteger.valueOf(512);

        assertEquals(1_000, JsonValues.measureJsonResources(List.of(oneThousandDigits),
                new JsonValues.JsonResourceLimits(1_000, 8, 1, 1)).encodedBytes());
        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.measureJsonResources(List.of(oneThousandDigits),
                        new JsonValues.JsonResourceLimits(999, 8, 1, 1)));
        assertEquals(3, JsonValues.measureJsonResources(List.of(uncertainOneByteBoundary),
                new JsonValues.JsonResourceLimits(3, 8, 1, 1)).encodedBytes());
    }

    @Test
    void exact_number_sizing_absolute_workspace_limit_cannot_be_overridden_by_a_caller() {
        BigDecimal excessiveWorkspace = new BigDecimal(BigInteger.ONE, 5_000_000);

        assertThrows(JsonValues.JsonResourceLimitException.class,
                () -> JsonValues.jsonEncodedNumberBytes(excessiveWorkspace, Long.MAX_VALUE));
    }

    @Test
    void number_text_preflight_is_allocation_free_for_big_integer_and_rejects_host_subclasses() {
        BigInteger large = BigInteger.TEN.pow(100_000);
        assertTrue(JsonValues.javaNumberTextUpperBoundBytes(large) >= large.toString().length());

        for (Number hostNumber : List.of(new HostBigInteger("1"), new HostBigDecimal("1"))) {
            assertThrows(IllegalArgumentException.class,
                    () -> JsonValues.copyValue(hostNumber, "value"));
            assertThrows(IllegalArgumentException.class,
                    () -> JsonValues.jsonEncodedNumberBytes(hostNumber));
            assertThrows(IllegalArgumentException.class,
                    () -> JsonValues.javaNumberTextUpperBoundBytes(hostNumber));
        }
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

    private static final class HostBigInteger extends BigInteger {

        private HostBigInteger(String value) {
            super(value);
        }
    }

    private static final class HostBigDecimal extends BigDecimal {

        private HostBigDecimal(String value) {
            super(value);
        }
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
