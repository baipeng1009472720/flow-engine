package com.playrewards.flowengine.core.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.bundle.CompiledComponentContract;
import com.playrewards.flowengine.contract.bundle.ExecutableFlowBundle;
import com.playrewards.flowengine.contract.bundle.FlowIdentity;
import com.playrewards.flowengine.contract.bundle.InputSlotContract;
import com.playrewards.flowengine.contract.bundle.OutputSlotContract;
import com.playrewards.flowengine.contract.component.ComponentCapabilities;
import com.playrewards.flowengine.contract.component.ComponentEffect;
import com.playrewards.flowengine.contract.component.ComponentType;
import com.playrewards.flowengine.contract.execution.ContentChunk;
import com.playrewards.flowengine.contract.execution.ExecutionMode;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import com.playrewards.flowengine.contract.execution.ExecutionResult;
import com.playrewards.flowengine.contract.execution.ExecutionStatus;
import com.playrewards.flowengine.contract.flow.FlowDefinition;
import com.playrewards.flowengine.contract.flow.FlowEdge;
import com.playrewards.flowengine.contract.flow.FlowNode;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.core.compile.CompiledNode;
import com.playrewards.flowengine.core.compile.CompiledNode.InputBinding;
import com.playrewards.flowengine.core.compile.ExecutionPlan;
import com.playrewards.flowengine.core.compile.FlowCompiler;
import com.playrewards.flowengine.core.component.FlowBusinessComponentRegistry;
import com.playrewards.flowengine.core.execution.CancellationHandle;
import com.playrewards.flowengine.core.execution.ConcurrencyGovernor;
import com.playrewards.flowengine.core.execution.ExecutionContext;
import com.playrewards.flowengine.core.execution.FlowEngine;
import com.playrewards.flowengine.core.execution.ScopePath;
import com.playrewards.flowengine.core.node.builtin.BusinessNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.IfNodeExecutor;
import com.playrewards.flowengine.core.node.builtin.VariableNodeExecutor;
import com.playrewards.flowengine.core.protocol.InputBindingResolver;
import com.playrewards.flowengine.core.protocol.OutputValidator;
import com.playrewards.flowengine.spi.ComponentExecutionContext;
import com.playrewards.flowengine.spi.ComponentInvocation;
import com.playrewards.flowengine.spi.ComponentOutputs;
import com.playrewards.flowengine.spi.ExecutionLogEntry;
import com.playrewards.flowengine.spi.ExecutionLogger;
import com.playrewards.flowengine.spi.FlowBusinessComponent;
import com.playrewards.flowengine.spi.ScriptRuntime;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Verifies strict BUSINESS/SCRIPT boundaries and their real scheduler assembly. */
class BusinessNodeExecutorTest {

    private static final ProtocolSchema TEXT = protocol("text", Map.of("type", "string"));
    private static final ProtocolSchema NUMBER = protocol("number", Map.of("type", "integer"));
    private static final ComponentCapabilities PURE =
            new ComponentCapabilities(ComponentEffect.PURE, true, false, true, 8);

    @Test
    void resolves_required_and_optional_inputs_without_fabricating_missing_nulls() {
        AtomicReference<ComponentExecutionContext> captured = new AtomicReference<>();
        FlowBusinessComponent component = component(PURE, context -> {
            captured.set(context);
            return completedOutputs(Map.of());
        });
        BusinessNodeExecutor executor = businessExecutor(component);
        NodeExecutionContext missingRequired = businessContext(
                Map.of("required", input(true, TEXT), "optional", input(false, TEXT)),
                Map.of("optional", binding("source", "optional")),
                Map.of("optional", "available"), TEXT, PURE, ExecutionPurpose.PRODUCTION,
                Map.of("businessOption", "kept", "inputBindings", List.of(Map.of("private", true))));

        NodeExecutionException failure = assertThrows(NodeExecutionException.class,
                () -> executor.execute(missingRequired));
        assertEquals("NODE_REQUIRED_INPUT_MISSING", failure.code());
        assertEquals(null, captured.get());

        NodeExecutionContext optionalMissing = businessContext(
                Map.of("optional", input(false, TEXT)), Map.of(), Map.of(), TEXT, PURE,
                ExecutionPurpose.PRODUCTION,
                Map.of("businessOption", "kept", "inputBindings", List.of(Map.of("private", true))));
        join(executor.execute(optionalMissing));

        assertEquals(Map.of(), captured.get().inputs().optionalValues());
        assertEquals(Map.of("businessOption", "kept"), captured.get().config().values());
    }

    @Test
    void rejects_protocol_code_schema_and_runtime_value_mismatches_before_invocation() {
        FlowBusinessComponent component = component(PURE, ignored -> {
            throw new AssertionError("component must not be invoked");
        });
        BusinessNodeExecutor executor = businessExecutor(component);

        NodeExecutionException codeFailure = assertThrows(NodeExecutionException.class,
                () -> executor.execute(businessContext(
                        Map.of("value", input(true, protocol("target", Map.of("type", "string")))),
                        Map.of("value", binding("source", "value")), Map.of("value", "ok"),
                        protocol("source", Map.of("type", "string")), PURE,
                        ExecutionPurpose.PRODUCTION, Map.of())));
        assertEquals("NODE_INPUT_PROTOCOL_MISMATCH", codeFailure.code());

        NodeExecutionException schemaFailure = assertThrows(NodeExecutionException.class,
                () -> executor.execute(businessContext(
                        Map.of("value", input(true, protocol("shared", Map.of("type", "integer")))),
                        Map.of("value", binding("source", "value")), Map.of("value", "wrong"),
                        protocol("shared", Map.of("type", "string")), PURE,
                        ExecutionPurpose.PRODUCTION, Map.of())));
        assertEquals("NODE_INPUT_SCHEMA_MISMATCH", schemaFailure.code());

        NodeExecutionException valueFailure = assertThrows(NodeExecutionException.class,
                () -> executor.execute(businessContext(
                        Map.of("value", input(true, TEXT)),
                        Map.of("value", binding("source", "value")), Map.of("value", 42),
                        TEXT, PURE, ExecutionPurpose.PRODUCTION, Map.of())));
        assertEquals("NODE_INPUT_INVALID", valueFailure.code());
    }

    @Test
    void supplies_immutable_scopes_logger_cancellation_and_purpose_to_component() {
        AtomicReference<ComponentExecutionContext> captured = new AtomicReference<>();
        FlowBusinessComponent component = component(PURE, context -> {
            captured.set(context);
            return completedOutputs(Map.of());
        });
        CollectingLogger logger = new CollectingLogger();
        NodeExecutionContext context = businessContext(
                Map.of(), Map.of(), Map.of(), TEXT, PURE, ExecutionPurpose.TRIAL,
                Map.of("businessOption", "kept"), Map.of("play", Map.of("name", "demo")), logger);

        join(new BusinessNodeExecutor(new FlowBusinessComponentRegistry(List.of(component)),
                new InputBindingResolver(), new OutputValidator()).execute(context));

        assertEquals(Map.of("play", Map.of("name", "demo")), captured.get().context().scopes());
        assertSame(logger, captured.get().logger());
        assertSame(context.cancellationToken(), captured.get().cancellationToken());
        assertEquals(ExecutionPurpose.TRIAL, captured.get().purpose());
        assertThrows(UnsupportedOperationException.class,
                () -> captured.get().context().requiredScope("play").put("name", "changed"));
    }

    @Test
    void rejects_missing_duplicate_or_capability_inconsistent_components_and_unsafe_trials() {
        assertThrows(IllegalArgumentException.class, () -> new FlowBusinessComponentRegistry(
                List.of(component(PURE, ignored -> completedOutputs(Map.of())),
                        component(PURE, ignored -> completedOutputs(Map.of())))));

        NodeExecutionContext context = businessContext(Map.of(), Map.of(), Map.of(), TEXT, PURE,
                ExecutionPurpose.PRODUCTION, Map.of());
        NodeExecutionException missing = assertThrows(NodeExecutionException.class,
                () -> new BusinessNodeExecutor(new FlowBusinessComponentRegistry(List.of()),
                        new InputBindingResolver(), new OutputValidator()).execute(context));
        assertEquals("BUSINESS_COMPONENT_NOT_FOUND", missing.code());

        ComponentCapabilities inconsistent =
                new ComponentCapabilities(ComponentEffect.IDEMPOTENT, true, false, true, 8);
        NodeExecutionException mismatch = assertThrows(NodeExecutionException.class,
                () -> businessExecutor(component(inconsistent, ignored -> completedOutputs(Map.of())))
                        .execute(context));
        assertEquals("BUSINESS_CAPABILITIES_MISMATCH", mismatch.code());

        ComponentCapabilities noTrial =
                new ComponentCapabilities(ComponentEffect.PURE, false, false, true, 8);
        NodeExecutionContext trial = businessContext(Map.of(), Map.of(), Map.of(), TEXT, noTrial,
                ExecutionPurpose.TRIAL, Map.of());
        NodeExecutionException unsupportedTrial = assertThrows(NodeExecutionException.class,
                () -> businessExecutor(component(noTrial, ignored -> completedOutputs(Map.of())))
                        .execute(trial));
        assertEquals("BUSINESS_TRIAL_UNSUPPORTED", unsupportedTrial.code());
    }

    @Test
    void validates_only_returned_declared_outputs_and_allows_declared_outputs_to_be_absent() {
        OutputSlotContract output = new OutputSlotContract("result", TEXT);

        NodeResult missing = join(businessExecutor(component(PURE,
                ignored -> completedOutputs(Map.of()))).execute(businessContext(
                        Map.of(), Map.of(), Map.of(), TEXT, PURE, ExecutionPurpose.PRODUCTION,
                        Map.of(), Map.of(), new CollectingLogger(), Map.of("result", output))));
        assertEquals(Map.of(), missing.outputs());

        CompletionException unknownFailure = assertThrows(CompletionException.class,
                () -> join(businessExecutor(component(PURE,
                        ignored -> completedOutputs(Map.of("other", "value")))).execute(businessContext(
                                Map.of(), Map.of(), Map.of(), TEXT, PURE, ExecutionPurpose.PRODUCTION,
                                Map.of(), Map.of(), new CollectingLogger(), Map.of("result", output)))));
        assertNodeFailure("NODE_OUTPUT_UNKNOWN", unknownFailure);

        CompletionException invalidFailure = assertThrows(CompletionException.class,
                () -> join(businessExecutor(component(PURE,
                        ignored -> completedOutputs(Map.of("result", 42)))).execute(businessContext(
                                Map.of(), Map.of(), Map.of(), TEXT, PURE, ExecutionPurpose.PRODUCTION,
                                Map.of(), Map.of(), new CollectingLogger(), Map.of("result", output)))));
        assertNodeFailure("NODE_OUTPUT_INVALID", invalidFailure);
    }

    @Test
    void preserves_streaming_publisher_while_validating_final_outputs() {
        Flow.Publisher<ContentChunk> publisher = subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long count) {
                subscriber.onComplete();
            }

            @Override
            public void cancel() {
            }
        });
        FlowBusinessComponent component = component(PURE, ignored -> new ComponentInvocation(
                CompletableFuture.completedFuture(new ComponentOutputs(Map.of("result", "done"))), publisher));

        NodeInvocation invocation = businessExecutor(component).execute(businessContext(
                Map.of(), Map.of(), Map.of(), TEXT, PURE, ExecutionPurpose.PRODUCTION,
                Map.of(), Map.of(), new CollectingLogger(),
                Map.of("result", new OutputSlotContract("result", TEXT))));

        assertSame(publisher, invocation.contentStream());
        assertEquals(Map.of("result", "done"), join(invocation).outputs());
    }

    @Test
    void executes_if_and_variable_groovy_modes_with_strict_result_types() {
        ScriptRuntime trueRuntime = invocation -> CompletableFuture.completedFuture(true);
        NodeResult selected = join(new IfNodeExecutor(trueRuntime).execute(nodeContext(
                ComponentType.IF, Map.of("conditionMode", "GROOVY", "groovyScript", "return true"),
                Map.of(), Map.of(), PURE, ExecutionPurpose.PRODUCTION, Map.of(), Map.of(),
                new CollectingLogger())));
        assertEquals(Set.of("TRUE"), selected.activatedPorts());

        ScriptRuntime invalidRuntime = invocation -> CompletableFuture.completedFuture("yes");
        CompletionException invalid = assertThrows(CompletionException.class,
                () -> join(new IfNodeExecutor(invalidRuntime).execute(nodeContext(
                        ComponentType.IF,
                        Map.of("conditionMode", "GROOVY", "groovyScript", "return 'yes'"),
                        Map.of(), Map.of(), PURE, ExecutionPurpose.PRODUCTION, Map.of(), Map.of(),
                        new CollectingLogger()))));
        assertNodeFailure("IF_CONDITION_NOT_BOOLEAN", invalid);

        ScriptRuntime numberRuntime = invocation -> CompletableFuture.completedFuture(7);
        NodeResult variable = join(new VariableNodeExecutor(numberRuntime, new OutputValidator()).execute(nodeContext(
                ComponentType.VARIABLE,
                Map.of("valueMode", "GROOVY", "groovyScript", "return 7"), Map.of(),
                Map.of("value", new OutputSlotContract("value", NUMBER)), PURE,
                ExecutionPurpose.PRODUCTION, Map.of(), Map.of(), new CollectingLogger())));
        assertEquals(Map.of("value", 7), variable.outputs());
    }

    @Test
    void executes_script_business_end_through_flow_engine_with_full_runtime_assembly() throws Exception {
        CollectingLogger logger = new CollectingLogger();
        ScriptRuntime scripts = invocation -> CompletableFuture.completedFuture(
                String.valueOf(invocation.externalScopes().get("play").get("prefix"))
                        + ((Map<?, ?>) invocation.ctx().get("start")).get("message"));
        FlowBusinessComponent business = component(PURE, context -> completedOutputs(Map.of(
                "result", context.inputs().requiredJson("input") + "!")));
        ExecutionPlan plan = integrationPlan();
        NodeExecutorRegistry registry = NodeExecutorRegistry.runtimeBuiltIns(
                scripts, new FlowBusinessComponentRegistry(List.of(business)));
        ExecutionRequest request = request(ExecutionPurpose.TRIAL,
                Map.of("message", "hello"), Map.of("play", Map.of("prefix", "say:")));

        try (FlowEngine engine = new FlowEngine(
                registry, ConcurrencyGovernor.defaults(), Clock.systemUTC(), logger)) {
            ExecutionResult result = engine.execute(plan, request).completion()
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertEquals(ExecutionStatus.SUCCEEDED, result.status());
            assertEquals("say:hello!", result.result());
        }
    }

    private static BusinessNodeExecutor businessExecutor(FlowBusinessComponent component) {
        return new BusinessNodeExecutor(new FlowBusinessComponentRegistry(List.of(component)),
                new InputBindingResolver(), new OutputValidator());
    }

    private static FlowBusinessComponent component(ComponentCapabilities capabilities,
            java.util.function.Function<ComponentExecutionContext, ComponentInvocation> invocation) {
        return new FlowBusinessComponent() {
            @Override
            public String executorRef() {
                return "business.executor";
            }

            @Override
            public ComponentCapabilities capabilities() {
                return capabilities;
            }

            @Override
            public ComponentInvocation execute(ComponentExecutionContext context) {
                return invocation.apply(context);
            }
        };
    }

    private static ComponentInvocation completedOutputs(Map<String, Object> outputs) {
        return new ComponentInvocation(CompletableFuture.completedFuture(new ComponentOutputs(outputs)),
                ComponentInvocation.emptyContent());
    }

    private static NodeExecutionContext businessContext(
            Map<String, InputSlotContract> inputs,
            Map<String, InputBinding> bindings,
            Map<String, Object> sourceValues,
            ProtocolSchema sourceProtocol,
            ComponentCapabilities capabilities,
            ExecutionPurpose purpose,
            Map<String, Object> config) {
        return businessContext(inputs, bindings, sourceValues, sourceProtocol, capabilities, purpose,
                config, Map.of(), new CollectingLogger());
    }

    private static NodeExecutionContext businessContext(
            Map<String, InputSlotContract> inputs,
            Map<String, InputBinding> bindings,
            Map<String, Object> sourceValues,
            ProtocolSchema sourceProtocol,
            ComponentCapabilities capabilities,
            ExecutionPurpose purpose,
            Map<String, Object> config,
            Map<String, Map<String, Object>> externalScopes,
            ExecutionLogger logger) {
        Map<String, OutputSlotContract> outputs = Map.of();
        return businessContext(inputs, bindings, sourceValues, sourceProtocol, capabilities,
                purpose, config, externalScopes, logger, outputs);
    }

    private static NodeExecutionContext businessContext(
            Map<String, InputSlotContract> inputs,
            Map<String, InputBinding> bindings,
            Map<String, Object> sourceValues,
            ProtocolSchema sourceProtocol,
            ComponentCapabilities capabilities,
            ExecutionPurpose purpose,
            Map<String, Object> config,
            Map<String, Map<String, Object>> externalScopes,
            ExecutionLogger logger,
            Map<String, OutputSlotContract> outputs) {
        Map<String, OutputSlotContract> sourceOutputs = new LinkedHashMap<>();
        sourceValues.keySet().forEach(key -> sourceOutputs.put(
                key, new OutputSlotContract(key, sourceProtocol)));
        CompiledNode source = compiledNode("source", ComponentType.START, null, Map.of(), sourceOutputs,
                Map.of(), Map.of(), PURE);
        CompiledNode target = compiledNode("business", ComponentType.BUSINESS, "business.executor", inputs,
                outputs, bindings, config, capabilities);
        ExecutionPlan plan = plan(Map.of("source", source, "business", target));
        ExecutionContext executionContext = new ExecutionContext(externalScopes);
        sourceValues.forEach((key, value) -> executionContext.writeOnce(
                "source", ScopePath.root(), key, value));
        return new NodeExecutionContext("execution", plan, target, executionContext, ScopePath.root(),
                new com.playrewards.flowengine.spi.ComponentInputs(Map.of(), Map.of()),
                new CancellationHandle(), request(purpose, Map.of(), externalScopes), Clock.systemUTC(), logger);
    }

    private static NodeExecutionContext nodeContext(ComponentType type, Map<String, Object> config,
            Map<String, InputSlotContract> inputs, Map<String, OutputSlotContract> outputs,
            ComponentCapabilities capabilities, ExecutionPurpose purpose,
            Map<String, Object> startInputs, Map<String, Map<String, Object>> scopes,
            ExecutionLogger logger) {
        CompiledNode node = compiledNode("target", type, null, inputs, outputs, Map.of(), config, capabilities);
        return new NodeExecutionContext("execution", plan(Map.of("target", node)), node,
                new ExecutionContext(scopes), ScopePath.root(),
                new com.playrewards.flowengine.spi.ComponentInputs(Map.of(), Map.of()),
                new CancellationHandle(), request(purpose, startInputs, scopes), Clock.systemUTC(), logger);
    }

    private static CompiledNode compiledNode(String id, ComponentType type, String executorRef,
            Map<String, InputSlotContract> inputs, Map<String, OutputSlotContract> outputs,
            Map<String, InputBinding> bindings, Map<String, Object> config,
            ComponentCapabilities capabilities) {
        FlowNode node = new FlowNode(id, "component." + id, id, id, config);
        return new CompiledNode(node, new CompiledComponentContract(
                node.componentCode(), type, executorRef, Map.of(), inputs, outputs, capabilities), bindings);
    }

    private static ExecutionPlan plan(Map<String, CompiledNode> nodes) {
        return new ExecutionPlan(new FlowIdentity("tenant", "flow", "v1"), "hash", nodes,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static ExecutionRequest request(ExecutionPurpose purpose, Map<String, Object> startInputs,
            Map<String, Map<String, Object>> scopes) {
        return new ExecutionRequest("inline:flow", ExecutionMode.EPHEMERAL, purpose,
                startInputs, scopes, null, "key", "tenant",
                Instant.now().plus(10, ChronoUnit.SECONDS), Map.of());
    }

    private static InputSlotContract input(boolean required, ProtocolSchema protocol) {
        return new InputSlotContract("input", required, protocol);
    }

    private static InputBinding binding(String sourceNodeId, String outputKey) {
        return new InputBinding(outputKey, sourceNodeId, outputKey);
    }

    private static NodeResult join(NodeInvocation invocation) {
        return invocation.completion().toCompletableFuture().join();
    }

    private static void assertNodeFailure(String code, CompletionException failure) {
        assertTrue(failure.getCause() instanceof NodeExecutionException);
        assertEquals(code, ((NodeExecutionException) failure.getCause()).code());
    }

    private static ExecutionPlan integrationPlan() {
        FlowNode start = node("start", "builtin.start", Map.of(
                "inputProtocols", List.of(inputProtocol("message", TEXT))));
        FlowNode script = node("script", "builtin.script", Map.of(
                "groovyScript", "return play.prefix + ctx.start.message"));
        FlowNode business = node("business", "business.component", Map.of(
                "businessOption", "kept",
                "inputBindings", List.of(Map.of(
                        "inputKey", "input", "sourceNodeId", "script", "sourceOutputKey", "result"))));
        FlowNode end = node("end", "builtin.end", Map.of(
                "resultStatus", "SUCCESS",
                "returnBinding", Map.of("sourceNodeId", "business", "sourceOutputKey", "result")));
        List<FlowEdge> edges = List.of(
                edge("e1", "start", "script"), edge("e2", "script", "business"),
                edge("e3", "business", "end"));
        Map<String, CompiledComponentContract> contracts = Map.of(
                "builtin.start", contract("builtin.start", ComponentType.START, null, Map.of(),
                        Map.of("message", new OutputSlotContract("message", TEXT)), PURE),
                "builtin.script", contract("builtin.script", ComponentType.SCRIPT, null, Map.of(),
                        Map.of("result", new OutputSlotContract("result", TEXT)), PURE),
                "business.component", contract("business.component", ComponentType.BUSINESS,
                        "business.executor", Map.of("input", input(true, TEXT)),
                        Map.of("result", new OutputSlotContract("result", TEXT)), PURE),
                "builtin.end", contract("builtin.end", ComponentType.END, null, Map.of(), Map.of(), PURE));
        ExecutableFlowBundle unhashed = new ExecutableFlowBundle("1.0",
                new FlowIdentity("tenant", "flow", "v1"), new FlowDefinition("2.0",
                        List.of(start, script, business, end), edges),
                contracts, Map.of("text", TEXT), Map.of(), null, null);
        return new FlowCompiler().compile(unhashed.withCalculatedHash());
    }

    private static FlowNode node(String id, String componentCode, Map<String, Object> config) {
        return new FlowNode(id, componentCode, id, id, config);
    }

    private static FlowEdge edge(String id, String source, String target) {
        return new FlowEdge(id, source, target, "NEXT", "IN");
    }

    private static Map<String, Object> inputProtocol(String key, ProtocolSchema protocol) {
        return Map.of("key", key, "name", key, "protocolCode", protocol.code(),
                "required", true, "schema", protocol.schema());
    }

    private static CompiledComponentContract contract(String code, ComponentType type, String executorRef,
            Map<String, InputSlotContract> inputs, Map<String, OutputSlotContract> outputs,
            ComponentCapabilities capabilities) {
        return new CompiledComponentContract(code, type, executorRef, Map.of(), inputs, outputs, capabilities);
    }

    private static ProtocolSchema protocol(String code, Map<String, Object> schema) {
        return new ProtocolSchema(code, "1.0", schema);
    }

    private static final class CollectingLogger implements ExecutionLogger {

        private final List<ExecutionLogEntry> entries = new ArrayList<>();

        @Override
        public void write(ExecutionLogEntry entry) {
            entries.add(entry);
        }
    }
}
