package com.playrewards.flowengine.server.demo;

import com.playrewards.flowengine.contract.bundle.*;
import com.playrewards.flowengine.contract.component.*;
import com.playrewards.flowengine.contract.flow.*;
import com.playrewards.flowengine.contract.protocol.ProtocolSchema;
import com.playrewards.flowengine.spi.FlowBundleLoader;

import java.util.*;

/** Trusted immutable demo catalog, available only when a host explicitly enables local examples. */
public final class DemoBundleLoader implements FlowBundleLoader {
    private static final ComponentCapabilities PURE =
            new ComponentCapabilities(ComponentEffect.PURE, true, false, false, 64);
    private static final ProtocolSchema STRING = protocol("demoString", Map.of("type", "string"));
    private static final ProtocolSchema NUMBER = protocol("demoNumber", Map.of("type", "integer"));
    private static final ProtocolSchema ARRAY =
            protocol("demoNumbers", Map.of("type", "array", "items", Map.of("type", "integer")));
    private static final ProtocolSchema OBJECT = protocol("demoObject", Map.of("type", "object"));
    private final Map<String, ExecutableFlowBundle> bundles;

    /** Builds a finite catalog; the loader never reads caller files or external resources. */
    public DemoBundleLoader() {
        this("http://127.0.0.1:9/demo");
    }

    /** Binds the full-node example to the local fixture owned by its explicitly enabled host. */
    public DemoBundleLoader(String httpUrl) {
        bundles =
                Map.of(
                        "main",
                        main(),
                        "all",
                        all(httpUrl),
                        "child",
                        child(),
                        "slow",
                        slow(),
                        "failure",
                        failure());
    }

    @Override
    public String scheme() {
        return "demo";
    }

    /** Only fixed demo references are supported; malformed and unknown references fail closed. */
    @Override
    public ExecutableFlowBundle load(String reference) {
        if (reference == null || !reference.startsWith("demo:")) throw invalid();
        var bundle = bundles.get(reference.substring(5));
        if (bundle == null) throw invalid();
        return bundle;
    }

    /** Resolves a child only inside the same demo tenant and version. */
    public ExecutableFlowBundle resolve(FlowIdentity parent, String childId) {
        if (!"demo".equals(parent.tenantId()) || !"v1".equals(parent.version())) throw invalid();
        return load("demo:" + childId);
    }

    private static ExecutableFlowBundle main() {
        return bundle(
                "main",
                List.of(
                        node("start", "start", Map.of()),
                        node("stream", "stream", Map.of()),
                        node("parallel", "parallel", Map.of()),
                        node("left", "numberScript", Map.of("groovyScript", "return 1")),
                        node("right", "numberScript", Map.of("groovyScript", "return 2")),
                        node(
                                "join",
                                "arrayScript",
                                Map.of(
                                        "groovyScript",
                                        "return [ctx.left.result, ctx.right.result]")),
                        node(
                                "loop",
                                "loop",
                                Map.of(
                                        "role",
                                        "START",
                                        "pairNodeId",
                                        "loopEnd",
                                        "collectionMode",
                                        "UPSTREAM",
                                        "collectionBinding",
                                        source("join", "result"),
                                        "itemAlias",
                                        "item",
                                        "indexAlias",
                                        "index",
                                        "maxIterations",
                                        10,
                                        "resultBinding",
                                        source("body", "result"))),
                        node(
                                "body",
                                "numberScript",
                                Map.of(
                                        "groovyScript",
                                        "println '循环演示'; return ctx.loop.item * 10")),
                        node("loopEnd", "loopEnd", Map.of("role", "END", "pairNodeId", "loop")),
                        node("sub", "sub", Map.of("targetFlowId", "child")),
                        node(
                                "summary",
                                "objectScript",
                                Map.of(
                                        "groovyScript",
                                        "return [results: ctx.loop.results, child:"
                                                + " ctx.sub.result]")),
                        node("end", "end", success("summary"))),
                List.of(
                        edge("start", "stream", "NEXT", "IN"),
                        edge("stream", "parallel", "NEXT", "IN"),
                        edge("parallel", "left", "BRANCH", "IN"),
                        edge("parallel", "right", "BRANCH", "IN"),
                        edge("left", "join", "NEXT", "IN"),
                        edge("right", "join", "NEXT", "IN"),
                        edge("join", "loop", "NEXT", "ENTRY"),
                        edge("loop", "body", "BODY", "IN"),
                        edge("body", "loopEnd", "NEXT", "IN"),
                        edge("loopEnd", "loop", "NEXT", "NEXT"),
                        edge("loop", "sub", "DONE", "IN"),
                        edge("sub", "summary", "NEXT", "IN"),
                        edge("summary", "end", "NEXT", "IN")));
    }

    private static ExecutableFlowBundle child() {
        return bundle(
                "child",
                List.of(
                        node("start", "start", Map.of()),
                        node(
                                "value",
                                "objectScript",
                                Map.of("groovyScript", "return [message: 'child-complete']")),
                        node("end", "end", success("value"))),
                List.of(edge("start", "value", "NEXT", "IN"), edge("value", "end", "NEXT", "IN")));
    }

    private static ExecutableFlowBundle slow() {
        return bundle(
                "slow",
                List.of(
                        node("start", "start", Map.of()),
                        node("wait", "slow", Map.of()),
                        node("end", "end", success("wait"))),
                List.of(edge("start", "wait", "NEXT", "IN"), edge("wait", "end", "NEXT", "IN")));
    }

    private static ExecutableFlowBundle failure() {
        return bundle(
                "failure",
                List.of(
                        node("start", "start", Map.of()),
                        node(
                                "invalid",
                                "numberScript",
                                Map.of("groovyScript", "return 'wrong-type'")),
                        node("end", "end", success("invalid"))),
                List.of(
                        edge("start", "invalid", "NEXT", "IN"),
                        edge("invalid", "end", "NEXT", "IN")));
    }

    private static ExecutableFlowBundle all(String httpUrl) {
        var main = main().flowDefinition();
        List<FlowNode> nodes = new ArrayList<>(main.nodes());
        nodes.removeIf(n -> n.id().equals("summary"));
        nodes.addAll(
                List.of(
                        node(
                                "condition",
                                "condition",
                                Map.of("conditionMode", "GROOVY", "groovyScript", "return true")),
                        node(
                                "fallback",
                                "stringScript",
                                Map.of("groovyScript", "return 'fallback'")),
                        node(
                                "selected",
                                "variable",
                                Map.of(
                                        "valueMode",
                                        "FIXED",
                                        "valueSources",
                                        List.of(
                                                source("stream", "result"),
                                                source("fallback", "result")))),
                        node(
                                "acquire",
                                "lock",
                                Map.of(
                                        "role",
                                        "ACQUIRE",
                                        "pairNodeId",
                                        "release",
                                        "lockKey",
                                        "return 'demo-all'",
                                        "waitMillis",
                                        1000,
                                        "leaseMillis",
                                        10000)),
                        node("release", "lock", Map.of("role", "RELEASE", "pairNodeId", "acquire")),
                        node(
                                "http",
                                "http",
                                Map.of(
                                        "method",
                                        "GET",
                                        "url",
                                        httpUrl,
                                        "requestParamsScript",
                                        "return [selected: ctx.selected]",
                                        "headers",
                                        Map.of(),
                                        "timeoutMillis",
                                        3000,
                                        "retryCount",
                                        0,
                                        "outputProtocol",
                                        Map.of("code", OBJECT.code(), "schema", OBJECT.schema()))),
                        node(
                                "summary",
                                "objectScript",
                                Map.of(
                                        "groovyScript",
                                        "return [results: ctx.loop.results, child: ctx.sub.result,"
                                            + " selected: ctx.selected, http: ctx.http.result]"))));
        List<FlowEdge> edges = new ArrayList<>(main.edges());
        edges.removeIf(
                e ->
                        Set.of("start_stream", "stream_parallel", "sub_summary", "summary_end")
                                .contains(e.id()));
        edges.addAll(
                List.of(
                        edge("start", "condition", "NEXT", "IN"),
                        edge("condition", "stream", "TRUE", "IN"),
                        edge("condition", "fallback", "FALSE", "IN"),
                        edge("stream", "selected", "NEXT", "IN"),
                        edge("fallback", "selected", "NEXT", "IN"),
                        edge("selected", "acquire", "NEXT", "IN"),
                        edge("acquire", "parallel", "NEXT", "IN"),
                        edge("sub", "http", "NEXT", "IN"),
                        edge("http", "summary", "NEXT", "IN"),
                        edge("summary", "release", "NEXT", "IN"),
                        edge("release", "end", "NEXT", "IN")));
        return bundle("all", nodes, edges);
    }

    private static ExecutableFlowBundle bundle(
            String id, List<FlowNode> nodes, List<FlowEdge> edges) {
        Map<String, CompiledComponentContract> contracts = new LinkedHashMap<>();
        contracts.put("start", contract("start", ComponentType.START, Map.of()));
        contracts.put("end", contract("end", ComponentType.END, Map.of()));
        contracts.put("parallel", contract("parallel", ComponentType.PARALLEL, Map.of()));
        contracts.put("condition", contract("condition", ComponentType.IF, Map.of()));
        contracts.put("variable", contract("variable", ComponentType.VARIABLE, output(STRING)));
        contracts.put(
                "stringScript", contract("stringScript", ComponentType.SCRIPT, output(STRING)));
        contracts.put("lock", contract("lock", ComponentType.LOCK, Map.of()));
        contracts.put(
                "http",
                contract(
                        "http",
                        ComponentType.HTTP,
                        Map.of("result", new OutputSlotContract("result", OBJECT))));
        contracts.put(
                "numberScript", contract("numberScript", ComponentType.SCRIPT, output(NUMBER)));
        contracts.put("arrayScript", contract("arrayScript", ComponentType.SCRIPT, output(ARRAY)));
        contracts.put(
                "objectScript", contract("objectScript", ComponentType.SCRIPT, output(OBJECT)));
        contracts.put(
                "loop",
                contract(
                        "loop",
                        ComponentType.LOOP,
                        Map.of(
                                "count",
                                new OutputSlotContract("轮次", NUMBER),
                                "results",
                                new OutputSlotContract("结果", ARRAY))));
        contracts.put("loopEnd", contract("loopEnd", ComponentType.LOOP, Map.of()));
        contracts.put("sub", contract("sub", ComponentType.SUB_FLOW, output(OBJECT)));
        contracts.put(
                "stream",
                new CompiledComponentContract(
                        "stream",
                        ComponentType.BUSINESS,
                        "demo.stream",
                        Map.of(),
                        Map.of(),
                        output(STRING),
                        DemoStreamComponent.CAPABILITIES));
        contracts.put(
                "slow",
                new CompiledComponentContract(
                        "slow",
                        ComponentType.BUSINESS,
                        "demo.slow",
                        Map.of(),
                        Map.of(),
                        output(STRING),
                        PURE));
        Set<String> used = new HashSet<>();
        nodes.forEach(node -> used.add(node.componentCode()));
        contracts.keySet().retainAll(used);
        return new ExecutableFlowBundle(
                        "1.0",
                        new FlowIdentity("demo", id, "v1"),
                        new FlowDefinition("2.0", nodes, edges),
                        contracts,
                        Map.of(
                                STRING.code(),
                                STRING,
                                NUMBER.code(),
                                NUMBER,
                                ARRAY.code(),
                                ARRAY,
                                OBJECT.code(),
                                OBJECT),
                        Map.of(),
                        null,
                        null)
                .withCalculatedHash();
    }

    private static CompiledComponentContract contract(
            String code, ComponentType type, Map<String, OutputSlotContract> outputs) {
        return new CompiledComponentContract(code, type, null, Map.of(), Map.of(), outputs, PURE);
    }

    private static Map<String, OutputSlotContract> output(ProtocolSchema protocol) {
        return Map.of("result", new OutputSlotContract("返回数据", protocol));
    }

    private static ProtocolSchema protocol(String code, Map<String, Object> schema) {
        return new ProtocolSchema(code, "1.0", schema);
    }

    private static FlowNode node(String id, String component, Map<String, Object> config) {
        return new FlowNode(id, component, id, "演示 " + id, config);
    }

    private static FlowEdge edge(String from, String to, String sourcePort, String targetPort) {
        return new FlowEdge(from + "_" + to, from, to, sourcePort, targetPort);
    }

    private static Map<String, Object> source(String node, String output) {
        return Map.of("sourceNodeId", node, "sourceOutputKey", output);
    }

    private static Map<String, Object> success(String node) {
        return Map.of("resultStatus", "SUCCESS", "returnBinding", source(node, "result"));
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Unknown or forbidden demo flow reference");
    }
}
