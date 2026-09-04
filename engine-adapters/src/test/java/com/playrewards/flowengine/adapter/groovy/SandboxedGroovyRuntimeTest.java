package com.playrewards.flowengine.adapter.groovy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.spi.CancellationToken;
import com.playrewards.flowengine.spi.ExecutionLogEntry;
import com.playrewards.flowengine.spi.ExecutionLogLevel;
import com.playrewards.flowengine.spi.ExecutionLogger;
import com.playrewards.flowengine.spi.ScriptRuntime;
import com.playrewards.flowengine.spi.ScriptRuntime.ScriptExecutionException;
import com.playrewards.flowengine.spi.ScriptRuntime.ScriptInvocation;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Exercises the real Groovy compiler and runtime security/resource boundaries. */
class SandboxedGroovyRuntimeTest {

    @Test
    void reads_read_only_scopes_and_ctx_and_redirects_prints_to_info_logs() throws Exception {
        CollectingLogger logger = new CollectingLogger();
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PrintStream original = System.out;
        Object result;
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
            try {
                result = await(runtime.execute(invocation(
                        "println(play.name); print(ctx.before.value); "
                                + "return [message: play.name + ctx.before.value]",
                        Map.of("play", Map.of("name", "Ada")),
                        Map.of("before", Map.of("value", 42)), logger, () -> false,
                        Instant.now().plusSeconds(2))));
            } finally {
                System.setOut(original);
            }
        }

        assertEquals(Map.of("message", "Ada42"), result);
        assertEquals(List.of("Ada", "42"), logger.entries.stream().map(ExecutionLogEntry::message).toList());
        assertTrue(logger.entries.stream().allMatch(entry -> entry.level() == ExecutionLogLevel.INFO));
        assertEquals("", stdout.toString(StandardCharsets.UTF_8));
    }

    @Test
    void supports_only_json_reads_and_allowlisted_business_operations() throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            Object result = await(runtime.execute(invocation("""
                    def subtotal = play.amount * 2
                    if (ctx.before.enabled && play.name.startsWith('A')) {
                        return [message: play.name.trim().toUpperCase(),
                                values: [subtotal, ctx.before.count + 1],
                                present: play.containsKey('amount')]
                    }
                    return [message: 'skipped', values: [], present: false]
                    """, Map.of("play", Map.of("name", "Ada ", "amount", 6)),
                    Map.of("before", Map.of("enabled", true, "count", 2)),
                    entry -> { }, () -> false, Instant.now().plusSeconds(2))));

            assertEquals(Map.of(
                    "message", "ADA",
                    "values", List.of(12, 3),
                    "present", true), result);
        }
    }

    @Test
    void runs_the_collection_closure_snippets_published_by_the_management_editor() throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            Object result = await(runtime.execute(invocation("""
                    def items = play.items
                    def found = items.find { item -> item.active }
                    def active = items.findAll { it.active }
                    def ids = active.collect { item -> item.id + 10 }
                    def anyActive = items.any { item -> item.active }
                    def everyNamed = items.every { it.name.startsWith('item-') }
                    return [found: found.get('id'), ids: ids, anyActive: anyActive, everyNamed: everyNamed]
                    """, Map.of("play", Map.of("items", List.of(
                            Map.of("id", 1, "name", "item-one", "active", false),
                            Map.of("id", 2, "name", "item-two", "active", true),
                            Map.of("id", 3, "name", "item-three", "active", true)))),
                    Map.of(), entry -> { }, () -> false, Instant.now().plusSeconds(2))));

            assertEquals(Map.of(
                    "found", 2,
                    "ids", List.of(12, 13),
                    "anyActive", true,
                    "everyNamed", true), result);

            Object managementSandboxResult = await(runtime.execute(invocation("""
                    def items = [1, 2]
                    def doubled = items.collect { item -> item * 2 }
                    return doubled.collect { it + 1 }
                    """, Map.of(), Map.of(), entry -> { }, () -> false,
                    Instant.now().plusSeconds(2))));
            assertEquals(List.of(3, 5), managementSandboxResult);
        }
    }

    @Test
    void rejects_collection_closure_capture_nesting_side_effects_and_non_list_receivers() {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            for (String script : List.of(
                    "return [1, 2].collect { item -> ctx.safe }",
                    "return [1, 2].collect { item -> println(item); item }",
                    "return [[1]].collect { item -> item.collect { it } }",
                    "return [[1]].collect { item -> item }",
                    "def helper = { it + 1 }; return helper(1)",
                    "return play.object.collect { it }")) {
                assertScriptCode(runtime, script, "SCRIPT_SECURITY_VIOLATION",
                        Map.of("play", Map.of("object", Map.of("value", 1))),
                        Map.of("safe", true));
            }
        }
    }

    @Test
    void collection_closure_consumes_growth_budget_before_allocation_and_runtime_remains_reusable()
            throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 64, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            assertScriptCode(runtime,
                    "return play.items.collect { item -> item + '-expanded' }",
                    "SCRIPT_OPERATION_LIMIT_EXCEEDED",
                    Map.of("play", Map.of("items", List.of(
                            "one", "two", "three", "four", "five", "six", "seven"))),
                    Map.of());
            assertEquals(11, await(runtime.execute(invocation("return 11", Map.of(), Map.of(),
                    entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
        }
    }

    @ParameterizedTest(name = "rejects escape: {0}")
    @MethodSource("forbiddenScripts")
    void rejects_file_process_reflection_class_loading_thread_and_network_escapes(String label, String script) {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            CompletionException failure = assertThrows(CompletionException.class,
                    () -> runtime.execute(invocation(script, Map.of(), Map.of("safe", Map.of()),
                            entry -> { }, () -> false, Instant.now().plusSeconds(2)))
                            .toCompletableFuture().join());

            assertTrue(failure.getCause() instanceof ScriptExecutionException, label);
            assertEquals("SCRIPT_SECURITY_VIOLATION",
                    ((ScriptExecutionException) failure.getCause()).code(), label);
        }
    }

    @Test
    void rejects_property_getters_even_when_the_receiver_arrives_through_json_ctx() {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            CompletionException failure = assertThrows(CompletionException.class,
                    () -> runtime.execute(invocation("return ctx.secret.allWhitespace", Map.of(),
                            Map.of("secret", "   "),
                            entry -> { }, () -> false, Instant.now().plusSeconds(2)))
                            .toCompletableFuture().join());

            assertTrue(failure.getCause() instanceof ScriptExecutionException);
            assertEquals("SCRIPT_SECURITY_VIOLATION",
                    ((ScriptExecutionException) failure.getCause()).code());
        }
    }

    @Test
    void enforces_timeout_and_cancellation_and_closes_without_waiting_for_stuck_scripts() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                    Duration.ofMillis(75), 10_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC());
            CompletionException timeout = assertThrows(CompletionException.class,
                    () -> runtime.execute(invocation("while (true) { }", Map.of(), Map.of(),
                            entry -> { }, () -> false, Instant.now().plusSeconds(5)))
                            .toCompletableFuture().join());
            assertEquals("SCRIPT_TIMEOUT", ((ScriptExecutionException) timeout.getCause()).code());
            runtime.close();
        });

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            AtomicBoolean cancelled = new AtomicBoolean();
            try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                    Duration.ofSeconds(5), 10_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
                var completion = runtime.execute(invocation("while (true) { }", Map.of(), Map.of(),
                        entry -> { }, cancelled::get, Instant.now().plusSeconds(5)));
                Thread.sleep(50);
                cancelled.set(true);
                assertThrows(CancellationException.class,
                        () -> completion.toCompletableFuture().join());
            }
        });
    }

    @Test
    void cancelling_returned_future_stops_the_real_worker_and_releases_runtime_for_reuse() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                    Duration.ofSeconds(5), 10_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
                var running = runtime.execute(invocation("while (true) { }", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(5))).toCompletableFuture();
                awaitActiveInvocations(runtime, 1);

                assertTrue(running.cancel(true));
                assertThrows(CancellationException.class, running::join);
                awaitActiveInvocations(runtime, 0);

                assertEquals(7, await(runtime.execute(invocation("return 7", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
            }
        });
    }

    @Test
    void caller_cannot_forge_completion_while_the_real_worker_is_still_running() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                    Duration.ofSeconds(5), 10_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
                var running = runtime.execute(invocation("while (true) { }", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(5))).toCompletableFuture();
                awaitActiveInvocations(runtime, 1);

                assertFalse(running.complete("forged"));
                assertFalse(running.isDone());
                assertEquals(1, runtime.activeInvocationCount());

                running.cancel(true);
                awaitActiveInvocations(runtime, 0);
            }
        });
    }

    @Test
    void one_finite_admission_covers_compile_and_execution_until_the_worker_really_stops() {
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                    Duration.ofSeconds(5), 10_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC(),
                    1, 1_024, 8_192, 32)) {
                var running = runtime.execute(invocation("while (true) { }", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(5))).toCompletableFuture();
                awaitActiveInvocations(runtime, 1);

                assertScriptCode(runtime, "return 2", "SCRIPT_CAPACITY_EXCEEDED");
                running.cancel(true);
                awaitActiveInvocations(runtime, 0);
                assertEquals(3, await(runtime.execute(invocation("return 3", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
            }
        });
    }

    @Test
    void rejects_input_bytes_depth_nodes_and_large_string_tokens_without_leaking_values() {
        String marker = "input-marker-";
        Map<String, Map<String, Object>> oversizedBytes = Map.of("play", Map.of(
                "one", "a".repeat(240_000), "two", "b".repeat(240_000),
                "three", "c".repeat(240_000), "four", "d".repeat(240_000),
                "five", "e".repeat(240_000)));
        Map<String, Map<String, Object>> oversizedToken = Map.of(
                "play", Map.of("secret", marker + "x".repeat(300_000)));
        Object nested = true;
        for (int depth = 0; depth < 40; depth++) {
            nested = Map.of("next", nested);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> deepScope = (Map<String, Object>) nested;
        Map<String, Map<String, Object>> oversizedDepth = Map.of("play", deepScope);
        Map<String, Map<String, Object>> oversizedNodes = Map.of(
                "play", Map.of("items", java.util.Collections.nCopies(10_001, true)));

        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            for (Map<String, Map<String, Object>> scopes : List.of(
                    oversizedBytes, oversizedToken, oversizedDepth, oversizedNodes)) {
                CompletionException failure = assertThrows(CompletionException.class,
                        () -> runtime.execute(invocation("return true", scopes, Map.of(),
                                entry -> { }, () -> false, Instant.now().plusSeconds(2)))
                                .toCompletableFuture().join());
                assertEquals("SCRIPT_INPUT_TOO_LARGE",
                        ((ScriptExecutionException) failure.getCause()).code());
                assertFalse(failure.getCause().toString().contains(marker));
            }
        }
    }

    @Test
    void rejects_string_and_list_amplification_before_allocation_budget_and_remains_reusable() throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 64, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            assertScriptCode(runtime, "return 'x' * 100000", "SCRIPT_OPERATION_LIMIT_EXCEEDED");
            assertScriptCode(runtime, "return [1] * 100000", "SCRIPT_OPERATION_LIMIT_EXCEEDED");
            assertEquals(7, await(runtime.execute(invocation("return 7", Map.of(), Map.of(),
                    entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
        }
    }

    @Test
    void repeated_references_in_list_and_map_literals_consume_budget_before_container_allocation()
            throws Exception {
        String payload = "payload-".repeat(8);
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 96, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            Map<String, Map<String, Object>> scopes = Map.of("play", Map.of("payload", payload));
            assertScriptCode(runtime,
                    "return [play.payload, play.payload, play.payload]",
                    "SCRIPT_OPERATION_LIMIT_EXCEEDED", scopes, Map.of());
            assertScriptCode(runtime,
                    "return [one: play.payload, two: play.payload, three: play.payload]",
                    "SCRIPT_OPERATION_LIMIT_EXCEEDED", scopes, Map.of());
            assertEquals(13, await(runtime.execute(invocation("return 13", Map.of(), Map.of(),
                    entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
        }
    }

    @Test
    void removes_unbudgeted_join_and_replace_expansion_paths() {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 64, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            assertScriptCode(runtime, "return [1, 2].join('x')", "SCRIPT_SECURITY_VIOLATION");
            assertScriptCode(runtime, "return 'xx'.replace('x', 'y' * 20)", "SCRIPT_SECURITY_VIOLATION");
        }
    }

    @Test
    void case_conversion_reserves_its_worst_case_growth_before_allocating() throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 3, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            assertScriptCode(runtime, "return 'ß'.toUpperCase()", "SCRIPT_OPERATION_LIMIT_EXCEEDED");
            assertEquals(1, await(runtime.execute(invocation("return 1", Map.of(), Map.of(),
                    entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
        }
    }

    @Test
    void string_growth_reserves_json_escape_bytes_before_allocating() throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 64, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            for (String script : List.of(
                    "return '\\n' * 40", "return '\"' * 40", "return '\\\\' * 40")) {
                assertScriptCode(runtime, script, "SCRIPT_OPERATION_LIMIT_EXCEEDED");
            }
            assertEquals(1, await(runtime.execute(invocation("return 1", Map.of(), Map.of(),
                    entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
        }
    }

    @Test
    void pre_serialization_measurement_counts_valid_unicode_surrogate_pairs_as_one_code_point()
            throws Exception {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 6, 8, Duration.ofMinutes(1), Clock.systemUTC())) {
            assertEquals("😀", await(runtime.execute(invocation("return '😀'", Map.of(), Map.of(),
                    entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
        }
    }

    @Test
    void compilation_itself_is_async_admitted_timed_and_cancelled_before_runtime_reuse() {
        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
            StringBuilder source = new StringBuilder();
            for (int index = 0; index < 4_000; index++) {
                source.append("def value").append(index).append(" = ").append(index).append('\n');
            }
            source.append("return value3999");
            AtomicReference<java.util.concurrent.CompletionStage<Object>> completion = new AtomicReference<>();
            try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                    Duration.ofSeconds(2), 100_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC(),
                    1, 1_024, 8_192, 32)) {
                assertEquals(9, await(runtime.execute(invocation("return 9", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
                assertTimeoutPreemptively(Duration.ofMillis(100), () -> completion.set(runtime.execute(
                        invocation(source.toString(), Map.of(), Map.of(), entry -> { }, () -> false,
                                Instant.now().plusMillis(50)))));

                CompletionException timeout = assertThrows(CompletionException.class,
                        () -> completion.get().toCompletableFuture().join());
                assertEquals("SCRIPT_TIMEOUT", ((ScriptExecutionException) timeout.getCause()).code());
                awaitActiveInvocations(runtime, 0);
                assertEquals(9, await(runtime.execute(invocation("return 9", Map.of(), Map.of(),
                        entry -> { }, () -> false, Instant.now().plusSeconds(1)))));
            }
        });
    }

    @Test
    void truncates_logs_by_line_bytes_total_bytes_and_entry_count() throws Exception {
        CollectingLogger logger = new CollectingLogger();
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 32_000, 8, Duration.ofMinutes(1), Clock.systemUTC(),
                1, 16, 24, 2)) {
            assertEquals(true, await(runtime.execute(invocation("""
                    println('你好世界你好世界')
                    println('abcdefghijklmnopqrstuvwxyz')
                    println('abcdefghijklmnopqrstuvwxyz')
                    return true
                    """, Map.of(), Map.of(), logger, () -> false, Instant.now().plusSeconds(1)))));
        }

        assertEquals(List.of("你好世[...]", "abcde[...]"),
                logger.entries.stream().map(ExecutionLogEntry::message).toList());
        assertTrue(logger.entries.stream()
                .mapToInt(entry -> entry.message().getBytes(StandardCharsets.UTF_8).length)
                .allMatch(bytes -> bytes <= 16));
        assertEquals(24, logger.entries.stream()
                .mapToInt(entry -> entry.message().getBytes(StandardCharsets.UTF_8).length).sum());
    }

    @Test
    void flushes_the_final_partial_log_line_before_completing_the_result() throws Exception {
        CollectingLogger logger = new CollectingLogger();
        List<String> messagesAtCompletion;
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            messagesAtCompletion = runtime.execute(invocation(
                            "print('tail-line'); return true", Map.of(), Map.of(), logger,
                            () -> false, Instant.now().plusSeconds(1)))
                    .thenApply(ignored -> logger.entries.stream().map(ExecutionLogEntry::message).toList())
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
        }

        assertEquals(List.of("tail-line"), messagesAtCompletion);
    }

    @Test
    void flushes_the_final_partial_log_line_before_completing_a_failure() throws Exception {
        CollectingLogger logger = new CollectingLogger();
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            List<String> messagesAtCompletion = runtime.execute(invocation(
                            "print('failure-tail'); return 1 / 0", Map.of(), Map.of(), logger,
                            () -> false, Instant.now().plusSeconds(1)))
                    .handle((ignored, failure) ->
                            logger.entries.stream().map(ExecutionLogEntry::message).toList())
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertEquals(List.of("failure-tail"), messagesAtCompletion);
        }
    }

    @Test
    void bounds_digest_keyed_cache_and_expires_entries_without_retaining_script_keys() throws Exception {
        MutableClock clock = new MutableClock(Instant.parse("2026-09-04T00:00:00Z"));
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 10_000, 32_000, 2, Duration.ofSeconds(5), clock)) {
            await(runtime.execute(invocation("return 1", Map.of(), Map.of(),
                    entry -> { }, () -> false, clock.instant().plusSeconds(20))));
            await(runtime.execute(invocation("return 2", Map.of(), Map.of(),
                    entry -> { }, () -> false, clock.instant().plusSeconds(20))));
            Set<String> initialKeys = runtime.cachedDigests();
            assertEquals(2, runtime.cacheSize());
            assertTrue(initialKeys.stream().allMatch(key -> key.matches("[0-9a-f]{64}")));
            assertTrue(initialKeys.stream().noneMatch(key -> key.contains("return")));

            await(runtime.execute(invocation("return 3", Map.of(), Map.of(),
                    entry -> { }, () -> false, clock.instant().plusSeconds(20))));
            assertEquals(2, runtime.cacheSize());

            clock.advance(Duration.ofSeconds(6));
            await(runtime.execute(invocation("return 4", Map.of(), Map.of(),
                    entry -> { }, () -> false, clock.instant().plusSeconds(20))));
            assertEquals(1, runtime.cacheSize());
        }
    }

    @Test
    void rejects_oversized_scripts_results_and_non_json_values_without_leaking_sources_or_context() {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime(
                Duration.ofSeconds(1), 40, 32, 2, Duration.ofMinutes(1), Clock.systemUTC())) {
            assertScriptCode(runtime, "return '" + "x".repeat(50) + "'", "SCRIPT_TOO_LARGE");
            assertScriptCode(runtime, "return '" + "x".repeat(31) + "'", "SCRIPT_RESULT_TOO_LARGE");
            assertScriptCode(runtime, "return new Object()", "SCRIPT_SECURITY_VIOLATION");

            String secretScript = "throw new RuntimeException('script-secret')";
            CompletionException failure = assertThrows(CompletionException.class,
                    () -> runtime.execute(invocation(secretScript,
                            Map.of("play", Map.of("token", "scope-secret")), Map.of(),
                            entry -> { }, () -> false, Instant.now().plusSeconds(2)))
                            .toCompletableFuture().join());
            assertFalse(failure.getCause().toString().contains("script-secret"));
            assertFalse(failure.getCause().toString().contains("scope-secret"));
            assertFalse(failure.getCause().toString().contains(secretScript));
        }
    }

    @Test
    void distinguishes_ordinary_compile_errors_from_security_policy_rejections() {
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            assertScriptCode(runtime, "return (", "SCRIPT_COMPILE_FAILED");
            assertScriptCode(runtime, "break", "SCRIPT_COMPILE_FAILED");
        }
    }

    @Test
    void rejects_local_ast_transforms_before_they_can_execute_during_compilation() throws Exception {
        String marker = "flow.engine.sandbox.ast.escape";
        System.clearProperty(marker);
        try (SandboxedGroovyRuntime runtime = new SandboxedGroovyRuntime()) {
            assertScriptCode(runtime,
                    "@groovy.transform.ASTTest(value={ System.setProperty('" + marker
                            + "', 'escaped') }) def marker; return true",
                    "SCRIPT_SECURITY_VIOLATION");
            assertNull(System.getProperty(marker));
            assertEquals("user@example.com", await(runtime.execute(invocation(
                    "return 'user@example.com'", Map.of(), Map.of(), entry -> { },
                    () -> false, Instant.now().plusSeconds(2)))));
        } finally {
            System.clearProperty(marker);
        }
    }

    private static Stream<org.junit.jupiter.params.provider.Arguments> forbiddenScripts() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of("file import",
                        "import java.io.File; return new File('/tmp/secret').text"),
                org.junit.jupiter.params.provider.Arguments.of("fully qualified file",
                        "return new java.io.File('/tmp/secret').text"),
                org.junit.jupiter.params.provider.Arguments.of("process GDK", "return 'whoami'.execute().text"),
                org.junit.jupiter.params.provider.Arguments.of("reflection method", "return ctx.getClass()"),
                org.junit.jupiter.params.provider.Arguments.of("reflection property", "return ctx.class"),
                org.junit.jupiter.params.provider.Arguments.of("dynamic reflection method",
                        "def method = 'getClass'; return ctx.\"${method}\"()"),
                org.junit.jupiter.params.provider.Arguments.of("dynamic reflection method pointer",
                        "def method = 'getClass'; return (ctx.&\"${method}\")()"),
                org.junit.jupiter.params.provider.Arguments.of("dynamic reflection property",
                        "def property = 'class'; return ctx.\"${property}\""),
                org.junit.jupiter.params.provider.Arguments.of("class loading",
                        "return java.lang.Class.forName('java.lang.System')"),
                org.junit.jupiter.params.provider.Arguments.of("nested evaluator",
                        "return groovy.util.Eval.me(\"return System.getProperty('user.home')\")"),
                org.junit.jupiter.params.provider.Arguments.of("script evaluator",
                        "return evaluate('return 1')"),
                org.junit.jupiter.params.provider.Arguments.of("script receiver",
                        "return this.dump()"),
                org.junit.jupiter.params.provider.Arguments.of("unlisted GDK method",
                        "return 'secret'.getBytes()"),
                org.junit.jupiter.params.provider.Arguments.of("indirect getter property",
                        "return 'secret'.bytes"),
                org.junit.jupiter.params.provider.Arguments.of("local getter property bypass",
                        "def secret = '   '; return secret.allWhitespace"),
                org.junit.jupiter.params.provider.Arguments.of("direct attribute access",
                        "return ctx.@table"),
                org.junit.jupiter.params.provider.Arguments.of("general closure value",
                        "def helper = { it + 1 }; return helper(1)"),
                org.junit.jupiter.params.provider.Arguments.of("thread",
                        "new java.lang.Thread({ -> }); return true"),
                org.junit.jupiter.params.provider.Arguments.of("network",
                        "return new java.net.URL('https://example.com').text"));
    }

    private static ScriptInvocation invocation(String script,
            Map<String, Map<String, Object>> scopes, Map<String, Object> ctx,
            ExecutionLogger logger, CancellationToken cancellation, Instant deadline) {
        return new ScriptInvocation(script, scopes, ctx, logger, cancellation, deadline);
    }

    private static Object await(java.util.concurrent.CompletionStage<Object> completion) throws Exception {
        return completion.toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static void awaitActiveInvocations(SandboxedGroovyRuntime runtime, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (runtime.activeInvocationCount() != expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, runtime.activeInvocationCount());
    }

    private static void assertScriptCode(ScriptRuntime runtime, String script, String code) {
        assertScriptCode(runtime, script, code, Map.of(), Map.of());
    }

    private static void assertScriptCode(ScriptRuntime runtime, String script, String code,
            Map<String, Map<String, Object>> scopes, Map<String, Object> ctx) {
        CompletionException failure = assertThrows(CompletionException.class,
                () -> runtime.execute(invocation(script, scopes, ctx,
                        entry -> { }, () -> false, Instant.now().plusSeconds(2)))
                        .toCompletableFuture().join());
        assertEquals(code, ((ScriptExecutionException) failure.getCause()).code());
    }

    private static final class CollectingLogger implements ExecutionLogger {

        private final List<ExecutionLogEntry> entries = new ArrayList<>();

        @Override
        public void write(ExecutionLogEntry entry) {
            entries.add(entry);
        }
    }

    private static final class MutableClock extends Clock {

        private Instant current;

        private MutableClock(Instant current) {
            this.current = current;
        }

        private void advance(Duration duration) {
            current = current.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return current;
        }
    }
}
