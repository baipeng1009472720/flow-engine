package com.playrewards.flowengine.spi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.playrewards.flowengine.contract.execution.ContentChunk;
import com.playrewards.flowengine.contract.execution.ExecutionPurpose;
import com.playrewards.flowengine.contract.execution.ExecutionRequest;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Verifies that public SPI boundaries retain only immutable, safe and protocol-correct values. */
class SpiBoundaryTest {

    @Test
    void context_loader_accepts_only_the_stable_context_load_request() throws Exception {
        Class<?> requestType = Class.forName("com.playrewards.flowengine.spi.ContextLoadRequest");

        assertTrue(requestType.isRecord());
        assertEquals(List.of("contextReference", "tenantId"), Arrays.stream(requestType.getRecordComponents())
                .map(RecordComponent::getName).toList());
        assertEquals(requestType, ExecutionContextLoader.class.getMethod("load", requestType).getParameterTypes()[0]);
        assertThrows(NoSuchMethodException.class,
                () -> ExecutionContextLoader.class.getMethod("load", ExecutionRequest.class));
    }

    @Test
    void logger_has_no_raw_map_sink_and_log_entries_copy_attributes() throws Exception {
        assertFalse(Arrays.stream(ExecutionLogger.class.getMethods()).anyMatch(method -> method.getName().equals("log")));

        Class<?> entryType = Class.forName("com.playrewards.flowengine.spi.ExecutionLogEntry");
        Method attributes = entryType.getMethod("attributes");
        Map<String, Object> source = new java.util.LinkedHashMap<>();
        Map<String, Object> sourceNested = new java.util.LinkedHashMap<>();
        sourceNested.put("value", "original");
        source.put("nested", sourceNested);
        Object entry = entryType.getConstructor(ExecutionLogLevel.class, String.class, Throwable.class, Map.class)
                .newInstance(ExecutionLogLevel.INFO, "safe", new IllegalStateException("not-an-attribute"), source);
        sourceNested.put("value", "mutated");
        source.put("new", true);

        @SuppressWarnings("unchecked")
        Map<String, Object> copied = (Map<String, Object>) attributes.invoke(entry);
        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) copied.get("nested");
        assertEquals("original", nested.get("value"));
        assertFalse(copied.containsKey("new"));
        assertThrows(UnsupportedOperationException.class, () -> nested.put("blocked", true));
        assertFalse(entry.toString().contains("not-an-attribute"));
    }

    @Test
    void empty_content_rejects_non_positive_demand_once() {
        RecordingSubscriber subscriber = new RecordingSubscriber(0, false);

        ComponentInvocation.emptyContent().subscribe(subscriber);

        assertEquals(1, subscriber.errorCount);
        assertTrue(subscriber.error instanceof IllegalArgumentException);
        assertEquals(0, subscriber.completeCount);
        subscriber.subscription.request(-1);
        assertEquals(1, subscriber.errorCount);
    }

    @Test
    void empty_content_honors_cancel_before_a_positive_demand() {
        RecordingSubscriber subscriber = new RecordingSubscriber(1, true);

        ComponentInvocation.emptyContent().subscribe(subscriber);

        assertEquals(0, subscriber.errorCount);
        assertEquals(0, subscriber.completeCount);
    }

    @Test
    void empty_content_completes_once_on_the_first_positive_demand() {
        RecordingSubscriber subscriber = new RecordingSubscriber(1, false);

        ComponentInvocation.emptyContent().subscribe(subscriber);

        assertEquals(0, subscriber.errorCount);
        assertEquals(1, subscriber.completeCount);
        subscriber.subscription.request(1);
        subscriber.subscription.cancel();
        assertEquals(1, subscriber.completeCount);
        assertEquals(0, subscriber.errorCount);
    }

    @Test
    void secret_value_copies_input_and_output_arrays_without_mutable_lifecycle_operations() throws Exception {
        char[] source = "secret".toCharArray();
        SecretValue value = new SecretValue(source);
        source[0] = 'X';
        char[] exposed = value.copyCharacters();
        exposed[1] = 'Y';

        assertArrayEquals("secret".toCharArray(), value.copyCharacters());
        assertFalse(Arrays.stream(SecretValue.class.getMethods())
                .anyMatch(method -> method.getName().equals("destroy") || method.getName().equals("close")));
        assertEquals("SecretValue[<redacted>]", value.toString());
    }

    @Test
    void secret_value_returns_consistent_copies_to_concurrent_readers() throws InterruptedException {
        SecretValue value = new SecretValue("stable".toCharArray());
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(4);
        List<char[]> observed = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        for (int index = 0; index < 4; index++) {
            Thread.startVirtualThread(() -> {
                ready.countDown();
                try {
                    start.await();
                    observed.add(value.copyCharacters());
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });
        }
        assertTrue(ready.await(1, TimeUnit.SECONDS));
        start.countDown();
        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertEquals(4, observed.size());
        observed.forEach(copy -> assertArrayEquals("stable".toCharArray(), copy));
    }

    @Test
    void component_value_objects_and_composite_context_have_redacted_to_strings() {
        String marker = "private-marker-value";
        ComponentConfig config = new ComponentConfig(Map.of("secretConfig", marker));
        ComponentInputs inputs = new ComponentInputs(
                Map.of("requiredSecret", marker), Map.of("optionalSecret", marker));
        ReadOnlyExecutionContext context = new ReadOnlyExecutionContext(
                Map.of("privateScope", Map.of("secret", marker)));
        ComponentOutputs outputs = new ComponentOutputs(Map.of("secretOutput", marker));
        ExecutionLogger logger = new ExecutionLogger() {
            @Override
            public void write(ExecutionLogEntry entry) {
            }

            @Override
            public String toString() {
                return marker;
            }
        };
        CancellationToken cancellation = new CancellationToken() {
            @Override
            public boolean isCancellationRequested() {
                return false;
            }

            @Override
            public String toString() {
                return marker;
            }
        };
        ComponentExecutionContext composite = new ComponentExecutionContext(
                config, inputs, context, logger, cancellation, ExecutionPurpose.PRODUCTION);

        for (Object value : List.of(config, inputs, context, outputs, composite)) {
            assertFalse(value.toString().contains(marker), value.getClass().getSimpleName());
        }
        assertTrue(config.toString().contains("secretConfig"));
        assertTrue(inputs.toString().contains("requiredSecret"));
        assertTrue(context.toString().contains("privateScope"));
        assertTrue(outputs.toString().contains("secretOutput"));
        assertTrue(composite.toString().contains("loggerType"));
    }

    @Test
    void script_invocation_rejects_an_unbounded_string_token_before_copying_context() {
        Map<String, Map<String, Object>> scopes = Map.of(
                "play", Map.of("oversized", "x".repeat(1_048_577)));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new ScriptRuntime.ScriptInvocation("return true", scopes, Map.of(),
                        ExecutionLogger.NOOP, () -> false, Instant.now().plusSeconds(1)));

        assertEquals("script invocation context exceeds the hard resource limit", failure.getMessage());
    }

    @Test
    void script_invocation_counts_null_values_toward_the_pre_copy_node_limit() {
        Map<String, Map<String, Object>> scopes = Map.of(
                "play", Map.of("items", java.util.Collections.nCopies(100_001, null)));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new ScriptRuntime.ScriptInvocation("return true", scopes, Map.of(),
                        ExecutionLogger.NOOP, () -> false, Instant.now().plusSeconds(1)));

        assertEquals("script invocation context exceeds the hard resource limit", failure.getMessage());
    }

    @Test
    void script_invocation_uses_encoded_json_bytes_before_copying_escaped_input() {
        String exactToken = "\n".repeat(524_287);
        assertDoesNotThrow(() -> new ScriptRuntime.ScriptInvocation("return true",
                Map.of("play", Map.of("exact", exactToken)), Map.of(),
                ExecutionLogger.NOOP, () -> false, Instant.now().plusSeconds(1)));

        String overToken = exactToken + "\n";
        IllegalArgumentException tokenFailure = assertThrows(IllegalArgumentException.class,
                () -> new ScriptRuntime.ScriptInvocation("return true",
                        Map.of("play", Map.of("over", overToken)), Map.of(),
                        ExecutionLogger.NOOP, () -> false, Instant.now().plusSeconds(1)));
        assertEquals("script invocation context exceeds the hard resource limit", tokenFailure.getMessage());

        String repeated = "\n".repeat(500_000);
        IllegalArgumentException totalFailure = assertThrows(IllegalArgumentException.class,
                () -> new ScriptRuntime.ScriptInvocation("return true", Map.of("play", Map.of(
                        "one", repeated, "two", repeated, "three", repeated,
                        "four", repeated, "five", repeated)), Map.of(),
                        ExecutionLogger.NOOP, () -> false, Instant.now().plusSeconds(1)));
        assertEquals("script invocation context exceeds the hard resource limit", totalFailure.getMessage());
    }

    @Test
    void script_invocation_rejects_fastjson_plain_big_decimal_before_copying() {
        java.math.BigDecimal hugePlain = new java.math.BigDecimal(
                java.math.BigInteger.ONE, 4_194_304);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new ScriptRuntime.ScriptInvocation("return true",
                        Map.of("play", Map.of("amount", hugePlain)), Map.of(),
                        ExecutionLogger.NOOP, () -> false, Instant.now().plusSeconds(1)));

        assertEquals("script invocation context exceeds the hard resource limit", failure.getMessage());
    }

    private static final class RecordingSubscriber implements Flow.Subscriber<ContentChunk> {

        private final long initialDemand;
        private final boolean cancelOnSubscribe;
        private Flow.Subscription subscription;
        private Throwable error;
        private int errorCount;
        private int completeCount;

        private RecordingSubscriber(long initialDemand, boolean cancelOnSubscribe) {
            this.initialDemand = initialDemand;
            this.cancelOnSubscribe = cancelOnSubscribe;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (cancelOnSubscribe) {
                subscription.cancel();
            } else {
                subscription.request(initialDemand);
            }
        }

        @Override
        public void onNext(ContentChunk item) {
            throw new AssertionError("empty publisher must not emit items");
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
            errorCount++;
        }

        @Override
        public void onComplete() {
            completeCount++;
        }
    }
}
