package com.playrewards.flowengine.adapter.groovy;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.spi.ExecutionLogger;
import com.playrewards.flowengine.spi.ScriptRuntime;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import groovy.transform.ThreadInterrupt;
import groovyjarjarantlr4.v4.runtime.CharStreams;
import groovyjarjarantlr4.v4.runtime.Token;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.groovy.parser.antlr4.GroovyLexer;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.ClassCodeExpressionTransformer;
import org.codehaus.groovy.ast.ClassHelper;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.expr.ArgumentListExpression;
import org.codehaus.groovy.ast.expr.AttributeExpression;
import org.codehaus.groovy.ast.expr.BinaryExpression;
import org.codehaus.groovy.ast.expr.BooleanExpression;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.DeclarationExpression;
import org.codehaus.groovy.ast.expr.ElvisOperatorExpression;
import org.codehaus.groovy.ast.expr.EmptyExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.GStringExpression;
import org.codehaus.groovy.ast.expr.ListExpression;
import org.codehaus.groovy.ast.expr.MapEntryExpression;
import org.codehaus.groovy.ast.expr.MapExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.NotExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
import org.codehaus.groovy.ast.expr.StaticMethodCallExpression;
import org.codehaus.groovy.ast.expr.TernaryExpression;
import org.codehaus.groovy.ast.expr.TupleExpression;
import org.codehaus.groovy.ast.expr.UnaryMinusExpression;
import org.codehaus.groovy.ast.expr.UnaryPlusExpression;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.BreakStatement;
import org.codehaus.groovy.ast.stmt.ContinueStatement;
import org.codehaus.groovy.ast.stmt.EmptyStatement;
import org.codehaus.groovy.ast.stmt.ExpressionStatement;
import org.codehaus.groovy.ast.stmt.IfStatement;
import org.codehaus.groovy.ast.stmt.ReturnStatement;
import org.codehaus.groovy.ast.stmt.Statement;
import org.codehaus.groovy.ast.stmt.WhileStatement;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.syntax.Types;

/**
 * Bounded Groovy adapter which compiles by source digest and executes only copied JSON bindings.
 *
 * <p>A closed-world AST policy admits only JSON reads, primitive expressions, structured control
 * flow and explicitly listed side-effect-free methods; all other syntax, receivers and dispatch
 * forms are rejected. Runtime interruption, deadline checks, finite cache/resource limits and
 * redacted failures form a second boundary.</p>
 */
public final class SandboxedGroovyRuntime implements ScriptRuntime, AutoCloseable {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration DEFAULT_CACHE_TTL = Duration.ofMinutes(10);
    private static final int DEFAULT_MAX_SCRIPT_LENGTH = 50_000;
    private static final int DEFAULT_MAX_RESULT_BYTES = 1_048_576;
    private static final int DEFAULT_CACHE_CAPACITY = 256;
    private static final int DEFAULT_MAX_CONCURRENT_INVOCATIONS = 64;
    private static final int DEFAULT_MAX_LOG_LINE_BYTES = 8_192;
    private static final int DEFAULT_MAX_LOG_TOTAL_BYTES = 65_536;
    private static final int DEFAULT_MAX_LOG_ENTRIES = 256;
    private static final int MAX_RESULT_DEPTH = 32;
    private static final int MAX_RESULT_NODES = 10_000;
    private static final long STOP_POLL_MILLIS = 10;
    private static final Set<String> ALLOWED_METHODS = Set.of(
            "abs", "contains", "containsKey", "containsValue", "endsWith", "get", "isEmpty",
            "join", "length", "max", "min", "replace", "size", "startsWith", "strip",
            "substring", "toLowerCase", "toUpperCase", "trim");
    private static final Set<String> LOG_METHODS = Set.of("print", "println");
    private static final Set<String> RESERVED_VARIABLES = Set.of(
            "this", "super", "owner", "delegate", "binding", "out", "metaClass");
    private static final Set<String> RESERVED_PROPERTIES = Set.of(
            "class", "metaClass", "classLoader", "declaredFields", "declaredMethods",
            "fields", "methods", "protectionDomain", "binding", "properties",
            "bytes", "chars", "codePoints");
    private static final Set<Integer> ALLOWED_BINARY_TOKENS = Set.of(
            Types.ASSIGN,
            Types.COMPARE_EQUAL, Types.COMPARE_NOT_EQUAL,
            Types.COMPARE_LESS_THAN, Types.COMPARE_LESS_THAN_EQUAL,
            Types.COMPARE_GREATER_THAN, Types.COMPARE_GREATER_THAN_EQUAL,
            Types.LOGICAL_AND, Types.LOGICAL_OR,
            Types.PLUS, Types.MINUS, Types.MULTIPLY, Types.DIVIDE, Types.INTDIV, Types.MOD,
            Types.LEFT_SQUARE_BRACKET);

    private final Duration timeout;
    private final int maxScriptLength;
    private final int maxResultBytes;
    private final int cacheCapacity;
    private final Duration cacheTtl;
    private final Clock clock;
    private final int maxLogLineBytes;
    private final int maxLogTotalBytes;
    private final int maxLogEntries;
    private final Semaphore invocationAdmission;
    private final ExecutorService tasks = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("flow-groovy-", 0).factory());
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("flow-groovy-watchdog").factory());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong invocationSequence = new AtomicLong();
    private final Map<Long, InvocationControl> activeInvocations = new ConcurrentHashMap<>();
    private final Object cacheMonitor = new Object();
    private final LinkedHashMap<String, CachedScript> cache = new LinkedHashMap<>(16, 0.75f, true);

    /** Creates the production runtime with finite script, result, timeout and cache limits. */
    public SandboxedGroovyRuntime() {
        this(DEFAULT_TIMEOUT, DEFAULT_MAX_SCRIPT_LENGTH, DEFAULT_MAX_RESULT_BYTES,
                DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_TTL, Clock.systemUTC(),
                DEFAULT_MAX_CONCURRENT_INVOCATIONS, DEFAULT_MAX_LOG_LINE_BYTES,
                DEFAULT_MAX_LOG_TOTAL_BYTES, DEFAULT_MAX_LOG_ENTRIES);
    }

    /** Package-local configuration seam for deterministic boundary tests. */
    SandboxedGroovyRuntime(Duration timeout, int maxScriptLength, int maxResultBytes,
            int cacheCapacity, Duration cacheTtl, Clock clock) {
        this(timeout, maxScriptLength, maxResultBytes, cacheCapacity, cacheTtl, clock,
                DEFAULT_MAX_CONCURRENT_INVOCATIONS, DEFAULT_MAX_LOG_LINE_BYTES,
                DEFAULT_MAX_LOG_TOTAL_BYTES, DEFAULT_MAX_LOG_ENTRIES);
    }

    /** Package-local full limit seam for deterministic resource-boundary tests. */
    SandboxedGroovyRuntime(Duration timeout, int maxScriptLength, int maxResultBytes,
            int cacheCapacity, Duration cacheTtl, Clock clock, int maxConcurrentInvocations,
            int maxLogLineBytes, int maxLogTotalBytes, int maxLogEntries) {
        this.timeout = positive(timeout, "script timeout");
        this.maxScriptLength = positive(maxScriptLength, "maxScriptLength");
        this.maxResultBytes = positive(maxResultBytes, "maxResultBytes");
        this.cacheCapacity = positive(cacheCapacity, "cacheCapacity");
        this.cacheTtl = positive(cacheTtl, "cacheTtl");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.invocationAdmission = new Semaphore(
                positive(maxConcurrentInvocations, "maxConcurrentInvocations"), true);
        this.maxLogLineBytes = positive(maxLogLineBytes, "maxLogLineBytes");
        this.maxLogTotalBytes = positive(maxLogTotalBytes, "maxLogTotalBytes");
        this.maxLogEntries = positive(maxLogEntries, "maxLogEntries");
        watchdog.scheduleAtFixedRate(this::inspectActiveInvocations,
                STOP_POLL_MILLIS, STOP_POLL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Compiles by SHA-256 digest and runs one script under the invocation deadline and cancellation token.
     *
     * @param invocation immutable copied script inputs and cooperative controls
     * @return completion containing only a normalized JSON-compatible value
     */
    @Override
    public CompletionStage<Object> execute(ScriptInvocation invocation) {
        Objects.requireNonNull(invocation, "script invocation must not be null");
        if (closed.get()) {
            return failed("SCRIPT_RUNTIME_CLOSED", "script runtime is closed");
        }
        if (invocation.script().length() > maxScriptLength) {
            return failed("SCRIPT_TOO_LARGE", "script exceeds the configured size limit");
        }
        if (invocation.cancellationToken().isCancellationRequested()) {
            return CompletableFuture.failedFuture(new CancellationException("script execution was cancelled"));
        }
        Instant startedAt = clock.instant();
        Instant timeoutAt = earlier(invocation.deadline(), startedAt.plus(timeout));
        if (!startedAt.isBefore(timeoutAt)) {
            return failed("SCRIPT_TIMEOUT", "script execution timed out");
        }
        if (!invocationAdmission.tryAcquire()) {
            return failed("SCRIPT_CAPACITY_EXCEEDED", "script runtime capacity is exhausted");
        }
        if (closed.get()) {
            invocationAdmission.release();
            return failed("SCRIPT_RUNTIME_CLOSED", "script runtime is closed");
        }
        long invocationId = invocationSequence.incrementAndGet();
        InvocationControl control = new InvocationControl(invocationId, invocation, timeoutAt);
        activeInvocations.put(invocationId, control);
        try {
            control.worker(tasks.submit(() -> run(control)));
        } catch (RuntimeException failure) {
            control.runtimeClosed();
            control.finishBeforeStart();
        }
        return control.completion();
    }

    private void run(InvocationControl control) {
        if (!control.startWorker()) {
            return;
        }
        ScriptInvocation invocation = control.invocation();
        LoggingWriter output = null;
        try {
            control.throwIfStopped();
            CachedScript compiled = compiled(invocation.script(), clock.instant());
            control.throwIfStopped();
            output = new LoggingWriter(invocation.logger(), maxLogLineBytes,
                    maxLogTotalBytes, maxLogEntries);
            Map<String, Object> variables = new LinkedHashMap<>(invocation.externalScopes());
            variables.put("ctx", invocation.ctx());
            Script script = InvokerHelper.createScript(compiled.scriptClass(), new Binding(variables));
            script.setProperty("out", new PrintWriter(output, true));
            Object normalized = normalize(script.run(), 0, new ResultCounter());
            if (JSON.toJSONBytes(normalized).length > maxResultBytes) {
                throw new ScriptExecutionException(
                        "SCRIPT_RESULT_TOO_LARGE", "script result exceeds the configured size limit");
            }
            control.succeed(normalized);
        } catch (SecurityException failure) {
            control.fail(new ScriptExecutionException(
                    "SCRIPT_SECURITY_VIOLATION", "script violates the sandbox policy"));
        } catch (MultipleCompilationErrorsException failure) {
            control.fail(isSecurityFailure(failure)
                    ? new ScriptExecutionException(
                            "SCRIPT_SECURITY_VIOLATION", "script violates the sandbox policy")
                    : new ScriptExecutionException("SCRIPT_COMPILE_FAILED", "script compilation failed"));
        } catch (CancellationException failure) {
            control.cancelFromWorker();
        } catch (ScriptExecutionException failure) {
            control.fail(failure);
        } catch (Throwable failure) {
            if (Thread.currentThread().isInterrupted()
                    || invocation.cancellationToken().isCancellationRequested()) {
                control.cancelFromWorker();
            } else {
                control.fail(new ScriptExecutionException(
                        "SCRIPT_RUNTIME_FAILED", "script execution failed"));
            }
        } finally {
            if (output != null) {
                output.close();
            }
            control.finishFromWorker();
        }
    }

    /** One runtime-wide watchdog replaces one polling monitor task per invocation. */
    private void inspectActiveInvocations() {
        try {
            activeInvocations.values().forEach(InvocationControl::inspect);
        } catch (RuntimeException ignored) {
            // A single malformed invocation must not terminate the shared cancellation watchdog.
        }
    }

    private CachedScript compiled(String source, Instant now) {
        rejectCompileTimeAnnotations(source);
        String digest = digest(source);
        synchronized (cacheMonitor) {
            evictExpired(now);
            CachedScript existing = cache.get(digest);
            if (existing != null) {
                return existing;
            }
        }
        CachedScript created = compile(source, digest, now.plus(cacheTtl));
        synchronized (cacheMonitor) {
            if (closed.get()) {
                created.close();
                throw new IllegalStateException("script runtime is closed");
            }
            evictExpired(now);
            CachedScript raced = cache.get(digest);
            if (raced != null) {
                created.close();
                return raced;
            }
            cache.put(digest, created);
            while (cache.size() > cacheCapacity) {
                String eldest = cache.keySet().iterator().next();
                cache.remove(eldest).close();
            }
            return created;
        }
    }

    /**
     * Rejects annotation tokens before Groovy can load a local AST transform such as ASTTest/Grab.
     * The lexer distinguishes code from string/comment contents, so ordinary JSON text containing
     * an at-sign remains valid.
     */
    private static void rejectCompileTimeAnnotations(String source) {
        GroovyLexer lexer = new GroovyLexer(CharStreams.fromString(source));
        lexer.removeErrorListeners();
        for (Token token = lexer.nextToken(); token.getType() != Token.EOF; token = lexer.nextToken()) {
            if (token.getType() == GroovyLexer.AT) {
                throw new SecurityException("annotations are not allowed by the script sandbox");
            }
        }
    }

    private static CachedScript compile(String source, String digest, Instant expiresAt) {
        CompilerConfiguration configuration = new CompilerConfiguration();
        configuration.addCompilationCustomizers(
                new ASTTransformationCustomizer(ThreadInterrupt.class),
                secureCustomizer(),
                new RuntimeSecurityCustomizer(),
                new JsonPropertyAccessCustomizer());
        GroovyClassLoader classLoader = new GroovyClassLoader(
                SandboxedGroovyRuntime.class.getClassLoader(), configuration);
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Script> scriptClass = (Class<? extends Script>) classLoader.parseClass(
                    source, "FlowScript_" + digest + ".groovy");
            return new CachedScript(classLoader, scriptClass, expiresAt);
        } catch (RuntimeException failure) {
            try {
                classLoader.close();
            } catch (Exception ignored) {
                // Closing a rejected compiler cannot make its safe diagnostic less useful.
            }
            throw failure;
        }
    }

    private static SecureASTCustomizer secureCustomizer() {
        SecureASTCustomizer customizer = new SecureASTCustomizer();
        customizer.setClosuresAllowed(false);
        customizer.setMethodDefinitionAllowed(false);
        customizer.setPackageAllowed(false);
        customizer.setAllowedImports(List.of());
        customizer.setAllowedStarImports(List.of());
        customizer.setAllowedStaticImports(List.of());
        customizer.setAllowedStaticStarImports(List.of());
        customizer.setIndirectImportCheckEnabled(false);
        return customizer;
    }

    /**
     * Executes a source property read as an exact JSON object-key lookup.
     *
     * <p>The compiler rewrites admitted property expressions to this method so Groovy cannot turn
     * a data-looking access into a Java/GDK getter call on a String, number, list or boolean.</p>
     *
     * @param receiver runtime receiver produced by the immutable JSON binding
     * @param key statically compiled property name
     * @param safe whether the source used Groovy safe-navigation
     * @return mapped JSON value, including null for an absent key
     * @throws SecurityException when the runtime receiver is not a JSON object
     */
    public static Object readJsonProperty(Object receiver, String key, boolean safe) {
        if (receiver == null && safe) {
            return null;
        }
        if (!(receiver instanceof Map<?, ?> map)) {
            throw new SecurityException("script sandbox property receiver must be a JSON object");
        }
        return map.get(key);
    }

    private void evictExpired(Instant now) {
        List<String> expired = cache.entrySet().stream()
                .filter(entry -> !now.isBefore(entry.getValue().expiresAt()))
                .map(Map.Entry::getKey).toList();
        expired.forEach(key -> cache.remove(key).close());
    }

    private static Object normalize(Object value, int depth, ResultCounter counter) {
        if (depth > MAX_RESULT_DEPTH || ++counter.value > MAX_RESULT_NODES) {
            throw new ScriptExecutionException(
                    "SCRIPT_RESULT_TOO_LARGE", "script result exceeds the configured structure limit");
        }
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Number) {
            return JsonValues.copyValue(value, "script result");
        }
        if (value instanceof Character || value instanceof CharSequence) {
            return value.toString();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, child) -> {
                if (!(key instanceof String name)) {
                    throw new ScriptExecutionException(
                            "SCRIPT_RESULT_NOT_JSON", "script result object keys must be strings");
                }
                result.put(name, normalize(child, depth + 1, counter));
            });
            return Collections.unmodifiableMap(result);
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> result = new ArrayList<>();
            iterable.forEach(child -> result.add(normalize(child, depth + 1, counter)));
            return Collections.unmodifiableList(result);
        }
        if (value.getClass().isArray()) {
            List<Object> result = new ArrayList<>();
            for (int index = 0; index < Array.getLength(value); index++) {
                result.add(normalize(Array.get(value, index), depth + 1, counter));
            }
            return Collections.unmodifiableList(result);
        }
        throw new ScriptExecutionException("SCRIPT_RESULT_NOT_JSON", "script result must be JSON-compatible");
    }

    /** @return current cache size without exposing sources; package-local for runtime metrics/tests */
    int cacheSize() {
        synchronized (cacheMonitor) {
            evictExpired(clock.instant());
            return cache.size();
        }
    }

    /** @return immutable SHA-256 cache keys only; package-local for runtime metrics/tests */
    Set<String> cachedDigests() {
        synchronized (cacheMonitor) {
            evictExpired(clock.instant());
            return Collections.unmodifiableSet(new LinkedHashSet<>(cache.keySet()));
        }
    }

    /** @return workers still compiling or executing; package-local for lifecycle tests/metrics */
    int activeInvocationCount() {
        return activeInvocations.size();
    }

    /** Interrupts owned tasks and releases every cached class loader. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        activeInvocations.values().forEach(InvocationControl::closeFromRuntime);
        watchdog.shutdownNow();
        tasks.shutdownNow();
        synchronized (cacheMonitor) {
            cache.values().forEach(CachedScript::close);
            cache.clear();
        }
    }

    private static CompletableFuture<Object> failed(String code, String message) {
        return CompletableFuture.failedFuture(new ScriptExecutionException(code, message));
    }

    private static String digest(String source) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static boolean isSecurityFailure(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof SecurityException) {
                return true;
            }
            String message = current.getMessage();
            if (message != null && (message.contains("script sandbox")
                    || message.contains("not allowed")
                    || message.contains("not allowed by the script sandbox"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Instant earlier(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private record CachedScript(GroovyClassLoader classLoader,
                                Class<? extends Script> scriptClass,
                                Instant expiresAt) implements AutoCloseable {

        @Override
        public void close() {
            try {
                classLoader.close();
            } catch (Exception ignored) {
                // Eviction must remain bounded even when a loader cannot release an optional resource.
            }
        }
    }

    /** Binds one caller completion, cooperative token and watchdog deadline to the actual worker. */
    private final class InvocationControl {

        private final long invocationId;
        private final ScriptInvocation invocation;
        private final Instant timeoutAt;
        private final InvocationFuture completion = new InvocationFuture(this);
        private final AtomicReference<Future<?>> worker = new AtomicReference<>();
        private final AtomicBoolean stopRequested = new AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicInteger lifecycle =
                new java.util.concurrent.atomic.AtomicInteger();

        private InvocationControl(long invocationId, ScriptInvocation invocation, Instant timeoutAt) {
            this.invocationId = invocationId;
            this.invocation = invocation;
            this.timeoutAt = timeoutAt;
        }

        private long invocationId() {
            return invocationId;
        }

        private ScriptInvocation invocation() {
            return invocation;
        }

        private InvocationFuture completion() {
            return completion;
        }

        private void worker(Future<?> submitted) {
            if (!worker.compareAndSet(null, submitted)) {
                submitted.cancel(true);
                throw new IllegalStateException("script worker was already assigned");
            }
            if (stopRequested.get()) {
                cancelSubmitted(submitted);
            }
        }

        private boolean startWorker() {
            return lifecycle.compareAndSet(0, 1);
        }

        private void finishBeforeStart() {
            if (lifecycle.compareAndSet(0, 2)) {
                releaseInvocation();
            }
        }

        private void finishFromWorker() {
            if (lifecycle.compareAndSet(1, 2)) {
                releaseInvocation();
            }
        }

        private void releaseInvocation() {
            activeInvocations.remove(invocationId, this);
            invocationAdmission.release();
        }

        private void inspect() {
            if (invocation.cancellationToken().isCancellationRequested() || closed.get()) {
                cancelAndStop();
            } else if (!clock.instant().isBefore(timeoutAt)) {
                timeoutAndStop();
            }
        }

        private void throwIfStopped() {
            if (invocation.cancellationToken().isCancellationRequested() || closed.get()
                    || completion.isCancelled()) {
                throw new CancellationException("script execution was cancelled");
            }
            if (!clock.instant().isBefore(timeoutAt)) {
                throw new ScriptExecutionException("SCRIPT_TIMEOUT", "script execution timed out");
            }
        }

        private void succeed(Object value) {
            completion.succeed(value);
        }

        private void fail(Throwable failure) {
            completion.fail(failure);
        }

        private void cancelFromWorker() {
            completion.cancelFromRuntime();
        }

        private void runtimeClosed() {
            completion.fail(new ScriptExecutionException("SCRIPT_RUNTIME_CLOSED", "script runtime is closed"));
        }

        private void closeFromRuntime() {
            cancelAndStop();
        }

        private void cancelAndStop() {
            completion.cancelFromRuntime();
            stopWorker();
        }

        private void timeoutAndStop() {
            completion.fail(new ScriptExecutionException("SCRIPT_TIMEOUT", "script execution timed out"));
            stopWorker();
        }

        private void stopWorker() {
            stopRequested.set(true);
            Future<?> running = worker.get();
            if (running != null) {
                cancelSubmitted(running);
            }
        }

        private void cancelSubmitted(Future<?> running) {
            running.cancel(true);
            finishBeforeStart();
        }
    }

    /** Caller cancellation is write-through; ordinary caller completion cannot forge a result. */
    private static final class InvocationFuture extends CompletableFuture<Object> {

        private final InvocationControl control;

        private InvocationFuture(InvocationControl control) {
            this.control = control;
        }

        private boolean succeed(Object value) {
            return super.complete(value);
        }

        private boolean fail(Throwable failure) {
            return super.completeExceptionally(failure);
        }

        private boolean cancelFromRuntime() {
            return super.cancel(true);
        }

        @Override
        public boolean complete(Object value) {
            return false;
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return false;
        }

        @Override
        public void obtrudeValue(Object value) {
            throw new UnsupportedOperationException("script completion is read-only");
        }

        @Override
        public void obtrudeException(Throwable failure) {
            throw new UnsupportedOperationException("script completion is read-only");
        }

        @Override
        public CompletableFuture<Object> completeAsync(
                java.util.function.Supplier<? extends Object> supplier) {
            throw new UnsupportedOperationException("script completion is read-only");
        }

        @Override
        public CompletableFuture<Object> completeAsync(
                java.util.function.Supplier<? extends Object> supplier,
                java.util.concurrent.Executor executor) {
            throw new UnsupportedOperationException("script completion is read-only");
        }

        @Override
        public CompletableFuture<Object> completeOnTimeout(Object value, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("script completion timeout is runtime-owned");
        }

        @Override
        public CompletableFuture<Object> orTimeout(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("script completion timeout is runtime-owned");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                control.stopWorker();
            }
            return cancelled;
        }
    }

    private static final class ResultCounter {
        private int value;
    }

    /** Line-buffered adapter which cannot leak script output to the server console. */
    private static final class LoggingWriter extends Writer {

        private static final String TRUNCATION_MARKER = "[...]";

        private final ExecutionLogger logger;
        private final int maxLineBytes;
        private final int maxTotalBytes;
        private final int maxEntries;
        private final StringBuilder line = new StringBuilder();
        private int totalBytes;
        private int entries;
        private boolean lineTruncated;
        private boolean closed;

        private LoggingWriter(ExecutionLogger logger, int maxLineBytes, int maxTotalBytes, int maxEntries) {
            this.logger = logger;
            this.maxLineBytes = maxLineBytes;
            this.maxTotalBytes = maxTotalBytes;
            this.maxEntries = maxEntries;
        }

        @Override
        public synchronized void write(char[] buffer, int offset, int length) {
            if (closed) {
                return;
            }
            for (int index = offset; index < offset + length; index++) {
                char character = buffer[index];
                if (character == '\n') {
                    emit();
                } else if (character != '\r') {
                    if (line.length() < maxLineBytes) {
                        line.append(character);
                    } else {
                        lineTruncated = true;
                    }
                }
            }
        }

        @Override
        public synchronized void flush() {
            // A bare print remains buffered until a newline or script completion so fragmented Writer
            // calls form one useful structured log entry.
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                emit();
                closed = true;
            }
        }

        private void emit() {
            if (line.isEmpty() && !lineTruncated) {
                return;
            }
            if (entries >= maxEntries || totalBytes >= maxTotalBytes) {
                resetLine();
                return;
            }
            int availableBytes = Math.min(maxLineBytes, maxTotalBytes - totalBytes);
            String message = boundedMessage(line.toString(), availableBytes, lineTruncated);
            int messageBytes = message.getBytes(StandardCharsets.UTF_8).length;
            totalBytes += messageBytes;
            entries++;
            resetLine();
            if (message.isEmpty()) {
                return;
            }
            try {
                logger.info(message, null, Map.of());
            } catch (RuntimeException ignored) {
                // Observation loss must not change an ordinary EPHEMERAL business result.
            }
        }

        private void resetLine() {
            line.setLength(0);
            lineTruncated = false;
        }

        private static String boundedMessage(String value, int byteLimit, boolean alreadyTruncated) {
            if (byteLimit < 1) {
                return "";
            }
            int originalBytes = value.getBytes(StandardCharsets.UTF_8).length;
            if (!alreadyTruncated && originalBytes <= byteLimit) {
                return value;
            }
            int markerBytes = TRUNCATION_MARKER.getBytes(StandardCharsets.UTF_8).length;
            if (byteLimit <= markerBytes) {
                return utf8Prefix(TRUNCATION_MARKER, byteLimit);
            }
            return utf8Prefix(value, byteLimit - markerBytes) + TRUNCATION_MARKER;
        }

        private static String utf8Prefix(String value, int byteLimit) {
            StringBuilder prefix = new StringBuilder();
            int used = 0;
            for (int offset = 0; offset < value.length();) {
                int codePoint = value.codePointAt(offset);
                String text = new String(Character.toChars(codePoint));
                int bytes = text.getBytes(StandardCharsets.UTF_8).length;
                if (used + bytes > byteLimit) {
                    break;
                }
                prefix.append(text);
                used += bytes;
                offset += Character.charCount(codePoint);
            }
            return prefix.toString();
        }
    }

    /** Rewrites approved data properties before bytecode generation to remove getter dispatch. */
    private static final class JsonPropertyAccessCustomizer extends CompilationCustomizer {

        private JsonPropertyAccessCustomizer() {
            super(CompilePhase.CANONICALIZATION);
        }

        @Override
        public void call(SourceUnit source, GeneratorContext context, ClassNode classNode) {
            if (!classNode.isScript()) {
                return;
            }
            var run = classNode.getMethod("run", org.codehaus.groovy.ast.Parameter.EMPTY_ARRAY);
            if (run == null || run.getCode() == null) {
                return;
            }
            run.getCode().visit(new ClassCodeExpressionTransformer() {
                @Override
                protected SourceUnit getSourceUnit() {
                    return source;
                }

                @Override
                public Expression transform(Expression expression) {
                    if (!(expression instanceof PropertyExpression property)
                            || property instanceof AttributeExpression) {
                        return super.transform(expression);
                    }
                    String name = property.getPropertyAsString();
                    if (name == null) {
                        throw new SecurityException("script sandbox property name must be static");
                    }
                    StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                            ClassHelper.make(SandboxedGroovyRuntime.class),
                            "readJsonProperty",
                            new ArgumentListExpression(
                                    transform(property.getObjectExpression()),
                                    new ConstantExpression(name),
                                    new ConstantExpression(property.isSafe())));
                    rewritten.setSourcePosition(property);
                    return rewritten;
                }
            });
        }
    }

    /** Semantic-phase allowlist which rejects every statement/expression shape not named here. */
    private static final class RuntimeSecurityCustomizer extends CompilationCustomizer {

        private RuntimeSecurityCustomizer() {
            super(CompilePhase.SEMANTIC_ANALYSIS);
        }

        @Override
        public void call(SourceUnit source, GeneratorContext context, ClassNode classNode) {
            if (!classNode.isScript()) {
                reject(classNode, "class declarations are outside the allowed AST");
            }
            var run = classNode.getMethod("run", org.codehaus.groovy.ast.Parameter.EMPTY_ARRAY);
            if (run == null || run.getCode() == null) {
                return;
            }
            new AllowedScriptAst().validateStatement(run.getCode());
        }

        /** Recursive closed-world validator; adding a Groovy AST type requires an explicit review. */
        private static final class AllowedScriptAst {

            private final Set<String> localVariables = new HashSet<>();

            private void validateStatement(Statement statement) {
                if (statement instanceof BlockStatement block) {
                    block.getStatements().forEach(this::validateStatement);
                    return;
                }
                if (statement instanceof ExpressionStatement expression) {
                    validateExpression(expression.getExpression());
                    return;
                }
                if (statement instanceof ReturnStatement returned) {
                    validateExpression(returned.getExpression());
                    return;
                }
                if (statement instanceof IfStatement conditional) {
                    validateExpression(conditional.getBooleanExpression());
                    validateStatement(conditional.getIfBlock());
                    validateStatement(conditional.getElseBlock());
                    return;
                }
                if (statement instanceof WhileStatement loop) {
                    validateExpression(loop.getBooleanExpression());
                    validateStatement(loop.getLoopBlock());
                    return;
                }
                if (statement instanceof BreakStatement
                        || statement instanceof ContinueStatement
                        || statement instanceof EmptyStatement) {
                    return;
                }
                reject(statement, "statement type is outside the allowed AST");
            }

            private void validateExpression(Expression expression) {
                if (expression == null || expression instanceof EmptyExpression) {
                    return;
                }
                if (expression instanceof ConstantExpression constant) {
                    Object value = constant.getValue();
                    if (value != null && !(value instanceof String) && !(value instanceof Boolean)
                            && !(value instanceof Number)) {
                        reject(expression, "constant type is outside the JSON allowlist");
                    }
                    return;
                }
                if (expression instanceof VariableExpression variable) {
                    validateVariable(variable);
                    return;
                }
                if (expression instanceof DeclarationExpression declaration) {
                    if (declaration.isMultipleAssignmentDeclaration()) {
                        reject(declaration, "only one local variable may be declared at a time");
                    }
                    if (!(declaration.getLeftExpression() instanceof VariableExpression variable)) {
                        reject(declaration, "local declaration must target one variable");
                        return;
                    }
                    validateVariable(variable);
                    validateExpression(declaration.getRightExpression());
                    if (!localVariables.add(variable.getName())) {
                        reject(declaration, "local variables may only be declared once");
                    }
                    return;
                }
                if (expression instanceof BinaryExpression binary) {
                    validateBinary(binary);
                    return;
                }
                if (expression instanceof BooleanExpression bool) {
                    validateExpression(bool.getExpression());
                    return;
                }
                if (expression instanceof NotExpression not) {
                    validateExpression(not.getExpression());
                    return;
                }
                if (expression instanceof UnaryMinusExpression unary) {
                    validateExpression(unary.getExpression());
                    return;
                }
                if (expression instanceof UnaryPlusExpression unary) {
                    validateExpression(unary.getExpression());
                    return;
                }
                if (expression instanceof ElvisOperatorExpression elvis) {
                    validateTernary(elvis);
                    return;
                }
                if (expression instanceof TernaryExpression ternary) {
                    validateTernary(ternary);
                    return;
                }
                if (expression instanceof ListExpression list) {
                    list.getExpressions().forEach(this::validateExpression);
                    return;
                }
                if (expression instanceof MapExpression map) {
                    map.getMapEntryExpressions().forEach(this::validateExpression);
                    return;
                }
                if (expression instanceof MapEntryExpression entry) {
                    validateExpression(entry.getKeyExpression());
                    validateExpression(entry.getValueExpression());
                    return;
                }
                if (expression instanceof GStringExpression interpolation) {
                    interpolation.getValues().forEach(this::validateExpression);
                    return;
                }
                if (expression instanceof PropertyExpression property) {
                    validateProperty(property);
                    return;
                }
                if (expression instanceof MethodCallExpression methodCall) {
                    validateMethodCall(methodCall);
                    return;
                }
                if (expression instanceof ArgumentListExpression arguments) {
                    arguments.getExpressions().forEach(this::validateExpression);
                    return;
                }
                if (expression instanceof TupleExpression tuple) {
                    tuple.getExpressions().forEach(this::validateExpression);
                    return;
                }
                reject(expression, "expression type is outside the allowed AST");
            }

            private void validateVariable(VariableExpression variable) {
                if (RESERVED_VARIABLES.contains(variable.getName()) || variable.isThisExpression()
                        || variable.isSuperExpression()) {
                    reject(variable, "host script variables are outside the receiver allowlist");
                }
                if (!variable.isDynamicTyped()) {
                    reject(variable, "typed variables are outside the allowed AST");
                }
            }

            private void validateBinary(BinaryExpression binary) {
                int token = binary.getOperation().getType();
                if (!ALLOWED_BINARY_TOKENS.contains(token)) {
                    reject(binary, "operator is outside the token allowlist");
                }
                if (token == Types.ASSIGN) {
                    if (!(binary.getLeftExpression() instanceof VariableExpression variable)
                            || !localVariables.contains(variable.getName())) {
                        reject(binary, "assignment targets must be declared local variables");
                    }
                }
                validateExpression(binary.getLeftExpression());
                validateExpression(binary.getRightExpression());
            }

            private void validateTernary(TernaryExpression ternary) {
                validateExpression(ternary.getBooleanExpression());
                validateExpression(ternary.getTrueExpression());
                validateExpression(ternary.getFalseExpression());
            }

            private void validateProperty(PropertyExpression property) {
                String name = property.getPropertyAsString();
                if (property instanceof AttributeExpression || name == null || property.isSpreadSafe()
                        || RESERVED_PROPERTIES.contains(name)) {
                    reject(property, "property is outside the JSON receiver allowlist");
                }
                if (!isExternalJsonPath(property.getObjectExpression())) {
                    reject(property, "property receiver must originate from an external JSON scope");
                }
                validateExpression(property.getObjectExpression());
            }

            private boolean isExternalJsonPath(Expression expression) {
                if (expression instanceof VariableExpression variable) {
                    return !localVariables.contains(variable.getName())
                            && !RESERVED_VARIABLES.contains(variable.getName())
                            && !variable.isThisExpression()
                            && !variable.isSuperExpression();
                }
                if (expression instanceof PropertyExpression property) {
                    String name = property.getPropertyAsString();
                    return !(property instanceof AttributeExpression)
                            && name != null
                            && !property.isSpreadSafe()
                            && !RESERVED_PROPERTIES.contains(name)
                            && isExternalJsonPath(property.getObjectExpression());
                }
                if (expression instanceof BinaryExpression binary) {
                    return binary.getOperation().getType() == Types.LEFT_SQUARE_BRACKET
                            && isExternalJsonPath(binary.getLeftExpression());
                }
                if (expression instanceof MethodCallExpression call) {
                    return !call.isImplicitThis()
                            && !call.isSpreadSafe()
                            && "get".equals(call.getMethodAsString())
                            && isExternalJsonPath(call.getObjectExpression());
                }
                return false;
            }

            private void validateMethodCall(MethodCallExpression call) {
                String method = call.getMethodAsString();
                if (method == null || call.isSpreadSafe()) {
                    reject(call, "method dispatch is outside the method allowlist");
                }
                if (call.isImplicitThis()) {
                    if (!LOG_METHODS.contains(method)) {
                        reject(call, "implicit host dispatch is outside the method allowlist");
                    }
                } else {
                    if (!ALLOWED_METHODS.contains(method)) {
                        reject(call, "method is outside the method allowlist");
                    }
                    validateReceiver(call.getObjectExpression());
                }
                validateExpression(call.getArguments());
            }

            private void validateReceiver(Expression receiver) {
                if (!(receiver instanceof VariableExpression)
                        && !(receiver instanceof ConstantExpression)
                        && !(receiver instanceof PropertyExpression)
                        && !(receiver instanceof MethodCallExpression)
                        && !(receiver instanceof BinaryExpression)
                        && !(receiver instanceof ListExpression)
                        && !(receiver instanceof MapExpression)
                        && !(receiver instanceof GStringExpression)
                        && !(receiver instanceof TernaryExpression)
                        && !(receiver instanceof UnaryMinusExpression)
                        && !(receiver instanceof UnaryPlusExpression)) {
                    reject(receiver, "receiver shape is outside the JSON receiver allowlist");
                }
                validateExpression(receiver);
            }
        }

        private static void reject(ASTNode node, String message) {
            throw new SecurityException(
                    "script sandbox rejected " + message + " at line " + Math.max(1, node.getLineNumber()));
        }
    }
}
