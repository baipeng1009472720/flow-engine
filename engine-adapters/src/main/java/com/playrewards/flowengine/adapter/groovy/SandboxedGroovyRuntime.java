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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.groovy.parser.antlr4.GroovyLexer;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.CodeVisitorSupport;
import org.codehaus.groovy.ast.expr.CastExpression;
import org.codehaus.groovy.ast.expr.ClassExpression;
import org.codehaus.groovy.ast.expr.ConstructorCallExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.MethodPointerExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
import org.codehaus.groovy.ast.expr.StaticMethodCallExpression;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.codehaus.groovy.runtime.InvokerHelper;

/**
 * Bounded Groovy adapter which compiles by source digest and executes only copied JSON bindings.
 *
 * <p>Compilation rejects imports, explicit construction, reflection/class access, process helpers,
 * threads, file APIs and network APIs. Runtime interruption, deadline checks, finite cache/resource
 * limits and redacted failures form a second boundary; this class does not provide an in-process
 * security-manager replacement for arbitrary trusted JVM bytecode.</p>
 */
public final class SandboxedGroovyRuntime implements ScriptRuntime, AutoCloseable {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration DEFAULT_CACHE_TTL = Duration.ofMinutes(10);
    private static final int DEFAULT_MAX_SCRIPT_LENGTH = 50_000;
    private static final int DEFAULT_MAX_RESULT_BYTES = 1_048_576;
    private static final int DEFAULT_CACHE_CAPACITY = 256;
    private static final int MAX_RESULT_DEPTH = 32;
    private static final int MAX_RESULT_NODES = 10_000;
    private static final long STOP_POLL_MILLIS = 10;
    private static final Set<String> FORBIDDEN_METHODS = Set.of(
            "execute", "getClass", "getMetaClass", "setMetaClass", "invokeMethod",
            "evaluate", "getProperty", "setProperty", "getProperties", "getBinding", "setBinding",
            "forName", "loadClass", "defineClass", "getClassLoader", "newInstance",
            "getDeclaredField", "getDeclaredFields", "getDeclaredMethod", "getDeclaredMethods",
            "getField", "getFields", "getMethod", "getMethods", "setAccessible",
            "getRuntime", "exit", "halt", "start", "startVirtualThread",
            "toURL", "toURI", "sleep", "wait", "notify", "notifyAll");
    private static final Set<String> FORBIDDEN_PROPERTIES = Set.of(
            "class", "metaClass", "classLoader", "declaredFields", "declaredMethods",
            "fields", "methods", "protectionDomain", "binding", "properties");
    private static final List<String> FORBIDDEN_TYPE_PREFIXES = List.of(
            "java.io.", "java.net.", "java.nio.file.", "java.lang.reflect.",
            "java.lang.invoke.", "java.util.concurrent.", "groovy.lang.GroovyClassLoader");
    private static final Set<String> FORBIDDEN_TYPES = Set.of(
            "java.lang.Class", "java.lang.ClassLoader", "java.lang.ProcessBuilder",
            "java.lang.Runtime", "java.lang.System", "java.lang.Thread");

    private final Duration timeout;
    private final int maxScriptLength;
    private final int maxResultBytes;
    private final int cacheCapacity;
    private final Duration cacheTtl;
    private final Clock clock;
    private final ExecutorService tasks = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("flow-groovy-", 0).factory());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object cacheMonitor = new Object();
    private final LinkedHashMap<String, CachedScript> cache = new LinkedHashMap<>(16, 0.75f, true);

    /** Creates the production runtime with finite script, result, timeout and cache limits. */
    public SandboxedGroovyRuntime() {
        this(DEFAULT_TIMEOUT, DEFAULT_MAX_SCRIPT_LENGTH, DEFAULT_MAX_RESULT_BYTES,
                DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_TTL, Clock.systemUTC());
    }

    /** Package-local configuration seam for deterministic boundary tests. */
    SandboxedGroovyRuntime(Duration timeout, int maxScriptLength, int maxResultBytes,
            int cacheCapacity, Duration cacheTtl, Clock clock) {
        this.timeout = positive(timeout, "script timeout");
        this.maxScriptLength = positive(maxScriptLength, "maxScriptLength");
        this.maxResultBytes = positive(maxResultBytes, "maxResultBytes");
        this.cacheCapacity = positive(cacheCapacity, "cacheCapacity");
        this.cacheTtl = positive(cacheTtl, "cacheTtl");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
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
        CachedScript compiled;
        try {
            compiled = compiled(invocation.script(), startedAt);
        } catch (SecurityException failure) {
            return failed("SCRIPT_SECURITY_VIOLATION", "script violates the sandbox policy");
        } catch (MultipleCompilationErrorsException failure) {
            return isSecurityFailure(failure)
                    ? failed("SCRIPT_SECURITY_VIOLATION", "script violates the sandbox policy")
                    : failed("SCRIPT_COMPILE_FAILED", "script compilation failed");
        } catch (RuntimeException failure) {
            return failed("SCRIPT_COMPILE_FAILED", "script compilation failed");
        }

        CompletableFuture<Object> completion = new CompletableFuture<>();
        Future<?> worker;
        try {
            worker = tasks.submit(() -> run(compiled, invocation, completion));
            tasks.submit(() -> monitor(worker, invocation, timeoutAt, completion));
        } catch (RuntimeException failure) {
            return failed("SCRIPT_RUNTIME_CLOSED", "script runtime is closed");
        }
        return completion;
    }

    private void run(CachedScript compiled, ScriptInvocation invocation,
            CompletableFuture<Object> completion) {
        LoggingWriter output = new LoggingWriter(invocation.logger());
        try {
            Map<String, Object> variables = new LinkedHashMap<>(invocation.externalScopes());
            variables.put("ctx", invocation.ctx());
            Script script = InvokerHelper.createScript(compiled.scriptClass(), new Binding(variables));
            script.setProperty("out", new PrintWriter(output, true));
            Object normalized = normalize(script.run(), 0, new ResultCounter());
            if (JSON.toJSONBytes(normalized).length > maxResultBytes) {
                throw new ScriptExecutionException(
                        "SCRIPT_RESULT_TOO_LARGE", "script result exceeds the configured size limit");
            }
            completion.complete(normalized);
        } catch (CancellationException failure) {
            completion.completeExceptionally(failure);
        } catch (ScriptExecutionException failure) {
            completion.completeExceptionally(failure);
        } catch (Throwable failure) {
            if (Thread.currentThread().isInterrupted()
                    || invocation.cancellationToken().isCancellationRequested()) {
                completion.completeExceptionally(new CancellationException("script execution was cancelled"));
            } else {
                completion.completeExceptionally(new ScriptExecutionException(
                        "SCRIPT_RUNTIME_FAILED", "script execution failed"));
            }
        } finally {
            output.close();
        }
    }

    private void monitor(Future<?> worker, ScriptInvocation invocation, Instant timeoutAt,
            CompletableFuture<Object> completion) {
        try {
            while (!completion.isDone()) {
                if (invocation.cancellationToken().isCancellationRequested() || closed.get()) {
                    completion.completeExceptionally(new CancellationException("script execution was cancelled"));
                    worker.cancel(true);
                    return;
                }
                if (!clock.instant().isBefore(timeoutAt)) {
                    completion.completeExceptionally(new ScriptExecutionException(
                            "SCRIPT_TIMEOUT", "script execution timed out"));
                    worker.cancel(true);
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(STOP_POLL_MILLIS);
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            worker.cancel(true);
            completion.completeExceptionally(new CancellationException("script execution was cancelled"));
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
                new RuntimeSecurityCustomizer());
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
        customizer.setClosuresAllowed(true);
        customizer.setMethodDefinitionAllowed(false);
        customizer.setPackageAllowed(false);
        customizer.setAllowedImports(List.of());
        customizer.setAllowedStarImports(List.of());
        customizer.setAllowedStaticImports(List.of());
        customizer.setAllowedStaticStarImports(List.of());
        customizer.setIndirectImportCheckEnabled(false);
        return customizer;
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

    /** Interrupts owned tasks and releases every cached class loader. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
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

    private static final class ResultCounter {
        private int value;
    }

    /** Line-buffered adapter which cannot leak script output to the server console. */
    private static final class LoggingWriter extends Writer {

        private final ExecutionLogger logger;
        private final StringBuilder line = new StringBuilder();
        private boolean closed;

        private LoggingWriter(ExecutionLogger logger) {
            this.logger = logger;
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
                    line.append(character);
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
            if (line.isEmpty()) {
                return;
            }
            String message = line.toString();
            line.setLength(0);
            try {
                logger.info(message, null, Map.of());
            } catch (RuntimeException ignored) {
                // Observation loss must not change an ordinary EPHEMERAL business result.
            }
        }
    }

    /** Semantic-phase policy that catches fully qualified and dynamic escape paths. */
    private static final class RuntimeSecurityCustomizer extends CompilationCustomizer {

        private RuntimeSecurityCustomizer() {
            super(CompilePhase.SEMANTIC_ANALYSIS);
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
            run.getCode().visit(new CodeVisitorSupport() {
                @Override
                public void visitConstructorCallExpression(ConstructorCallExpression expression) {
                    if (!expression.isSpecialCall()) {
                        reject(expression, "explicit object construction is not allowed");
                    }
                    super.visitConstructorCallExpression(expression);
                }

                @Override
                public void visitClassExpression(ClassExpression expression) {
                    reject(expression, "class references are not allowed");
                    super.visitClassExpression(expression);
                }

                @Override
                public void visitCastExpression(CastExpression expression) {
                    rejectType(expression.getType(), expression);
                    super.visitCastExpression(expression);
                }

                @Override
                public void visitMethodCallExpression(MethodCallExpression expression) {
                    String method = expression.getMethodAsString();
                    if (method == null) {
                        reject(expression, "dynamic method names are not allowed");
                    }
                    rejectMethod(method, expression);
                    super.visitMethodCallExpression(expression);
                }

                @Override
                public void visitStaticMethodCallExpression(StaticMethodCallExpression expression) {
                    reject(expression, "static method calls are not allowed");
                    super.visitStaticMethodCallExpression(expression);
                }

                @Override
                public void visitMethodPointerExpression(MethodPointerExpression expression) {
                    reject(expression, "method pointers are not allowed");
                    super.visitMethodPointerExpression(expression);
                }

                @Override
                public void visitPropertyExpression(PropertyExpression expression) {
                    String property = expression.getPropertyAsString();
                    if (property == null) {
                        reject(expression, "dynamic property names are not allowed");
                    }
                    if (FORBIDDEN_PROPERTIES.contains(property)) {
                        reject(expression, "reflective property access is not allowed");
                    }
                    super.visitPropertyExpression(expression);
                }
            });
        }

        private static void rejectMethod(String method, ASTNode node) {
            if (method != null && FORBIDDEN_METHODS.contains(method)) {
                reject(node, "method is not allowed by the script sandbox");
            }
        }

        private static void rejectType(ClassNode type, ASTNode node) {
            if (type == null) {
                return;
            }
            String name = type.getName();
            if (FORBIDDEN_TYPES.contains(name)
                    || FORBIDDEN_TYPE_PREFIXES.stream().anyMatch(name::startsWith)) {
                reject(node, "type is not allowed by the script sandbox");
            }
        }

        private static void reject(ASTNode node, String message) {
            throw new SecurityException(message + " at line " + Math.max(1, node.getLineNumber()));
        }
    }
}
