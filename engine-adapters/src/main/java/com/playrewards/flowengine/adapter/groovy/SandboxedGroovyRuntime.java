package com.playrewards.flowengine.adapter.groovy;

import com.alibaba.fastjson2.JSON;
import com.playrewards.flowengine.contract.json.JsonValues;
import com.playrewards.flowengine.spi.ExecutionLogger;
import com.playrewards.flowengine.spi.ScriptRuntime;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import groovy.transform.ThreadInterrupt;
import groovyjarjarantlr4.v4.runtime.CharStreams;
import groovyjarjarantlr4.v4.runtime.Token;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.util.Locale;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.groovy.parser.antlr4.GroovyLexer;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.ClassCodeExpressionTransformer;
import org.codehaus.groovy.ast.ClassHelper;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.DynamicVariable;
import org.codehaus.groovy.ast.expr.ArgumentListExpression;
import org.codehaus.groovy.ast.expr.AttributeExpression;
import org.codehaus.groovy.ast.expr.BinaryExpression;
import org.codehaus.groovy.ast.expr.BooleanExpression;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.ClosureExpression;
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
import org.codehaus.groovy.runtime.typehandling.DefaultTypeTransformation;
import org.codehaus.groovy.runtime.typehandling.NumberMath;
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
    private static final int DEFAULT_MAX_INPUT_BYTES = 1_048_576;
    private static final int DEFAULT_MAX_INPUT_STRING_BYTES = 262_144;
    private static final int DEFAULT_MAX_INPUT_DEPTH = 32;
    private static final int DEFAULT_MAX_INPUT_NODES = 10_000;
    private static final long DEFAULT_MAX_OPERATION_UNITS = 100_000;
    private static final int MAX_RESULT_DEPTH = 32;
    private static final int MAX_RESULT_NODES = 10_000;
    private static final long STOP_POLL_MILLIS = 10;
    private static final String INTERNAL_JSON_ROOTS = "__flowJsonRoots";
    /**
     * Groovy Script 宿主属性名：脚本引用它们说明在探测宿主对象，必须保持沙箱违规；
     * 其余未注入的根变量只是宿主未提供该作用域（数据缺失），按运行时错误报告。
     */
    private static final Set<String> SCRIPT_HOST_PROPERTY_NAMES = Set.of(
            "binding", "properties", "metaClass", "class", "out", "args",
            "context", "delegate", "owner", "thisObject");
    /** 只接受普通标识符作为缺失作用域名回显，避免把任意文本写进错误消息。 */
    private static final Pattern SCOPE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");
    /** 从沙箱自身抛出的规则消息中提取摘要与行号；这些消息均为常量文本，不含宿主类名或输入值。 */
    private static final Pattern SANDBOX_RULE = Pattern.compile(
            "script sandbox (?:rejected )?([A-Za-z][A-Za-z0-9 ,'_-]{0,160}?)(?: at line (\\d+))?(?=$|[\\r\\n])",
            Pattern.MULTILINE);
    private static final Set<String> ALLOWED_METHODS = Set.of(
            "contains", "containsKey", "containsValue", "endsWith", "get", "isEmpty",
            "length", "max", "min", "size", "startsWith");
    private static final Set<String> ALLOWED_COLLECTION_METHODS = Set.of(
            "any", "collect", "every", "find", "findAll");
    private static final Set<String> BUDGETED_STRING_METHODS = Set.of(
            "strip", "substring", "toLowerCase", "toUpperCase", "trim");
    private static final Set<String> BUDGETED_NUMERIC_METHODS = Set.of("abs");
    private static final Set<String> LOG_METHODS = Set.of("print", "println");
    private static final Set<String> RESERVED_VARIABLES = Set.of(
            "this", "super", "owner", "delegate", "binding", "out", "metaClass",
            INTERNAL_JSON_ROOTS);
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
    private static final Set<Integer> BUDGETED_BINARY_TOKENS = Set.of(
            Types.PLUS, Types.MINUS, Types.MULTIPLY, Types.DIVIDE, Types.INTDIV, Types.MOD);
    private static final ThreadLocal<OperationBudget> ACTIVE_OPERATION_BUDGET = new ThreadLocal<>();

    private final Duration timeout;
    private final int maxScriptLength;
    private final int maxResultBytes;
    private final int cacheCapacity;
    private final Duration cacheTtl;
    private final Clock clock;
    private final int maxLogLineBytes;
    private final int maxLogTotalBytes;
    private final int maxLogEntries;
    private final int maxInputBytes;
    private final int maxInputStringBytes;
    private final int maxInputDepth;
    private final int maxInputNodes;
    private final long maxOperationUnits;
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
        this.maxInputBytes = DEFAULT_MAX_INPUT_BYTES;
        this.maxInputStringBytes = DEFAULT_MAX_INPUT_STRING_BYTES;
        this.maxInputDepth = DEFAULT_MAX_INPUT_DEPTH;
        this.maxInputNodes = DEFAULT_MAX_INPUT_NODES;
        this.maxOperationUnits = DEFAULT_MAX_OPERATION_UNITS;
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
        try {
            validateInvocationInputs(invocation);
        } catch (ScriptExecutionException failure) {
            return CompletableFuture.failedFuture(failure);
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
        Class<?> scriptClass = null;
        try {
            control.throwIfStopped();
            CachedScript compiled = compiled(invocation.script(), clock.instant());
            scriptClass = compiled.scriptClass();
            control.throwIfStopped();
            output = new LoggingWriter(invocation.logger(), maxLogLineBytes,
                    maxLogTotalBytes, maxLogEntries);
            Map<String, Object> jsonRoots = new LinkedHashMap<>(invocation.externalScopes());
            jsonRoots.put("ctx", invocation.ctx());
            Map<String, Object> bindings = new LinkedHashMap<>();
            bindings.put(INTERNAL_JSON_ROOTS, Collections.unmodifiableMap(jsonRoots));
            Script script = InvokerHelper.createScript(compiled.scriptClass(),
                    new Binding(bindings));
            script.setProperty("out", new PrintWriter(output, true));
            Object rawResult;
            ACTIVE_OPERATION_BUDGET.set(new OperationBudget(
                    maxResultBytes, MAX_RESULT_NODES, maxOperationUnits));
            try {
                rawResult = script.run();
            } finally {
                ACTIVE_OPERATION_BUDGET.remove();
            }
            new ResultResourceCounter(maxResultBytes, MAX_RESULT_DEPTH, MAX_RESULT_NODES)
                    .visit(rawResult, 0);
            Object normalized = normalize(rawResult, 0, new ResultCounter());
            if (JSON.toJSONBytes(normalized).length > maxResultBytes) {
                throw new ScriptExecutionException(
                        "SCRIPT_RESULT_TOO_LARGE", "script result exceeds the configured size limit");
            }
            output.close();
            output = null;
            control.succeed(normalized);
        } catch (SecurityException failure) {
            output = closeLogOutput(output);
            control.fail(securityViolation(failure, scriptClass));
        } catch (MultipleCompilationErrorsException failure) {
            output = closeLogOutput(output);
            control.fail(isSecurityFailure(failure)
                    ? securityViolation(failure, null)
                    : new ScriptExecutionException("SCRIPT_COMPILE_FAILED", "script compilation failed"));
        } catch (CancellationException failure) {
            output = closeLogOutput(output);
            control.cancelFromWorker();
        } catch (ScriptExecutionException failure) {
            output = closeLogOutput(output);
            control.fail(failure);
        } catch (Throwable failure) {
            output = closeLogOutput(output);
            if (Thread.currentThread().isInterrupted()
                    || invocation.cancellationToken().isCancellationRequested()) {
                control.cancelFromWorker();
            } else {
                ScriptExecutionException runtimeError = scriptRuntimeError(failure, scriptClass);
                control.fail(runtimeError != null ? runtimeError : new ScriptExecutionException(
                        "SCRIPT_RUNTIME_FAILED", "script execution failed"));
            }
        } finally {
            closeLogOutput(output);
            control.finishFromWorker();
        }
    }

    private static LoggingWriter closeLogOutput(LoggingWriter output) {
        if (output != null) {
            output.close();
        }
        return null;
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
        // Closure syntax is admitted only so the semantic validator can recognize the five
        // editor-published collection forms. Every other closure shape remains closed by default.
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
        operationBudget().consume(1);
        if (receiver == null && safe) {
            return null;
        }
        if (receiver == null) {
            // 读取空值的属性是脚本数据错误（等价 NPE），不是越权访问宿主对象。
            throw new ScriptDataException("null value has no property '" + key + "'");
        }
        if (!(receiver instanceof Map<?, ?> map)) {
            throw new SecurityException("script sandbox property receiver must be a JSON object");
        }
        return map.get(key);
    }

    /** Resolves a script root only from the adapter-injected JSON map, never Script properties. */
    public static Object readExternalJsonRoot(Object receiver, String name) {
        operationBudget().consume(1);
        if (!(receiver instanceof Map<?, ?> roots)) {
            throw new SecurityException("script sandbox JSON root is unavailable");
        }
        if (!roots.containsKey(name)) {
            if (SCRIPT_HOST_PROPERTY_NAMES.contains(name) || !SCOPE_NAME.matcher(name).matches()) {
                throw new SecurityException("script sandbox JSON root is unavailable");
            }
            // 根变量只从注入的 JSON 根读取、从不回退到宿主属性，缺失作用域不构成越权。
            throw new ScriptDataException("script references unavailable scope '" + name + "'");
        }
        return roots.get(name);
    }

    /** Reads only an exact JSON object key or JSON array index, without Groovy getAt dispatch. */
    public static Object readJsonIndex(Object receiver, Object index) {
        operationBudget().consume(1);
        if (receiver == null) {
            throw new ScriptDataException("null value cannot be indexed");
        }
        if (receiver instanceof Map<?, ?> map) {
            if (!(index instanceof String key)) {
                throw new SecurityException("script sandbox JSON object index must be a string");
            }
            return map.get(key);
        }
        if (receiver instanceof List<?> list) {
            int position = exactListIndex(index);
            if (position < 0 || position >= list.size()) {
                throw new ScriptDataException("list index " + position + " is out of range (size "
                        + list.size() + ")");
            }
            return list.get(position);
        }
        throw new SecurityException("script sandbox bracket receiver must be a JSON object or array");
    }

    private static int exactListIndex(Object value) {
        long index;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer
                || value instanceof Long) {
            index = ((Number) value).longValue();
        } else if (value instanceof BigInteger integer && integer.bitLength() < 63) {
            index = integer.longValue();
        } else {
            throw new SecurityException("script sandbox JSON array index must be an integer");
        }
        if (index < Integer.MIN_VALUE || index > Integer.MAX_VALUE) {
            throw new ScriptDataException("list index is out of range");
        }
        return (int) index;
    }

    /**
     * Executes every allocation-capable binary operator after reserving its output budget.
     *
     * @param operator Groovy numeric/text/list token generated by the trusted transformer
     * @param left copied JSON operand
     * @param right copied JSON operand
     * @return bounded JSON scalar or list result
     */
    public static Object applyBudgetedBinary(int operator, Object left, Object right) {
        OperationBudget budget = operationBudget();
        return switch (operator) {
            case Types.PLUS -> budgetedPlus(left, right, budget);
            case Types.MINUS, Types.DIVIDE, Types.INTDIV, Types.MOD ->
                    budgetedNumeric(operator, left, right, budget);
            case Types.MULTIPLY -> budgetedMultiply(left, right, budget);
            default -> throw new SecurityException("script sandbox binary operator is not generated");
        };
    }

    /** Executes unary numeric allocation only after reserving its projected result. */
    public static Object applyBudgetedUnary(int operator, Object value) {
        if (!(value instanceof Number number)) {
            throw new ScriptDataException("unary operand must be a number but was " + jsonType(value));
        }
        OperationBudget budget = operationBudget();
        long digits = numericAllocationBytes(number);
        budget.reserveNumber(saturatedAdd(digits, 1), digits);
        return switch (operator) {
            case Types.PLUS -> NumberMath.unaryPlus(number);
            case Types.MINUS -> NumberMath.unaryMinus(number);
            default -> throw new SecurityException("script sandbox unary operator is not generated");
        };
    }

    /**
     * Runs one editor-published collection operation over a JSON list with a controlled closure.
     *
     * @param method one of find/findAll/collect/any/every
     * @param receiver copied JSON list
     * @param closure compiler-validated single-argument closure
     * @return bounded JSON-compatible collection result
     */
    public static Object applyBudgetedCollection(String method, Object receiver, Closure<?> closure) {
        if (receiver == null) {
            throw new ScriptDataException("null value has no method '" + method + "'");
        }
        if (!(receiver instanceof List<?> values)) {
            throw new SecurityException("script sandbox collection receiver must be a JSON list");
        }
        Objects.requireNonNull(closure, "generated collection closure must not be null");
        OperationBudget budget = operationBudget();
        return switch (method) {
            case "find" -> find(values, closure, budget);
            case "findAll" -> findAll(values, closure, budget);
            case "collect" -> collect(values, closure, budget);
            case "any" -> any(values, closure, budget);
            case "every" -> every(values, closure, budget);
            default -> throw new SecurityException("script sandbox collection method is not generated");
        };
    }

    /**
     * Performs an allocation-capable string operation only after reserving a safe projection.
     *
     * @param method compiler-approved string operation
     * @param receiver copied JSON string
     * @return converted text using stable locale-independent rules
     */
    public static Object applyBudgetedStringMethod(String method, Object receiver, Object... arguments) {
        if (receiver == null) {
            throw new ScriptDataException("null value has no method '" + method + "'");
        }
        if (!(receiver instanceof String text)) {
            throw new SecurityException("script sandbox string method requires a JSON string");
        }
        OperationBudget budget = operationBudget();
        long inputBytes = JsonValues.jsonStringContentBytes(text);
        budget.consume(text.codePointCount(0, text.length()));
        return switch (method) {
            case "toLowerCase" -> {
                requireArity(method, arguments, 0, 0);
                budget.reserveString(saturatedMultiply(inputBytes, 3));
                yield text.toLowerCase(Locale.ROOT);
            }
            case "toUpperCase" -> {
                requireArity(method, arguments, 0, 0);
                budget.reserveString(saturatedMultiply(inputBytes, 3));
                yield text.toUpperCase(Locale.ROOT);
            }
            case "trim" -> {
                requireArity(method, arguments, 0, 0);
                budget.reserveString(inputBytes);
                yield text.trim();
            }
            case "strip" -> {
                requireArity(method, arguments, 0, 0);
                budget.reserveString(inputBytes);
                yield text.strip();
            }
            case "substring" -> {
                requireArity(method, arguments, 1, 2);
                int begin = exactStringIndex(arguments[0]);
                int end = arguments.length == 2 ? exactStringIndex(arguments[1]) : text.length();
                if (begin < 0 || end < begin || end > text.length()) {
                    throw new SecurityException("script sandbox substring indexes are outside the string");
                }
                // The full source bound is reserved before substring can allocate its copy.
                budget.reserveString(inputBytes);
                yield text.substring(begin, end);
            }
            default -> throw new SecurityException("script sandbox string method is not generated");
        };
    }

    /** Executes an allowlisted numeric method after reserving its projected result. */
    public static Object applyBudgetedNumericMethod(String method, Object receiver, Object... arguments) {
        if (receiver == null) {
            throw new ScriptDataException("null value has no method '" + method + "'");
        }
        if (!(receiver instanceof Number number)) {
            throw new SecurityException("script sandbox numeric method requires a JSON number");
        }
        requireArity(method, arguments, 0, 0);
        long digits = numericAllocationBytes(number);
        operationBudget().reserveNumber(saturatedAdd(digits, 1), digits);
        if ("abs".equals(method)) {
            return NumberMath.abs(number);
        }
        throw new SecurityException("script sandbox numeric method is not generated");
    }

    /** Builds one scalar-only interpolated string after all output resources are reserved. */
    public static Object applyBudgetedGString(Object... parts) {
        if (parts.length == 0 || (parts.length & 1) == 0) {
            throw new SecurityException("script sandbox interpolation shape is invalid");
        }
        for (int index = 0; index < parts.length; index++) {
            if ((index & 1) == 0 && !(parts[index] instanceof String)) {
                throw new SecurityException("script sandbox interpolation segment must be text");
            }
        }
        TextEstimate estimate = estimateScalarText(parts);
        OperationBudget budget = operationBudget();
        reserveTextEstimate(budget, estimate, true);
        StringBuilder result = new StringBuilder();
        for (Object part : parts) {
            result.append(part instanceof String text ? text : scalarText(part));
        }
        return result.toString();
    }

    private static void requireArity(String method, Object[] arguments, int minimum, int maximum) {
        if (arguments.length < minimum || arguments.length > maximum) {
            throw new SecurityException("script sandbox " + method + " argument count is invalid");
        }
    }

    private static int exactStringIndex(Object value) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).intValue();
        }
        throw new SecurityException("script sandbox substring indexes must be integers");
    }

    /** Builds a JSON list literal only after every referenced value fits the invocation budget. */
    public static Object applyBudgetedList(Object... values) {
        OperationBudget budget = operationBudget();
        budget.reserveCollectionStart();
        for (Object value : values) {
            budget.reserveCollectionElement(value);
        }
        List<Object> result = new ArrayList<>(values.length);
        Collections.addAll(result, values);
        return Collections.unmodifiableList(result);
    }

    /** Builds a JSON object literal only after every key/value pair fits the invocation budget. */
    public static Object applyBudgetedMap(Object... entries) {
        if ((entries.length & 1) != 0) {
            throw new SecurityException("script sandbox object literal entries must be paired");
        }
        OperationBudget budget = operationBudget();
        budget.reserveObjectStart();
        for (int index = 0; index < entries.length; index += 2) {
            if (!(entries[index] instanceof String key)) {
                throw new SecurityException("script sandbox object literal keys must be strings");
            }
            budget.reserveObjectEntry(key, entries[index + 1]);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            result.put((String) entries[index], entries[index + 1]);
        }
        return Collections.unmodifiableMap(result);
    }

    private static Object budgetedPlus(Object left, Object right, OperationBudget budget) {
        budget.consume(1);
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return budgetedNumeric(Types.PLUS, leftNumber, rightNumber, budget);
        }
        if (left instanceof List<?> leftList && right instanceof List<?> rightList) {
            budget.reserveCombinedLists(leftList, rightList);
            List<Object> result = new ArrayList<>(leftList.size() + rightList.size());
            result.addAll(leftList);
            result.addAll(rightList);
            return Collections.unmodifiableList(result);
        }
        if (isTextScalar(left) || isTextScalar(right)) {
            TextEstimate estimate = estimateScalarText(left, right);
            reserveTextEstimate(budget, estimate, false);
            String leftText = scalarText(left);
            String rightText = scalarText(right);
            return leftText + rightText;
        }
        throw new ScriptDataException("cannot apply '+' to " + jsonType(left) + " and " + jsonType(right));
    }

    private static Object budgetedMultiply(Object left, Object right, OperationBudget budget) {
        budget.consume(1);
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return budgetedNumeric(Types.MULTIPLY, leftNumber, rightNumber, budget);
        }
        int repetitions = exactRepetitions(right);
        if (left instanceof String text) {
            budget.reserveString(JsonValues.jsonRepeatedStringContentBytes(text, repetitions));
            return text.repeat(repetitions);
        }
        if (left instanceof List<?> values) {
            budget.reserveRepeatedList(values, repetitions);
            List<Object> result = new ArrayList<>(Math.multiplyExact(values.size(), repetitions));
            for (int index = 0; index < repetitions; index++) {
                result.addAll(values);
            }
            return Collections.unmodifiableList(result);
        }
        throw new ScriptDataException("cannot apply '*' to " + jsonType(left) + " and " + jsonType(right));
    }

    private static Object budgetedNumeric(int operator, Object left, Object right, OperationBudget budget) {
        if (!(left instanceof Number leftNumber) || !(right instanceof Number rightNumber)) {
            throw new ScriptDataException("arithmetic operator requires numbers but got "
                    + jsonType(left) + " and " + jsonType(right));
        }
        long leftDigits = numericAllocationBytes(leftNumber);
        long rightDigits = numericAllocationBytes(rightNumber);
        long projected = switch (operator) {
            case Types.PLUS, Types.MINUS ->
                    saturatedAdd(saturatedAdd(leftDigits, rightDigits), 1);
            case Types.MULTIPLY -> saturatedAdd(leftDigits, rightDigits);
            case Types.DIVIDE -> saturatedAdd(saturatedAdd(leftDigits, rightDigits), 32);
            case Types.INTDIV -> saturatedAdd(leftDigits, 1);
            case Types.MOD -> saturatedAdd(Math.max(leftDigits, rightDigits), 1);
            default -> throw new SecurityException("script sandbox numeric operator is not generated");
        };
        long complexity = switch (operator) {
            case Types.MULTIPLY, Types.DIVIDE, Types.INTDIV, Types.MOD ->
                    saturatedMultiply(leftDigits, rightDigits);
            default -> saturatedAdd(leftDigits, rightDigits);
        };
        budget.reserveNumber(projected, complexity);
        return switch (operator) {
            case Types.PLUS -> NumberMath.add(leftNumber, rightNumber);
            case Types.MINUS -> NumberMath.subtract(leftNumber, rightNumber);
            case Types.MULTIPLY -> NumberMath.multiply(leftNumber, rightNumber);
            case Types.DIVIDE -> NumberMath.divide(leftNumber, rightNumber);
            case Types.INTDIV -> NumberMath.intdiv(leftNumber, rightNumber);
            case Types.MOD -> NumberMath.mod(leftNumber, rightNumber);
            default -> throw new SecurityException("script sandbox numeric operator is not generated");
        };
    }

    private static int exactRepetitions(Object value) {
        long result;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            result = ((Number) value).longValue();
        } else if (value instanceof Long longValue) {
            result = longValue;
        } else if (value instanceof BigInteger integer && integer.bitLength() < 63) {
            result = integer.longValue();
        } else {
            throw new SecurityException("script sandbox repetition count must be an integer");
        }
        if (result < 0 || result > Integer.MAX_VALUE) {
            throw OperationBudget.exhausted();
        }
        return (int) result;
    }

    private static boolean isTextScalar(Object value) {
        return value instanceof String;
    }

    private static String scalarText(Object value) {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        throw new SecurityException("script sandbox string concatenation accepts JSON scalars only");
    }

    /** Estimates every string allocation before any scalar is rendered to text. */
    private static TextEstimate estimateScalarText(Object... values) {
        long bytes = 0;
        long codePoints = 0;
        List<Long> scalarAllocations = new ArrayList<>();
        List<CharSequence> textRun = new ArrayList<>();
        for (Object value : values) {
            if (value instanceof String text) {
                textRun.add(text);
            } else {
                if (!textRun.isEmpty()) {
                    JsonValues.JsonStringUsage usage = JsonValues.measureJsonString(textRun);
                    bytes = saturatedAdd(bytes, usage.contentBytes());
                    codePoints = saturatedAdd(codePoints, usage.codePoints());
                    textRun.clear();
                }
                if (value == null) {
                    bytes = saturatedAdd(bytes, 4);
                    codePoints = saturatedAdd(codePoints, 4);
                } else if (value instanceof Number number) {
                    long upperBound = JsonValues.javaNumberTextUpperBoundBytes(number);
                    bytes = saturatedAdd(bytes, upperBound);
                    codePoints = saturatedAdd(codePoints, upperBound);
                    scalarAllocations.add(upperBound);
                } else if (value instanceof Boolean bool) {
                    long length = bool ? 4 : 5;
                    bytes = saturatedAdd(bytes, length);
                    codePoints = saturatedAdd(codePoints, length);
                } else {
                    throw new SecurityException(
                            "script sandbox string concatenation accepts JSON scalars only");
                }
            }
        }
        if (!textRun.isEmpty()) {
            JsonValues.JsonStringUsage usage = JsonValues.measureJsonString(textRun);
            bytes = saturatedAdd(bytes, usage.contentBytes());
            codePoints = saturatedAdd(codePoints, usage.codePoints());
        }
        return new TextEstimate(bytes, codePoints, List.copyOf(scalarAllocations));
    }

    private static void reserveTextEstimate(
            OperationBudget budget, TextEstimate estimate, boolean consumeCodePoints) {
        if (consumeCodePoints) {
            budget.consume(estimate.codePoints());
        }
        for (long allocation : estimate.scalarAllocations()) {
            budget.reserveString(allocation);
        }
        budget.reserveString(estimate.contentBytes());
    }

    /** Upper-bounds the digits a numeric operation may need without materializing numeric text. */
    private static long numericAllocationBytes(Number value) {
        return JsonValues.numericOperationWorkspaceUpperBoundBytes(value);
    }

    /** Allocation and Unicode work reserved before scalar text construction. */
    private record TextEstimate(long contentBytes, long codePoints, List<Long> scalarAllocations) {
    }

    private static Object find(List<?> values, Closure<?> closure, OperationBudget budget) {
        for (Object value : values) {
            budget.consume(1);
            if (truth(closure.call(value), budget)) {
                return value;
            }
        }
        return null;
    }

    private static List<Object> findAll(List<?> values, Closure<?> closure, OperationBudget budget) {
        List<Object> result = new ArrayList<>();
        budget.reserveCollectionStart();
        for (Object value : values) {
            budget.consume(1);
            if (truth(closure.call(value), budget)) {
                budget.reserveCollectionElement(value);
                result.add(value);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static List<Object> collect(List<?> values, Closure<?> closure, OperationBudget budget) {
        List<Object> result = new ArrayList<>();
        budget.reserveCollectionStart();
        for (Object value : values) {
            budget.consume(1);
            Object mapped = jsonClosureResult(closure.call(value), budget);
            budget.reserveCollectionElement(mapped);
            result.add(mapped);
        }
        return Collections.unmodifiableList(result);
    }

    private static boolean any(List<?> values, Closure<?> closure, OperationBudget budget) {
        for (Object value : values) {
            budget.consume(1);
            if (truth(closure.call(value), budget)) {
                return true;
            }
        }
        return false;
    }

    private static boolean every(List<?> values, Closure<?> closure, OperationBudget budget) {
        for (Object value : values) {
            budget.consume(1);
            if (!truth(closure.call(value), budget)) {
                return false;
            }
        }
        return true;
    }

    private static boolean truth(Object value, OperationBudget budget) {
        return DefaultTypeTransformation.castToBoolean(scalarClosureResult(value, budget));
    }

    private static Object scalarClosureResult(Object value, OperationBudget budget) {
        if (value != null && !(value instanceof String) && !(value instanceof Boolean)
                && !(value instanceof Number)) {
            throw new SecurityException("script sandbox collection closure must return a JSON scalar");
        }
        budget.observeJson(value);
        return value;
    }

    private static Object jsonClosureResult(Object value, OperationBudget budget) {
        JsonMeasure.of(value);
        budget.observeJson(value);
        return value;
    }

    private static OperationBudget operationBudget() {
        OperationBudget budget = ACTIVE_OPERATION_BUDGET.get();
        if (budget == null) {
            throw new SecurityException("script sandbox operation budget is unavailable");
        }
        return budget;
    }

    private void evictExpired(Instant now) {
        List<String> expired = cache.entrySet().stream()
                .filter(entry -> !now.isBefore(entry.getValue().expiresAt()))
                .map(Map.Entry::getKey).toList();
        expired.forEach(key -> cache.remove(key).close());
    }

    private void validateInvocationInputs(ScriptInvocation invocation) {
        try {
            JsonValues.measureJsonResources(List.of(invocation.externalScopes(), invocation.ctx()),
                    new JsonValues.JsonResourceLimits(
                            maxInputBytes, maxInputStringBytes, maxInputDepth, maxInputNodes));
        } catch (JsonValues.JsonResourceLimitException failure) {
            throw new ScriptExecutionException(
                    "SCRIPT_INPUT_TOO_LARGE", "script input exceeds the configured resource limits");
        }
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

    private static int utf8Length(String value) {
        int length = 0;
        for (int offset = 0; offset < value.length();) {
            int codePoint = value.codePointAt(offset);
            length += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2
                    : codePoint <= 0xffff ? 3 : 4;
            offset += Character.charCount(codePoint);
        }
        return length;
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

    /**
     * 脚本对 JSON 数据的普通运行时错误（空值访问、越界、类型不匹配）。
     *
     * <p>与 {@link SecurityException} 区分：它不代表试图越过沙箱，只说明脚本逻辑与输入数据不匹配，
     * 对外映射为 {@code SCRIPT_RUNTIME_ERROR}。消息只包含脚本自身的键名与 JSON 类型名，不包含输入值。
     */
    static final class ScriptDataException extends RuntimeException {
        ScriptDataException(String reason) {
            // 保留调用栈：行号从栈中脚本类的帧解析，栈本身从不对外输出。
            super(reason);
        }
    }

    /** 只返回 JSON 类型名，避免把输入值写入错误消息。 */
    private static String jsonType(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "string";
        if (value instanceof Number) return "number";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof List<?>) return "list";
        if (value instanceof Map<?, ?>) return "object";
        return "unsupported value";
    }

    /** 宿主类型的普通运行时异常：以类型简名描述，消息文本可能包含输入值，因此不对外输出。 */
    private static final Set<Class<? extends Throwable>> SCRIPT_RUNTIME_TYPES = Set.of(
            NullPointerException.class, ClassCastException.class, ArithmeticException.class,
            IndexOutOfBoundsException.class, NumberFormatException.class,
            IllegalArgumentException.class, UnsupportedOperationException.class,
            groovy.lang.MissingMethodException.class, groovy.lang.MissingPropertyException.class);

    /**
     * 把脚本内的普通运行时失败转换为带行号的安全错误；无法归因到脚本代码时返回 null（保持原通用错误）。
     *
     * @param failure 脚本执行抛出的异常
     * @param scriptClass 本次执行的已编译脚本类，用于在调用栈中定位脚本行号
     */
    private static ScriptExecutionException scriptRuntimeError(Throwable thrown, Class<?> scriptClass) {
        // Groovy 反射调用可能把真实异常包在 InvokerInvocationException 等外层中，沿 cause 链寻找。
        Throwable failure = thrown;
        while (failure != null && !(failure instanceof ScriptDataException)
                && !isScriptRuntimeType(failure)) {
            failure = failure.getCause() == failure ? null : failure.getCause();
        }
        if (failure == null) {
            return null;
        }
        boolean data = failure instanceof ScriptDataException;
        String reason = data ? failure.getMessage() : failure.getClass().getSimpleName();
        int line = scriptLine(failure, scriptClass);
        if (line < 0 && !data) {
            return null;
        }
        String at = line > 0 ? " at line " + line : "";
        // 兼容：宿主运行时异常沿用既有 SCRIPT_RUNTIME_FAILED，只补充行号与异常类型；新码
        // SCRIPT_RUNTIME_ERROR 只用于此前被误报为沙箱违规的 JSON 数据错误。
        return data
                ? new ScriptExecutionException("SCRIPT_RUNTIME_ERROR",
                        "script runtime error" + at + ": " + reason)
                : new ScriptExecutionException("SCRIPT_RUNTIME_FAILED",
                        "script execution failed" + at + ": " + reason);
    }

    private static boolean isScriptRuntimeType(Throwable failure) {
        for (Class<? extends Throwable> type : SCRIPT_RUNTIME_TYPES) {
            if (type.isInstance(failure)) {
                return true;
            }
        }
        return false;
    }

    private static int scriptLine(Throwable failure, Class<?> scriptClass) {
        if (scriptClass == null) {
            return -1;
        }
        String name = scriptClass.getName();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            for (StackTraceElement frame : current.getStackTrace()) {
                // 闭包编译为 Script$_run_closure1 等内部类，同样属于脚本源码。
                if ((frame.getClassName().equals(name) || frame.getClassName().startsWith(name + "$"))
                        && frame.getLineNumber() > 0) {
                    return frame.getLineNumber();
                }
            }
        }
        return -1;
    }

    /**
     * 构造沙箱违规错误：在通用文案后附加沙箱自身的规则摘要与脚本行号，便于运营定位。
     *
     * <p>只回显匹配 {@link #SANDBOX_RULE} 的常量规则文本，不输出 Groovy 编译器原文、宿主类名、
     * 文件路径或输入值；无法提取时保持原通用文案。</p>
     *
     * @param failure 沙箱拒绝异常（运行期 SecurityException 或编译期 MultipleCompilationErrorsException）
     * @param scriptClass 运行期已编译脚本类，用于从调用栈定位行号；编译期传 null
     */
    private static ScriptExecutionException securityViolation(Throwable failure, Class<?> scriptClass) {
        String summary = null;
        int line = -1;
        for (Throwable current = failure; current != null && summary == null;
                current = current.getCause() == current ? null : current.getCause()) {
            String message = current.getMessage();
            Matcher matcher = message == null ? null : SANDBOX_RULE.matcher(message);
            if (matcher != null && matcher.find()) {
                summary = matcher.group(1).trim();
                line = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : -1;
            }
        }
        if (line < 0) {
            line = scriptLine(failure, scriptClass);
        }
        StringBuilder message = new StringBuilder("script violates the sandbox policy");
        if (line > 0) {
            message.append(" at line ").append(line);
        }
        if (summary != null && !summary.isEmpty()) {
            message.append(": ").append(summary);
        }
        return new ScriptExecutionException("SCRIPT_SECURITY_VIOLATION", message.toString());
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

    /** Measures the raw result before normalization or allocating a complete JSON byte buffer. */
    private static final class ResultResourceCounter {

        private final long maxBytes;
        private final int maxDepth;
        private final int maxNodes;
        private long bytes;
        private int nodes;

        private ResultResourceCounter(long maxBytes, int maxDepth, int maxNodes) {
            this.maxBytes = maxBytes;
            this.maxDepth = maxDepth;
            this.maxNodes = maxNodes;
        }

        private void visit(Object value, int depth) {
            if (depth > maxDepth || ++nodes > maxNodes) {
                tooLarge("script result exceeds the configured structure limit");
            }
            if (value == null) {
                addBytes(4);
                return;
            }
            if (value instanceof String text) {
                addBytes(JsonValues.jsonEncodedStringBytes(text));
                return;
            }
            if (value instanceof Character character) {
                addBytes(JsonValues.jsonEncodedStringBytes(character.toString()));
                return;
            }
            if (value instanceof CharSequence text) {
                addBytes(JsonValues.jsonEncodedStringBytes(text.toString()));
                return;
            }
            if (value instanceof Boolean bool) {
                addBytes(bool ? 4 : 5);
                return;
            }
            if (value instanceof Number number) {
                try {
                    addBytes(JsonValues.jsonEncodedNumberBytes(number, maxBytes));
                } catch (JsonValues.JsonResourceLimitException failure) {
                    tooLarge("script result exceeds the configured numeric workspace limit");
                }
                return;
            }
            if (value instanceof Map<?, ?> map) {
                addBytes(2);
                boolean first = true;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String key)) {
                        throw new ScriptExecutionException(
                                "SCRIPT_RESULT_NOT_JSON", "script result object keys must be strings");
                    }
                    if (!first) {
                        addBytes(1);
                    }
                    first = false;
                    addBytes(saturatedAdd(JsonValues.jsonEncodedStringBytes(key), 1));
                    visit(entry.getValue(), depth + 1);
                }
                return;
            }
            if (value instanceof Iterable<?> iterable) {
                addBytes(2);
                boolean first = true;
                for (Object child : iterable) {
                    if (!first) {
                        addBytes(1);
                    }
                    first = false;
                    visit(child, depth + 1);
                }
                return;
            }
            if (value.getClass().isArray()) {
                addBytes(2);
                for (int index = 0; index < Array.getLength(value); index++) {
                    if (index > 0) {
                        addBytes(1);
                    }
                    visit(Array.get(value, index), depth + 1);
                }
                return;
            }
            throw new ScriptExecutionException(
                    "SCRIPT_RESULT_NOT_JSON", "script result must be JSON-compatible");
        }

        private void addBytes(long amount) {
            bytes = saturatedAdd(bytes, amount);
            if (bytes > maxBytes) {
                tooLarge("script result exceeds the configured size limit");
            }
        }

        private static void tooLarge(String message) {
            throw new ScriptExecutionException("SCRIPT_RESULT_TOO_LARGE", message);
        }
    }

    /** Per-worker budget consumed before any growth-capable allocation is performed. */
    private static final class OperationBudget {

        private final long maxValueBytes;
        private final long maxNodes;
        private final long maxUnits;
        private long allocatedBytes;
        private long allocatedNodes;
        private long usedUnits;

        private OperationBudget(long maxValueBytes, long maxNodes, long maxUnits) {
            this.maxValueBytes = maxValueBytes;
            this.maxNodes = maxNodes;
            this.maxUnits = maxUnits;
        }

        private void consume(long units) {
            usedUnits = saturatedAdd(usedUnits, Math.max(1, units));
            if (usedUnits > maxUnits) {
                throw exhausted();
            }
        }

        private void reserveString(long bytes) {
            if (bytes > maxValueBytes) {
                throw exhausted();
            }
            reserve(saturatedAdd(bytes, 2), 1);
        }

        private void reserveCollectionStart() {
            reserve(2, 1);
        }

        private void reserveObjectStart() {
            reserve(2, 1);
        }

        private void reserveCollectionElement(Object value) {
            JsonMeasure measure = JsonMeasure.of(value);
            reserve(saturatedAdd(measure.bytes(), 1), measure.nodes());
        }

        private void reserveObjectEntry(String key, Object value) {
            JsonMeasure measure = JsonMeasure.of(value);
            reserve(saturatedAdd(saturatedAdd(JsonValues.jsonStringContentBytes(key), 4), measure.bytes()),
                    measure.nodes());
        }

        private void reserveCombinedLists(List<?> left, List<?> right) {
            reserveCollectionStart();
            left.forEach(this::reserveCollectionElement);
            right.forEach(this::reserveCollectionElement);
        }

        private void reserveRepeatedList(List<?> values, int repetitions) {
            consume(repetitions);
            JsonMeasure source = JsonMeasure.of(values);
            long projectedBytes = saturatedMultiply(source.bytes(), repetitions);
            long projectedNodes = saturatedAdd(
                    saturatedMultiply(Math.max(1, source.nodes() - 1), repetitions), 1);
            reserve(projectedBytes, projectedNodes);
        }

        private void reserveNumber(long bytes, long complexity) {
            consume(complexity);
            reserve(bytes, 1);
        }

        private void observeJson(Object value) {
            JsonMeasure measure = JsonMeasure.of(value);
            if (measure.bytes() > maxValueBytes || measure.nodes() > maxNodes) {
                throw exhausted();
            }
        }

        private void reserve(long bytes, long nodes) {
            if (bytes > maxValueBytes || nodes > maxNodes) {
                throw exhausted();
            }
            allocatedBytes = saturatedAdd(allocatedBytes, bytes);
            allocatedNodes = saturatedAdd(allocatedNodes, nodes);
            if (allocatedBytes > maxValueBytes || allocatedNodes > maxNodes) {
                throw exhausted();
            }
            consume(saturatedAdd(bytes, nodes));
        }

        private static ScriptExecutionException exhausted() {
            return new ScriptExecutionException(
                    "SCRIPT_OPERATION_LIMIT_EXCEEDED", "script operation budget was exceeded");
        }
    }

    /** Conservative JSON size/shape estimate used before collection allocations. */
    private record JsonMeasure(long bytes, long nodes) {

        private static JsonMeasure of(Object value) {
            return measure(value, 0, new ResultCounter());
        }

        private static JsonMeasure measure(Object value, int depth, ResultCounter counter) {
            if (depth > MAX_RESULT_DEPTH || ++counter.value > MAX_RESULT_NODES) {
                throw OperationBudget.exhausted();
            }
            if (value == null) {
                return new JsonMeasure(4, 1);
            }
            if (value instanceof String text) {
                return new JsonMeasure(JsonValues.jsonEncodedStringBytes(text), 1);
            }
            if (value instanceof Boolean) {
                return new JsonMeasure(5, 1);
            }
            if (value instanceof Number number) {
                return new JsonMeasure(JsonValues.jsonNumberWorkspaceUpperBoundBytes(number), 1);
            }
            if (value instanceof Map<?, ?> map) {
                long bytes = 2;
                long nodes = 1;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String key)) {
                        throw new SecurityException("script operation produced a non-JSON object key");
                    }
                    JsonMeasure child = measure(entry.getValue(), depth + 1, counter);
                    bytes = saturatedAdd(bytes,
                            saturatedAdd(JsonValues.jsonStringContentBytes(key) + 4L, child.bytes()));
                    nodes = saturatedAdd(nodes, child.nodes());
                }
                return new JsonMeasure(bytes, nodes);
            }
            if (value instanceof Iterable<?> iterable) {
                long bytes = 2;
                long nodes = 1;
                for (Object childValue : iterable) {
                    JsonMeasure child = measure(childValue, depth + 1, counter);
                    bytes = saturatedAdd(bytes, saturatedAdd(child.bytes(), 1));
                    nodes = saturatedAdd(nodes, child.nodes());
                }
                return new JsonMeasure(bytes, nodes);
            }
            throw new SecurityException("script operation produced a non-JSON value");
        }

    }

    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left != 0 && right > Long.MAX_VALUE / left) {
            return Long.MAX_VALUE;
        }
        return left * right;
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

    /** Rewrites approved operations before bytecode generation to remove unbudgeted GDK dispatch. */
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
                    if (expression instanceof GStringExpression interpolated) {
                        ArgumentListExpression arguments = new ArgumentListExpression();
                        List<ConstantExpression> strings = interpolated.getStrings();
                        List<Expression> values = interpolated.getValues();
                        for (int index = 0; index < strings.size(); index++) {
                            arguments.addExpression(transform(strings.get(index)));
                            if (index < values.size()) {
                                arguments.addExpression(transform(values.get(index)));
                            }
                        }
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedGString", arguments);
                        rewritten.setSourcePosition(interpolated);
                        return rewritten;
                    }
                    if (expression instanceof VariableExpression variable
                            && variable.getAccessedVariable() instanceof DynamicVariable) {
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "readExternalJsonRoot",
                                new ArgumentListExpression(
                                        new VariableExpression(
                                                new DynamicVariable(INTERNAL_JSON_ROOTS, false)),
                                        new ConstantExpression(variable.getName())));
                        rewritten.setSourcePosition(variable);
                        return rewritten;
                    }
                    if (expression instanceof PropertyExpression property
                            && !(property instanceof AttributeExpression)) {
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
                    if (expression instanceof BinaryExpression binary
                            && binary.getOperation().getType() == Types.LEFT_SQUARE_BRACKET) {
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "readJsonIndex",
                                new ArgumentListExpression(
                                        transform(binary.getLeftExpression()),
                                        transform(binary.getRightExpression())));
                        rewritten.setSourcePosition(binary);
                        return rewritten;
                    }
                    if (expression instanceof BinaryExpression binary
                            && BUDGETED_BINARY_TOKENS.contains(binary.getOperation().getType())) {
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedBinary",
                                new ArgumentListExpression(
                                        new ConstantExpression(binary.getOperation().getType()),
                                        transform(binary.getLeftExpression()),
                                        transform(binary.getRightExpression())));
                        rewritten.setSourcePosition(binary);
                        return rewritten;
                    }
                    if (expression instanceof UnaryMinusExpression unary) {
                        return budgetedUnary(unary, Types.MINUS, unary.getExpression());
                    }
                    if (expression instanceof UnaryPlusExpression unary) {
                        return budgetedUnary(unary, Types.PLUS, unary.getExpression());
                    }
                    if (expression instanceof ListExpression list) {
                        ArgumentListExpression arguments = new ArgumentListExpression();
                        list.getExpressions().forEach(
                                child -> arguments.addExpression(transform(child)));
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedList", arguments);
                        rewritten.setSourcePosition(list);
                        return rewritten;
                    }
                    if (expression instanceof MapExpression map) {
                        ArgumentListExpression arguments = new ArgumentListExpression();
                        map.getMapEntryExpressions().forEach(entry -> {
                            arguments.addExpression(transform(entry.getKeyExpression()));
                            arguments.addExpression(transform(entry.getValueExpression()));
                        });
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedMap", arguments);
                        rewritten.setSourcePosition(map);
                        return rewritten;
                    }
                    if (expression instanceof MethodCallExpression call
                            && ALLOWED_COLLECTION_METHODS.contains(call.getMethodAsString())) {
                        ArgumentListExpression arguments = (ArgumentListExpression) call.getArguments();
                        ClosureExpression closure = (ClosureExpression) arguments.getExpression(0);
                        // The general expression transformer does not descend into closure code when
                        // the enclosing method call is replaced, so rewrite the validated body first.
                        closure.getCode().visit(this);
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedCollection",
                                new ArgumentListExpression(
                                        new ConstantExpression(call.getMethodAsString()),
                                        transform(call.getObjectExpression()),
                                        closure));
                        rewritten.setSourcePosition(call);
                        return rewritten;
                    }
                    if (expression instanceof MethodCallExpression call
                            && BUDGETED_STRING_METHODS.contains(call.getMethodAsString())) {
                        ArgumentListExpression sourceArguments =
                                (ArgumentListExpression) call.getArguments();
                        ArgumentListExpression arguments = new ArgumentListExpression(
                                new ConstantExpression(call.getMethodAsString()),
                                transform(call.getObjectExpression()));
                        sourceArguments.getExpressions().forEach(
                                argument -> arguments.addExpression(transform(argument)));
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedStringMethod", arguments);
                        rewritten.setSourcePosition(call);
                        return rewritten;
                    }
                    if (expression instanceof MethodCallExpression call
                            && BUDGETED_NUMERIC_METHODS.contains(call.getMethodAsString())) {
                        ArgumentListExpression sourceArguments =
                                (ArgumentListExpression) call.getArguments();
                        ArgumentListExpression arguments = new ArgumentListExpression(
                                new ConstantExpression(call.getMethodAsString()),
                                transform(call.getObjectExpression()));
                        sourceArguments.getExpressions().forEach(
                                argument -> arguments.addExpression(transform(argument)));
                        StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                                ClassHelper.make(SandboxedGroovyRuntime.class),
                                "applyBudgetedNumericMethod", arguments);
                        rewritten.setSourcePosition(call);
                        return rewritten;
                    }
                    return super.transform(expression);
                }

                private Expression budgetedUnary(ASTNode sourceNode, int operator, Expression operand) {
                    StaticMethodCallExpression rewritten = new StaticMethodCallExpression(
                            ClassHelper.make(SandboxedGroovyRuntime.class),
                            "applyBudgetedUnary",
                            new ArgumentListExpression(
                                    new ConstantExpression(operator), transform(operand)));
                    rewritten.setSourcePosition(sourceNode);
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
            private final Set<String> assignedLocalVariables = new HashSet<>();
            private final Set<String> capturedLocalVariables = new HashSet<>();
            private final boolean closureMode;
            private final Set<String> closureParameters;
            private final AllowedScriptAst outerScope;

            private AllowedScriptAst() {
                this(false, Set.of(), null);
            }

            private AllowedScriptAst(boolean closureMode, Set<String> closureParameters,
                    AllowedScriptAst outerScope) {
                this.closureMode = closureMode;
                this.closureParameters = closureParameters;
                this.outerScope = outerScope;
            }

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
                    if (closureMode) {
                        reject(loop, "collection closures may only contain one scalar expression");
                    }
                    validateExpression(loop.getBooleanExpression());
                    validateStatement(loop.getLoopBlock());
                    return;
                }
                if (statement instanceof BreakStatement
                        || statement instanceof ContinueStatement
                        || statement instanceof EmptyStatement) {
                    if (closureMode && !(statement instanceof EmptyStatement)) {
                        reject(statement, "collection closures may not alter control flow");
                    }
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
                if (expression instanceof GStringExpression interpolated) {
                    interpolated.getValues().forEach(this::validateExpression);
                    return;
                }
                if (expression instanceof VariableExpression variable) {
                    validateVariable(variable);
                    return;
                }
                if (expression instanceof DeclarationExpression declaration) {
                    if (closureMode) {
                        reject(declaration, "collection closures may not declare local state");
                    }
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
                if (closureMode && !closureParameters.contains(variable.getName())) {
                    AllowedScriptAst declaredScope = findDeclaringOuterScope(variable.getName());
                    if (declaredScope != null) {
                        if (declaredScope.assignedLocalVariables.contains(variable.getName())) {
                            reject(variable, "collection closures may only capture immutable local values");
                        }
                        declaredScope.capturedLocalVariables.add(variable.getName());
                    }
                }
            }

            private AllowedScriptAst findDeclaringOuterScope(String name) {
                for (AllowedScriptAst scope = outerScope; scope != null; scope = scope.outerScope) {
                    if (scope.localVariables.contains(name)) {
                        return scope;
                    }
                }
                return null;
            }

            private void validateBinary(BinaryExpression binary) {
                int token = binary.getOperation().getType();
                if (!ALLOWED_BINARY_TOKENS.contains(token)) {
                    reject(binary, "operator is outside the token allowlist");
                }
                if (token == Types.ASSIGN) {
                    if (closureMode) {
                        reject(binary, "collection closures may not assign state");
                    }
                    if (!(binary.getLeftExpression() instanceof VariableExpression variable)
                            || !localVariables.contains(variable.getName())) {
                        reject(binary, "assignment targets must be declared local variables");
                    } else if (capturedLocalVariables.contains(variable.getName())) {
                        reject(binary, "captured local variables must remain immutable");
                    } else {
                        assignedLocalVariables.add(variable.getName());
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
                    boolean readable = closureMode
                            ? closureParameters.contains(variable.getName())
                                    || findDeclaringOuterScope(variable.getName()) != null
                                    || !localVariables.contains(variable.getName())
                            : !localVariables.contains(variable.getName());
                    return readable && !RESERVED_VARIABLES.contains(variable.getName())
                            && !variable.isThisExpression() && !variable.isSuperExpression();
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
                if (ALLOWED_COLLECTION_METHODS.contains(method)) {
                    validateCollectionCall(call);
                    return;
                }
                if (BUDGETED_STRING_METHODS.contains(method)) {
                    validateBudgetedStringCall(call);
                    return;
                }
                if (BUDGETED_NUMERIC_METHODS.contains(method)) {
                    validateBudgetedNumericCall(call);
                    return;
                }
                if (call.isImplicitThis()) {
                    if (closureMode || !LOG_METHODS.contains(method)) {
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

            private void validateBudgetedStringCall(MethodCallExpression call) {
                if (call.isImplicitThis()
                        || !(call.getArguments() instanceof ArgumentListExpression arguments)) {
                    reject(call, "string operations require an explicit receiver");
                    return;
                }
                String method = call.getMethodAsString();
                int size = arguments.getExpressions().size();
                if (("substring".equals(method) && (size < 1 || size > 2))
                        || (!"substring".equals(method) && size != 0)) {
                    reject(call, "string operation argument count is outside the allowlist");
                }
                validateReceiver(call.getObjectExpression());
                validateExpression(arguments);
            }

            private void validateBudgetedNumericCall(MethodCallExpression call) {
                if (call.isImplicitThis()
                        || !(call.getArguments() instanceof ArgumentListExpression arguments)
                        || !arguments.getExpressions().isEmpty()) {
                    reject(call, "numeric operations require an explicit receiver and no arguments");
                }
                validateReceiver(call.getObjectExpression());
            }

            private void validateCollectionCall(MethodCallExpression call) {
                if (closureMode || call.isImplicitThis()) {
                    reject(call, "nested or implicit collection dispatch is outside the allowlist");
                }
                validateReceiver(call.getObjectExpression());
                if (!(call.getArguments() instanceof ArgumentListExpression arguments)
                        || arguments.getExpressions().size() != 1) {
                    reject(call, "collection methods require exactly one controlled closure");
                    return;
                }
                Expression argument = arguments.getExpression(0);
                if (!(argument instanceof ClosureExpression)) {
                    reject(call, "collection methods require exactly one controlled closure");
                    return;
                }
                ClosureExpression closure = (ClosureExpression) argument;
                validateCollectionClosure(closure);
            }

            private void validateCollectionClosure(ClosureExpression closure) {
                org.codehaus.groovy.ast.Parameter[] parameters = closure.getParameters();
                String parameterName;
                if (!closure.isParameterSpecified()) {
                    parameterName = "it";
                } else if (parameters != null && parameters.length == 1
                        && parameters[0].getType().equals(ClassHelper.DYNAMIC_TYPE)
                        && !RESERVED_VARIABLES.contains(parameters[0].getName())) {
                    parameterName = parameters[0].getName();
                } else {
                    reject(closure, "collection closures accept one untyped element parameter");
                    return;
                }
                AllowedScriptAst body = new AllowedScriptAst(true, Set.of(parameterName), this);
                body.validateClosureBody(closure.getCode());
            }

            private void validateClosureBody(Statement statement) {
                if (statement instanceof BlockStatement block) {
                    if (block.getStatements().size() != 1) {
                        reject(block, "collection closures may only contain one scalar expression");
                    }
                    validateClosureBody(block.getStatements().getFirst());
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
                reject(statement, "collection closures may only contain one scalar expression");
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
