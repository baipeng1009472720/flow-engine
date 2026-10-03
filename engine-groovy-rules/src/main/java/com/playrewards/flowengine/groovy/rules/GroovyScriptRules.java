package com.playrewards.flowengine.groovy.rules;

import groovy.lang.GroovyClassLoader;
import groovy.transform.ThreadInterrupt;
import groovyjarjarantlr4.v4.runtime.CharStreams;
import groovyjarjarantlr4.v4.runtime.Token;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.groovy.parser.antlr4.GroovyLexer;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.CodeVisitorSupport;
import org.codehaus.groovy.ast.DynamicVariable;
import org.codehaus.groovy.ast.Parameter;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationUnit;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.codehaus.groovy.control.Phases;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.messages.ExceptionMessage;
import org.codehaus.groovy.control.messages.Message;
import org.codehaus.groovy.control.messages.SyntaxErrorMessage;
import org.codehaus.groovy.syntax.SyntaxException;

/**
 * 引擎 Groovy 沙箱规则的公共入口：平台设计态静态检查与引擎运行时编译共用同一套规则。
 *
 * <p>{@link #check(String, Set)} 只解析与做 AST 检查、不执行脚本，并收集全部违规；
 * {@link #enforce(ClassNode)} 与 {@link #enforceNoAnnotations(String)} 供运行时编译期失败快速使用，
 * 抛出的 SecurityException 文案是运行时既有对外文案。</p>
 *
 * <p>不负责：运行期预算、超时、数据类型约束（例如 Map 字面量键必须是字符串）、引用路径白名单。
 * 宿主比引擎更严的检查（危险类型、引用路径等）应在宿主侧叠加，不得放宽这里的结论。</p>
 */
public final class GroovyScriptRules {

    private static final String CHECK_SOURCE_NAME = "GroovyRuleCheck.groovy";

    private GroovyScriptRules() {
    }

    /**
     * 静态检查脚本，返回按发现顺序排列的全部违规；列表为空表示运行时编译会放行。
     *
     * <p>首条违规与运行时编译拒绝时报告的规则一致。含 @ 记号的脚本只返回注解违规且不编译，
     * 因为注解可在编译期触发本地 AST 变换。</p>
     *
     * @param source 脚本源码
     * @param externalScopes 宿主会注入的外部作用域根变量名（ctx 总会注入，可不列出）；
     *                       脚本引用集合外的根变量时报 {@link GroovyRuleCodes#VARIABLE_UNKNOWN}
     * @return 不可变违规列表
     * @throws NullPointerException source 或 externalScopes 为 null
     */
    public static List<GroovyRuleViolation> check(String source, Set<String> externalScopes) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(externalScopes, "externalScopes must not be null");
        List<GroovyRuleViolation> annotations;
        try {
            annotations = annotationViolations(source);
        } catch (RuntimeException lexerFailure) {
            // 词法失败交给编译阶段报告语法错误，避免同一问题出现两种诊断。
            annotations = List.of();
        }
        if (!annotations.isEmpty()) {
            return annotations;
        }
        return List.copyOf(compileAndInspect(source, externalScopes));
    }

    /**
     * 对宿主自行编译得到的类节点做规则检查（收集模式），供已有编译流程的宿主复用。
     *
     * @param classNode SEMANTIC_ANALYSIS 阶段的类节点；非脚本类节点报类声明违规
     * @return 不可变违规列表
     */
    public static List<GroovyRuleViolation> inspect(ClassNode classNode) {
        Objects.requireNonNull(classNode, "classNode must not be null");
        List<GroovyRuleViolation> violations = new ArrayList<>();
        ScriptAstValidator.validateClass(classNode, collecting(violations));
        return List.copyOf(violations);
    }

    /**
     * 运行时编译期失败快速检查：遇到首条违规即抛出。
     *
     * @param classNode SEMANTIC_ANALYSIS 阶段的类节点
     * @throws SecurityException 文案形如 {@code script sandbox rejected <原因> at line <行号>}
     */
    public static void enforce(ClassNode classNode) {
        Objects.requireNonNull(classNode, "classNode must not be null");
        ScriptAstValidator.validateClass(classNode, (code, message, sandboxReason, node) -> {
            throw new SecurityException("script sandbox rejected " + sandboxReason
                    + " at line " + Math.max(1, node.getLineNumber()));
        });
    }

    /**
     * 运行时编译前的注解预检：代码中出现任何 @ 记号即拒绝；字符串与注释中的 @ 不受影响。
     *
     * @param source 脚本源码
     * @throws SecurityException 发现注解或 .@ 字段访问
     */
    public static void enforceNoAnnotations(String source) {
        if (!annotationViolations(source).isEmpty()) {
            throw new SecurityException("annotations are not allowed by the script sandbox");
        }
    }

    /**
     * 用 Groovy 词法器定位代码中的全部 @ 记号。
     *
     * @param source 脚本源码
     * @return 每个 @ 记号一条 {@link GroovyRuleCodes#ANNOTATION_FORBIDDEN} 违规
     */
    public static List<GroovyRuleViolation> annotationViolations(String source) {
        GroovyLexer lexer = new GroovyLexer(CharStreams.fromString(source));
        lexer.removeErrorListeners();
        List<GroovyRuleViolation> violations = new ArrayList<>();
        for (Token token = lexer.nextToken(); token.getType() != Token.EOF; token = lexer.nextToken()) {
            if (token.getType() == GroovyLexer.AT) {
                int column = token.getCharPositionInLine() + 1;
                violations.add(new GroovyRuleViolation(GroovyRuleCodes.ANNOTATION_FORBIDDEN,
                        token.getLine(), column, token.getLine(), column + 1,
                        "脚本中禁止使用 @ 记号：不支持注解，也不支持 .@ 字段访问"));
            }
        }
        return violations;
    }

    /**
     * 按运行时同样的编译配置编译到字节码生成阶段（不加载类），收集 AST、作用域与编译器违规。
     * AST 规则已有违规时停在语义分析阶段：运行时在该阶段即拒绝，后续阶段的报错只会是噪音。
     */
    private static List<GroovyRuleViolation> compileAndInspect(String source, Set<String> externalScopes) {
        List<GroovyRuleViolation> astViolations = new ArrayList<>();
        Map<String, ASTNode> unknownRoots = new LinkedHashMap<>();
        CompilerConfiguration configuration = new CompilerConfiguration();
        configuration.addCompilationCustomizers(
                new ASTTransformationCustomizer(ThreadInterrupt.class),
                GroovyScriptPolicy.newSecureCustomizer(),
                new InspectingCustomizer(externalScopes, astViolations, unknownRoots));
        List<GroovyRuleViolation> compilerViolations = new ArrayList<>();
        try (GroovyClassLoader loader = new GroovyClassLoader(
                GroovyScriptRules.class.getClassLoader(), configuration)) {
            CompilationUnit unit = new CompilationUnit(configuration, null, loader);
            unit.addSource(CHECK_SOURCE_NAME, source);
            unit.compile(Phases.SEMANTIC_ANALYSIS);
            if (astViolations.isEmpty()) {
                unit.compile(Phases.CLASS_GENERATION);
            }
        } catch (MultipleCompilationErrorsException failure) {
            compilerViolations.addAll(compilerViolations(failure));
        } catch (IOException ignored) {
            // 关闭内存类加载器不涉及外部资源，失败不影响已收集的诊断。
        }
        List<GroovyRuleViolation> result = new ArrayList<>(astViolations);
        unknownRoots.forEach((name, node) -> result.add(violation(GroovyRuleCodes.VARIABLE_UNKNOWN,
                "变量 " + name + " 不存在：外部数据只能经 ctx 或已注入的作用域读取，局部变量请先用 def 声明",
                node)));
        result.addAll(compilerViolations);
        return result;
    }

    private static List<GroovyRuleViolation> compilerViolations(MultipleCompilationErrorsException failure) {
        List<GroovyRuleViolation> result = new ArrayList<>();
        for (Message message : failure.getErrorCollector().getErrors()) {
            if (message instanceof SyntaxErrorMessage syntaxMessage) {
                SyntaxException syntax = syntaxMessage.getCause();
                result.add(new GroovyRuleViolation(
                        isSecurityFailure(syntax) ? GroovyRuleCodes.SECURITY_VIOLATION : GroovyRuleCodes.COMPILE_ERROR,
                        syntax.getStartLine(), syntax.getStartColumn(),
                        syntax.getEndLine(), syntax.getEndColumn(),
                        syntax.getOriginalMessage()));
            } else if (message instanceof ExceptionMessage exceptionMessage) {
                Exception cause = exceptionMessage.getCause();
                result.add(new GroovyRuleViolation(
                        isSecurityFailure(cause) ? GroovyRuleCodes.SECURITY_VIOLATION : GroovyRuleCodes.COMPILE_ERROR,
                        1, 1, 1, 1, cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage()));
            } else {
                StringWriter writer = new StringWriter();
                message.write(new PrintWriter(writer));
                String text = writer.toString().trim();
                result.add(new GroovyRuleViolation(
                        isSecurityMessage(text) ? GroovyRuleCodes.SECURITY_VIOLATION : GroovyRuleCodes.COMPILE_ERROR,
                        1, 1, 1, 1, text.isBlank() ? "脚本编译失败" : text));
            }
        }
        if (result.isEmpty()) {
            result.add(new GroovyRuleViolation(GroovyRuleCodes.COMPILE_ERROR, 1, 1, 1, 1, "脚本编译失败"));
        }
        return result;
    }

    /** 与运行时识别沙箱拒绝的口径一致：SecurityException 或“not allowed”类编译器安全消息。 */
    private static boolean isSecurityFailure(Throwable failure) {
        for (Throwable current = failure; current != null;
                current = current.getCause() == current ? null : current.getCause()) {
            if (current instanceof SecurityException || isSecurityMessage(current.getMessage())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSecurityMessage(String message) {
        return message != null && (message.contains("script sandbox") || message.contains("not allowed"));
    }

    private static ScriptAstValidator.Sink collecting(List<GroovyRuleViolation> violations) {
        return (code, message, sandboxReason, node) -> violations.add(violation(code, message, node));
    }

    private static GroovyRuleViolation violation(String code, String message, ASTNode node) {
        return new GroovyRuleViolation(code, node.getLineNumber(), node.getColumnNumber(),
                node.getLastLineNumber(), node.getLastColumnNumber(), message);
    }

    /** 语义分析阶段执行 AST 规则（收集模式）并记录未注入的外部根变量。 */
    private static final class InspectingCustomizer extends CompilationCustomizer {

        private final Set<String> readableRoots;
        private final List<GroovyRuleViolation> violations;
        private final Map<String, ASTNode> unknownRoots;

        private InspectingCustomizer(Set<String> externalScopes, List<GroovyRuleViolation> violations,
                Map<String, ASTNode> unknownRoots) {
            super(CompilePhase.SEMANTIC_ANALYSIS);
            Set<String> roots = new HashSet<>(externalScopes);
            roots.add(GroovyScriptPolicy.CONTEXT_ROOT);
            this.readableRoots = Set.copyOf(roots);
            this.violations = violations;
            this.unknownRoots = unknownRoots;
        }

        @Override
        public void call(SourceUnit source, GeneratorContext context, ClassNode classNode) {
            ScriptAstValidator.validateClass(classNode, collecting(violations));
            var run = classNode.isScript() ? classNode.getMethod("run", Parameter.EMPTY_ARRAY) : null;
            if (run == null || run.getCode() == null) {
                return;
            }
            run.getCode().visit(new CodeVisitorSupport() {
                @Override
                public void visitVariableExpression(VariableExpression expression) {
                    String name = expression.getName();
                    // 保留名已由 AST 规则报告；每个未知根只报首次出现位置。
                    if (expression.getAccessedVariable() instanceof DynamicVariable
                            && !readableRoots.contains(name)
                            && !GroovyScriptPolicy.RESERVED_VARIABLES.contains(name)) {
                        unknownRoots.putIfAbsent(name, expression);
                    }
                    super.visitVariableExpression(expression);
                }
            });
        }
    }
}
