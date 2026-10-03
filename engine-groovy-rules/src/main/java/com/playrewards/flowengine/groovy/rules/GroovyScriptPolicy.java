package com.playrewards.flowengine.groovy.rules;

import java.util.List;
import java.util.Set;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.codehaus.groovy.syntax.Types;

/**
 * 引擎 Groovy 沙箱的白名单定义：运行时编译、AST 改写与静态检查引用同一份常量。
 *
 * <p>集合均为不可变集合。放宽任何一项都会同时改变运行时可执行范围与平台设计态校验结果，
 * 必须经过安全评审并同步补充对照语料。本类不负责运行期预算、超时或结果规整。</p>
 */
public final class GroovyScriptPolicy {

    /** 运行时向脚本注入外部 JSON 根的内部绑定名；脚本不得直接引用。 */
    public static final String INTERNAL_JSON_ROOTS = "__flowJsonRoots";
    /** 运行时总是注入的外部作用域根变量。 */
    public static final String CONTEXT_ROOT = "ctx";
    /** 无副作用、按显式接收者调用的方法。 */
    public static final Set<String> ALLOWED_METHODS = Set.of(
            "contains", "containsKey", "containsValue", "endsWith", "get", "isEmpty",
            "length", "max", "min", "size", "startsWith");
    /** 只接受一个受控闭包的集合方法。 */
    public static final Set<String> COLLECTION_METHODS = Set.of(
            "any", "collect", "every", "find", "findAll");
    /** 运行时按预算执行的字符串方法。 */
    public static final Set<String> STRING_METHODS = Set.of(
            "strip", "substring", "toLowerCase", "toUpperCase", "trim");
    /** 运行时按预算执行的数值方法。 */
    public static final Set<String> NUMERIC_METHODS = Set.of("abs");
    /** 仅允许隐式调用的日志方法。 */
    public static final Set<String> LOG_METHODS = Set.of("print", "println");
    /** 脚本宿主保留变量名，既不能读取也不能用作局部变量或闭包参数名。 */
    public static final Set<String> RESERVED_VARIABLES = Set.of(
            "this", "super", "owner", "delegate", "binding", "out", "metaClass",
            INTERNAL_JSON_ROOTS);
    /** 可能暴露宿主对象或反射能力的保留属性名。 */
    public static final Set<String> RESERVED_PROPERTIES = Set.of(
            "class", "metaClass", "classLoader", "declaredFields", "declaredMethods",
            "fields", "methods", "protectionDomain", "binding", "properties",
            "bytes", "chars", "codePoints");
    /** 允许的二元运算符记号（含下标 []）。 */
    public static final Set<Integer> ALLOWED_BINARY_TOKENS = Set.of(
            Types.ASSIGN,
            Types.COMPARE_EQUAL, Types.COMPARE_NOT_EQUAL,
            Types.COMPARE_LESS_THAN, Types.COMPARE_LESS_THAN_EQUAL,
            Types.COMPARE_GREATER_THAN, Types.COMPARE_GREATER_THAN_EQUAL,
            Types.LOGICAL_AND, Types.LOGICAL_OR,
            Types.PLUS, Types.MINUS, Types.MULTIPLY, Types.DIVIDE, Types.INTDIV, Types.MOD,
            Types.LEFT_SQUARE_BRACKET);

    private GroovyScriptPolicy() {
    }

    /**
     * 创建与运行时一致的编译器安全策略：禁止方法定义、package 与任何 import。
     *
     * <p>闭包语法放行只是为了让 AST 规则识别五种集合闭包；其余闭包形态仍由 AST 规则拒绝。
     * 间接 import 检查关闭，因为它会误拒 Groovy 自身生成的 Object/ThreadInterrupt 代码。
     * SecureASTCustomizer 有状态，每次编译必须新建。</p>
     *
     * @return 新的编译器安全策略实例
     */
    public static SecureASTCustomizer newSecureCustomizer() {
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
}
