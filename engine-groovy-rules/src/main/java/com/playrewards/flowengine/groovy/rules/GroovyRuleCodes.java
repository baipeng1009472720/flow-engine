package com.playrewards.flowengine.groovy.rules;

/**
 * Groovy 脚本静态规则的稳定规则码，是引擎与平台之间的对外契约。
 *
 * <p>平台按规则码映射中文修复建议，因此已发布的码值不得改名或改变含义；新增规则只能追加新码。
 * 本类只定义码值，不负责判定规则或生成提示文案。</p>
 */
public final class GroovyRuleCodes {

    /** 对局部变量、字面量或方法结果用点号取属性（运行时只允许从外部 JSON 根沿路径取值）。 */
    public static final String LOCAL_PROPERTY_ACCESS = "GROOVY_LOCAL_PROPERTY_ACCESS";
    /** .@、*.、动态属性名或 class/metaClass 等保留属性。 */
    public static final String PROPERTY_FORBIDDEN = "GROOVY_PROPERTY_FORBIDDEN";
    /** 白名单外的语句（for、try、switch、throw、类定义等）。 */
    public static final String STATEMENT_FORBIDDEN = "GROOVY_STATEMENT_FORBIDDEN";
    /** 白名单外的表达式（as/强制转型、new、区间、自增、类型声明、保留变量名等）。 */
    public static final String EXPRESSION_FORBIDDEN = "GROOVY_EXPRESSION_FORBIDDEN";
    /** 白名单外的运算符。 */
    public static final String OPERATOR_FORBIDDEN = "GROOVY_OPERATOR_FORBIDDEN";
    /** 给未声明的变量、Map/List 元素或属性赋值。 */
    public static final String ASSIGNMENT_FORBIDDEN = "GROOVY_ASSIGNMENT_FORBIDDEN";
    /** 白名单外的方法、宿主隐式调用或非 JSON 接收者。 */
    public static final String METHOD_FORBIDDEN = "GROOVY_METHOD_FORBIDDEN";
    /** 集合闭包形态不合规（独立闭包、嵌套、多参数、可变捕获等）。 */
    public static final String CLOSURE_FORBIDDEN = "GROOVY_CLOSURE_FORBIDDEN";
    /** 代码中出现 @ 记号（注解或 .@ 字段访问），编译前即拒绝。 */
    public static final String ANNOTATION_FORBIDDEN = "GROOVY_ANNOTATION_FORBIDDEN";
    /** 编译器安全策略拒绝（import、方法定义、package 等）。 */
    public static final String SECURITY_VIOLATION = "GROOVY_SECURITY_VIOLATION";
    /** 语法或编译错误。 */
    public static final String COMPILE_ERROR = "GROOVY_COMPILE_ERROR";
    /** 引用了宿主不会注入的外部作用域根变量（运行时会报缺失作用域）。 */
    public static final String VARIABLE_UNKNOWN = "GROOVY_VARIABLE_UNKNOWN";

    private GroovyRuleCodes() {
    }
}
