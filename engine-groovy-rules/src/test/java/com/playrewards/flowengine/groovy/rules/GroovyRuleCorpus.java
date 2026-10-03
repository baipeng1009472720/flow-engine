package com.playrewards.flowengine.groovy.rules;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Groovy 脚本规则对照语料：放行写法与带“首条违规码 + 行号”的拒绝写法。
 *
 * <p>语料来自平台设计态与引擎运行时沙箱的逐项实测对照（原平台 GroovyScriptRuntimeParityTest），
 * 由本模块的规则测试与 engine-adapters 的“静态检查 / 运行时编译一致”守护测试共同使用。
 * 新增或调整规则时必须同步补充语料，否则两侧漂移无法被发现。</p>
 *
 * <p>不负责：运行期才能判定的数据类型约束（例如对字符串调用集合方法、Map 字面量常量非字符串键），
 * 这些写法静态检查与运行时编译都放行，由运行时执行阶段拒绝。</p>
 */
public final class GroovyRuleCorpus {

    /** 语料脚本可读取的外部作用域；ctx 由运行时总是注入，不必列出。 */
    public static final Set<String> EXTERNAL_SCOPES = Set.of("play");

    private GroovyRuleCorpus() {
    }

    /**
     * 期望被拒绝的脚本。
     *
     * @param script 脚本源码
     * @param code 首条违规的稳定规则码
     * @param line 首条违规所在行（1 起）
     */
    public record Rejected(String script, String code, int line) {
    }

    /** @return 静态检查与运行时编译都必须放行的脚本 */
    public static List<String> accepted() {
        return List.of(
                "return ctx.entry.request.state.drawsToday",
                "def s = ctx.entry.request.state\nreturn s['drawsToday']",
                "def s = ctx.entry.request.state\nreturn s.get('drawsToday')",
                "def s = ctx.entry.request.state\nreturn s.containsKey('a')",
                "return [a: 1]['a']",
                "def m = [a: [b: 1]]\nreturn m['a']['b']",
                "return ctx.entry.request.state?.a",
                "return ctx.entry.request.state.list[0]",
                "return play.config.x",
                "return ctx.entry.request.state.items.collect { it.price }",
                "def items = ctx.entry.request.state.items\nreturn items.collect { item -> item.price }",
                "def items = [1, 2]\nreturn items.collect { item -> item * 2 }",
                "return ctx.entry.request.state.items.find { it.id == ctx.entry.request.payload.id }",
                "return ctx.entry.request.state.list.findAll { it > 1 }.size()",
                "return ctx.entry.request.state.list.collect { it > 1 ? 'big' : 'small' }",
                "return ctx.entry.request.state.items.collect { [id: it.id] }",
                "return ctx.entry.request.state.items.collect { it['price'] }",
                "def a = 1\nreturn [1].collect { it + a }",
                "def i = 0\nwhile (i < 3) { i = i + 1 }\nreturn i",
                "def b = true\nwhile (b) { b = false\n break }\nreturn b",
                "def a = 1\na = a + 1\nreturn a",
                "if (ctx.entry.request.state.a) return 1\nreturn 2",
                "return \"x${ctx.entry.request.today}\"",
                "return 'abc'.length()",
                "return 'abc'.toUpperCase()",
                "return ctx.entry.request.state.s.substring(1, 2)",
                "return ctx.entry.request.state.s.trim().startsWith('a')",
                "return ctx.entry.request.state.n.abs()",
                "def x = -1\nreturn x.abs()",
                "return [1, 2].size()",
                "return [1, 2].max()",
                "def l = [1]\nreturn l + [2]",
                "def m = [:]\nreturn m + [a: 1]",
                "def m = [(ctx.entry.request.today): 1]\nreturn m",
                // 常量非字符串键：编译期放行，运行时构造 Map 时拒绝；平台设计态另有更严检查。
                "def m = [1: 'a']\nreturn m",
                "return 'a' * 2",
                "return 10 / 3",
                "return 5 % 2",
                "return !ctx.entry.request.state.a",
                "return -1",
                "def a = 1L\ndef b = 1.5\nreturn [a, b, null, true]",
                "println 'x'\nreturn 1",
                "def a = 1\nreturn a == 1 && !(a > 2) || false",
                "return ctx.entry.request.state.a ?: ctx.entry.request.state.b",
                "// 注释里的 @ 与字符串里的 @ 不是注解\nreturn 'a@b'",
                """
                // 用途：每日签到
                def lastDate = ctx.entry.request.state.lastDate ?: ''
                def streak = ctx.entry.request.state.streak ?: 0
                def signedToday = lastDate == ctx.entry.request.today
                def newStreak = signedToday ? streak : (lastDate == ctx.entry.request.yesterday ? streak + 1 : 1)
                def points = signedToday ? 0 : (play.config.dailyPoints ?: 10)
                return [
                  state: [lastDate: ctx.entry.request.today, streak: newStreak],
                  viewModel: [streak: newStreak, signedToday: true, gainedPoints: points],
                  effect: signedToday ? [:] : [points: points]
                ]
                """,
                drawScript().replaceAll(
                        "state\\.(drawsToday|lastDrawDate|totalDraws|lastResult|lastRewardId)", "state['$1']"));
    }

    /** @return 静态检查与运行时编译都必须拒绝的脚本，附首条违规码与行号 */
    public static List<Rejected> rejected() {
        return List.of(
                // 局部变量、字面量或方法结果上的点号取属性
                r("def s = ctx.entry.request.state\nreturn s.drawsToday", "GROOVY_LOCAL_PROPERTY_ACCESS", 2),
                r("def m = [a: 1]\nreturn m.a", "GROOVY_LOCAL_PROPERTY_ACCESS", 2),
                r("def a = [b: 1]\nreturn a?.b", "GROOVY_LOCAL_PROPERTY_ACCESS", 2),
                r("def p = play.config\nreturn p.x", "GROOVY_LOCAL_PROPERTY_ACCESS", 2),
                r("def r = ctx.entry.request\nreturn r.state.a", "GROOVY_LOCAL_PROPERTY_ACCESS", 2),
                r("return [a: 1].a", "GROOVY_LOCAL_PROPERTY_ACCESS", 1),
                r("return ctx.entry.request.state.items.find { it.id == 1 }.price",
                        "GROOVY_LOCAL_PROPERTY_ACCESS", 1),
                r("def items = ctx.entry.request.state.items\ndef hit = items.find { it.id == 1 }\nreturn hit.price",
                        "GROOVY_LOCAL_PROPERTY_ACCESS", 3),
                r(drawScript(), "GROOVY_LOCAL_PROPERTY_ACCESS", 5),
                // 特殊属性形式
                r("return ctx.entry.request.state.\"${'a'}\"", "GROOVY_PROPERTY_FORBIDDEN", 1),
                r("return ctx.entry.request.state.class", "GROOVY_PROPERTY_FORBIDDEN", 1),
                r("return ctx.entry.request.state.metaClass", "GROOVY_PROPERTY_FORBIDDEN", 1),
                r("return ctx.entry.request.state.items*.id", "GROOVY_PROPERTY_FORBIDDEN", 1),
                // 语句白名单：for / do-while / try / switch / throw / assert / synchronized / 类定义
                r("def t = 0\nfor (i in [1, 2]) { t = t + i }\nreturn t", "GROOVY_STATEMENT_FORBIDDEN", 2),
                r("def t = 0\nfor (int i = 0; i < 3; i++) { t = t + 1 }\nreturn t", "GROOVY_STATEMENT_FORBIDDEN", 2),
                r("do { } while (false)\nreturn 1", "GROOVY_STATEMENT_FORBIDDEN", 1),
                r("try { return 1 } catch (e) { return 2 }", "GROOVY_STATEMENT_FORBIDDEN", 1),
                r("switch (1) { case 1: return 1 }", "GROOVY_STATEMENT_FORBIDDEN", 1),
                r("throw new Exception('x')", "GROOVY_STATEMENT_FORBIDDEN", 1),
                r("assert true\nreturn 1", "GROOVY_STATEMENT_FORBIDDEN", 1),
                r("synchronized (ctx) { }\nreturn 1", "GROOVY_STATEMENT_FORBIDDEN", 1),
                r("class A {}\nreturn 1", "GROOVY_STATEMENT_FORBIDDEN", 1),
                // 集合修改与赋值目标
                r("def l = []\nl.add(1)\nreturn l", "GROOVY_METHOD_FORBIDDEN", 2),
                r("def m = [:]\nm.put('a', 1)\nreturn m", "GROOVY_METHOD_FORBIDDEN", 2),
                r("def l = []\nl << 1\nreturn l", "GROOVY_OPERATOR_FORBIDDEN", 2),
                r("def m = [:]\nm.a = 1\nreturn m", "GROOVY_ASSIGNMENT_FORBIDDEN", 2),
                r("def m = [:]\nm['a'] = 1\nreturn m", "GROOVY_ASSIGNMENT_FORBIDDEN", 2),
                r("def a = 1\na += 1\nreturn a", "GROOVY_OPERATOR_FORBIDDEN", 2),
                r("def a = 1\ndef b = a++\nreturn b", "GROOVY_EXPRESSION_FORBIDDEN", 2),
                // 转型、构造、区间、方法引用、类型声明等表达式
                r("return '1' as Integer", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return (Integer) 1", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return new Date()", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return (1..3)", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return ~1", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return 'a'.&size", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("int a = 1\nreturn a", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("String s = 'x'\nreturn s", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("def (a, b) = [1, 2]\nreturn a", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("def a = ctx.entry.request.state.a\nif (a) { def x = 1 } else { def x = 2 }\nreturn a",
                        "GROOVY_EXPRESSION_FORBIDDEN", 2),
                r("def out = 1\nreturn out", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return binding", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                r("return this", "GROOVY_EXPRESSION_FORBIDDEN", 1),
                // 运算符白名单
                r("return ctx.entry.request.state.s ==~ /x/", "GROOVY_OPERATOR_FORBIDDEN", 1),
                r("return ctx.entry.request.state.a instanceof Map", "GROOVY_OPERATOR_FORBIDDEN", 1),
                r("def a = 1\ndef b = 2\nreturn a <=> b", "GROOVY_OPERATOR_FORBIDDEN", 3),
                r("return ctx.entry.request.state.a in [1, 2]", "GROOVY_OPERATOR_FORBIDDEN", 1),
                r("return 1 & 2", "GROOVY_OPERATOR_FORBIDDEN", 1),
                r("return 1 | 2", "GROOVY_OPERATOR_FORBIDDEN", 1),
                r("return 2 ** 3", "GROOVY_OPERATOR_FORBIDDEN", 1),
                // 方法白名单与宿主隐式调用
                r("return ctx.entry.request.today.toString()", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return Integer.parseInt('1')", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return Math.max(1, 2)", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.today.split('-')", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.s.toInteger()", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.s.replace('a', 'b')", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.sum()", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.join(',')", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.each { it }", "GROOVY_METHOD_FORBIDDEN", 1),
                r("def m = ctx.entry.request.state\nreturn m.keySet()", "GROOVY_METHOD_FORBIDDEN", 2),
                r("def a = 1\nreturn a.equals(1)", "GROOVY_METHOD_FORBIDDEN", 2),
                r("return 5.intdiv(2)", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return [1, 2]*.abs()", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return sleep(1)", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return evaluate('1')", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.s.substring()", "GROOVY_METHOD_FORBIDDEN", 1),
                // 集合闭包形态
                r("def c = { it }\nreturn 1", "GROOVY_CLOSURE_FORBIDDEN", 1),
                r("return ctx.entry.request.state.items.collect { it.collect { x -> x } }",
                        "GROOVY_CLOSURE_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.collect { def y = it\n y }", "GROOVY_CLOSURE_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.collect { if (it) { 1 } }", "GROOVY_CLOSURE_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.collect { println it }", "GROOVY_METHOD_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.collect { a, b -> a }", "GROOVY_CLOSURE_FORBIDDEN", 1),
                r("return ctx.entry.request.state.list.collect { int x -> x }", "GROOVY_CLOSURE_FORBIDDEN", 1),
                r("def a = 1\na = 2\nreturn [1].collect { it + a }", "GROOVY_CLOSURE_FORBIDDEN", 3),
                r("def a = 1\ndef r = [1].collect { it + a }\na = 2\nreturn r", "GROOVY_CLOSURE_FORBIDDEN", 3),
                // 注解与 .@ 字段访问（运行时在词法阶段拒绝任何代码中的 @ 记号）
                r("@SuppressWarnings('x')\ndef a = 1\nreturn a", "GROOVY_ANNOTATION_FORBIDDEN", 1),
                r("def a = 1\nreturn ctx.entry.request.state.@a", "GROOVY_ANNOTATION_FORBIDDEN", 2),
                // 编译器安全策略（import、方法定义）与语法错误
                r("import java.io.File\nreturn 1", "GROOVY_SECURITY_VIOLATION", 1),
                r("def f() { 1 }\nreturn 1", "GROOVY_SECURITY_VIOLATION", 1),
                r("return (", "GROOVY_COMPILE_ERROR", 1));
    }

    /** @return AI 生成的九宫格抽奖脚本：第 5、6、8、11 行对局部变量 state 用点号取属性 */
    public static String drawScript() {
        try (InputStream input = GroovyRuleCorpus.class.getResourceAsStream(
                "/groovy-rules/ai-draw-local-property.groovy")) {
            if (input == null) {
                throw new IllegalStateException("测试资源不存在: ai-draw-local-property.groovy");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static Rejected r(String script, String code, int line) {
        return new Rejected(script, code, line);
    }
}
