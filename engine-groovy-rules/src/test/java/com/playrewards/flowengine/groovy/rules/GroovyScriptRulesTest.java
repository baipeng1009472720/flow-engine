package com.playrewards.flowengine.groovy.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 验证 {@link GroovyScriptRules#check(String, Set)} 的稳定规则码、行列号与“收集全部违规”语义。
 * 语料与运行时编译的一致性由 engine-adapters 的守护测试另行保证。
 */
class GroovyScriptRulesTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("acceptedScripts")
    void 运行时可执行的写法不报违规(String script) {
        assertEquals(List.of(), GroovyScriptRules.check(script, GroovyRuleCorpus.EXTERNAL_SCOPES), script);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("rejectedScripts")
    void 运行时会拒绝的写法报对应规则码与行号(GroovyRuleCorpus.Rejected sample) {
        List<GroovyRuleViolation> violations =
                GroovyScriptRules.check(sample.script(), GroovyRuleCorpus.EXTERNAL_SCOPES);

        assertFalse(violations.isEmpty(), sample.script());
        GroovyRuleViolation first = violations.getFirst();
        assertEquals(sample.code(), first.code(), sample.script() + " -> " + violations);
        assertEquals(sample.line(), first.line(), sample.script() + " -> " + violations);
        assertTrue(first.column() >= 1, sample.script() + " -> " + violations);
        assertFalse(first.message().isBlank(), sample.script() + " -> " + violations);
    }

    @Test
    void 抽奖脚本收集全部局部属性违规并给出下标改写建议() {
        List<GroovyRuleViolation> violations =
                GroovyScriptRules.check(GroovyRuleCorpus.drawScript(), GroovyRuleCorpus.EXTERNAL_SCOPES);

        GroovyRuleViolation first = violations.getFirst();
        assertEquals(GroovyRuleCodes.LOCAL_PROPERTY_ACCESS, first.code());
        assertEquals(5, first.line());
        assertTrue(first.message().contains("state.drawsToday"), first.message());
        assertTrue(first.message().contains("state['drawsToday']"), first.message());
        assertEquals(Set.of(5, 6, 8, 11), Set.copyOf(violations.stream()
                .filter(item -> item.code().equals(GroovyRuleCodes.LOCAL_PROPERTY_ACCESS))
                .map(GroovyRuleViolation::line)
                .toList()));
    }

    @Test
    void 不在第一处违规停止而是按出现顺序收集() {
        List<GroovyRuleViolation> violations = GroovyScriptRules.check(
                "def a = [b: 1]\ndef x = a.b\ndef l = []\nl.add(1)\nreturn 1 & 2", Set.of());

        assertEquals(List.of(
                        GroovyRuleCodes.LOCAL_PROPERTY_ACCESS,
                        GroovyRuleCodes.METHOD_FORBIDDEN,
                        GroovyRuleCodes.OPERATOR_FORBIDDEN),
                violations.stream().map(GroovyRuleViolation::code).toList());
        assertEquals(List.of(2, 4, 5), violations.stream().map(GroovyRuleViolation::line).toList());
    }

    @Test
    void 未注入的外部作用域报变量不存在而ctx总可读取() {
        List<GroovyRuleViolation> violations =
                GroovyScriptRules.check("return ctx.a ?: play.config.b ?: vars.c", Set.of("play"));

        assertEquals(1, violations.size(), violations::toString);
        GroovyRuleViolation unknown = violations.getFirst();
        assertEquals(GroovyRuleCodes.VARIABLE_UNKNOWN, unknown.code());
        assertEquals(1, unknown.line());
        assertTrue(unknown.message().contains("vars"), unknown.message());
        assertEquals(List.of(), GroovyScriptRules.check("return vars.c", Set.of("vars")));
    }

    @Test
    void 注解在编译前拒绝且收集每个记号位置() {
        List<GroovyRuleViolation> violations =
                GroovyScriptRules.check("@Grab('x')\ndef a = 1\nreturn ctx.@a", Set.of());

        assertEquals(List.of(GroovyRuleCodes.ANNOTATION_FORBIDDEN, GroovyRuleCodes.ANNOTATION_FORBIDDEN),
                violations.stream().map(GroovyRuleViolation::code).toList());
        assertEquals(List.of(1, 3), violations.stream().map(GroovyRuleViolation::line).toList());
        assertEquals(1, violations.getFirst().column());
    }

    @Test
    void 空脚本与空作用域参数被拒绝() {
        assertThrows(NullPointerException.class, () -> GroovyScriptRules.check(null, Set.of()));
        assertThrows(NullPointerException.class, () -> GroovyScriptRules.check("return 1", null));
    }

    static Stream<String> acceptedScripts() {
        return GroovyRuleCorpus.accepted().stream();
    }

    static Stream<GroovyRuleCorpus.Rejected> rejectedScripts() {
        return GroovyRuleCorpus.rejected().stream();
    }
}
