package com.playrewards.flowengine.adapter.groovy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.playrewards.flowengine.groovy.rules.GroovyRuleCodes;
import com.playrewards.flowengine.groovy.rules.GroovyRuleCorpus;
import com.playrewards.flowengine.groovy.rules.GroovyRuleViolation;
import com.playrewards.flowengine.groovy.rules.GroovyScriptRules;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 守护测试：共享规则的静态检查 {@link GroovyScriptRules#check} 与 {@link SandboxedGroovyRuntime}
 * 编译期的拒绝/放行结论逐条一致，且 AST 规则拒绝时首条违规行号与运行时报错行号相同。
 *
 * <p>任何一侧单独修改规则都会让本测试失败，从而避免“设计态校验通过、试运行被沙箱拒绝”的漂移。
 * 只比较编译期结论；运行期数据类型约束不在静态检查范围内。</p>
 */
class GroovyRulesRuntimeParityTest {

    private static final Pattern RUNTIME_RULE_LINE =
            Pattern.compile("script sandbox rejected .+ at line (\\d+)");

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("corpusScripts")
    void 静态检查与运行时编译结论一致(String script) {
        List<GroovyRuleViolation> violations = GroovyScriptRules.check(script, GroovyRuleCorpus.EXTERNAL_SCOPES);
        String runtimeFailure = runtimeCompileFailure(script);

        assertEquals(runtimeFailure != null, !violations.isEmpty(),
                script + "\nruntime=" + runtimeFailure + "\ncheck=" + violations);
        if (runtimeFailure == null) {
            return;
        }
        GroovyRuleViolation first = violations.getFirst();
        assertFalse(first.code().equals(GroovyRuleCodes.VARIABLE_UNKNOWN), script + " -> " + violations);
        Matcher matcher = RUNTIME_RULE_LINE.matcher(runtimeFailure);
        if (matcher.find()) {
            assertEquals(Integer.parseInt(matcher.group(1)), first.line(),
                    script + "\nruntime=" + runtimeFailure + "\ncheck=" + violations);
        }
    }

    static Stream<String> corpusScripts() {
        return Stream.concat(GroovyRuleCorpus.accepted().stream(),
                GroovyRuleCorpus.rejected().stream().map(GroovyRuleCorpus.Rejected::script));
    }

    /** @return 运行时编译拒绝时的完整异常文本（含 cause 链），放行时返回 null */
    private static String runtimeCompileFailure(String script) {
        try {
            SandboxedGroovyRuntime.compileForVerification(script);
            return null;
        } catch (RuntimeException failure) {
            StringBuilder text = new StringBuilder();
            for (Throwable current = failure; current != null;
                    current = current.getCause() == current ? null : current.getCause()) {
                text.append(current.getMessage()).append('\n');
            }
            return text.toString();
        }
    }
}
