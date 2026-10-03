package com.playrewards.flowengine.groovy.rules;

import java.util.Objects;

/**
 * 一条脚本规则违规：稳定规则码、源码位置（1 起的行列号，含结束位置便于编辑器标注）与中文说明。
 *
 * <p>message 面向运营与 AI 修复，只包含脚本自身的标识符与规则说明，不包含宿主类名或输入数据。</p>
 *
 * @param code 稳定规则码，见 {@link GroovyRuleCodes}
 * @param line 起始行（1 起）
 * @param column 起始列（1 起）
 * @param endLine 结束行（1 起）
 * @param endColumn 结束列（1 起）
 * @param message 中文违规说明
 */
public record GroovyRuleViolation(
        String code, int line, int column, int endLine, int endColumn, String message) {

    /** 校验规则码与说明非空，位置统一收敛到不小于 1，避免下游编辑器标注越界。 */
    public GroovyRuleViolation {
        Objects.requireNonNull(code, "code must not be null");
        Objects.requireNonNull(message, "message must not be null");
        line = Math.max(1, line);
        column = Math.max(1, column);
        endLine = Math.max(line, endLine);
        endColumn = endLine == line ? Math.max(column, endColumn) : Math.max(1, endColumn);
    }
}
