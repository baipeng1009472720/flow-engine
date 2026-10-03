package com.playrewards.flowengine.groovy.rules;

import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.ASSIGNMENT_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.CLOSURE_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.EXPRESSION_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.LOCAL_PROPERTY_ACCESS;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.METHOD_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.OPERATOR_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.PROPERTY_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyRuleCodes.STATEMENT_FORBIDDEN;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.ALLOWED_BINARY_TOKENS;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.ALLOWED_METHODS;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.COLLECTION_METHODS;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.LOG_METHODS;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.NUMERIC_METHODS;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.RESERVED_PROPERTIES;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.RESERVED_VARIABLES;
import static com.playrewards.flowengine.groovy.rules.GroovyScriptPolicy.STRING_METHODS;

import java.util.HashSet;
import java.util.Set;
import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.ClassHelper;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.Parameter;
import org.codehaus.groovy.ast.expr.ArgumentListExpression;
import org.codehaus.groovy.ast.expr.AttributeExpression;
import org.codehaus.groovy.ast.expr.BinaryExpression;
import org.codehaus.groovy.ast.expr.BooleanExpression;
import org.codehaus.groovy.ast.expr.ClosureExpression;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.DeclarationExpression;
import org.codehaus.groovy.ast.expr.EmptyExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.GStringExpression;
import org.codehaus.groovy.ast.expr.ListExpression;
import org.codehaus.groovy.ast.expr.MapEntryExpression;
import org.codehaus.groovy.ast.expr.MapExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.NotExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
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
import org.codehaus.groovy.syntax.Types;

/**
 * 封闭世界 AST 校验器：只放行这里列出的语句、表达式、运算符与方法，其余一律违规。
 *
 * <p>运行时（失败快速，首条违规抛 SecurityException）与静态检查（收集全部违规）共用本访问器，
 * 通过 {@link Sink} 区分两种模式。遍历顺序决定“首条违规”，因此收集模式下的第一条违规
 * 必须等于运行时抛出的那一条；修改分支顺序或在违规后继续遍历的方式时要保持这一点。</p>
 *
 * <p>每个拒绝点同时给出稳定规则码、中文说明（静态检查用）和英文沙箱原因（运行时错误文案用）。
 * 英文原因是运行时既有对外文案，不得改动。本类不负责 AST 改写、运行期预算或数据类型判定。</p>
 */
final class ScriptAstValidator {

    /** 违规接收方：收集模式追加违规，失败快速模式直接抛出。 */
    interface Sink {
        /**
         * 报告一条违规。
         *
         * @param code 稳定规则码
         * @param message 中文说明
         * @param sandboxReason 运行时沙箱英文原因（不含行号）
         * @param node 违规 AST 节点
         */
        void reject(String code, String message, String sandboxReason, ASTNode node);
    }

    private final Sink sink;
    private final Set<String> localVariables = new HashSet<>();
    private final Set<String> assignedLocalVariables = new HashSet<>();
    private final Set<String> capturedLocalVariables = new HashSet<>();
    private final boolean closureMode;
    private final Set<String> closureParameters;
    private final ScriptAstValidator outerScope;

    private ScriptAstValidator(Sink sink, boolean closureMode, Set<String> closureParameters,
            ScriptAstValidator outerScope) {
        this.sink = sink;
        this.closureMode = closureMode;
        this.closureParameters = closureParameters;
        this.outerScope = outerScope;
    }

    /**
     * 校验一个编译单元中的类节点：脚本类校验其 run 方法体，其它类声明直接违规。
     *
     * @param classNode SEMANTIC_ANALYSIS 阶段的类节点
     * @param sink 违规接收方
     */
    static void validateClass(ClassNode classNode, Sink sink) {
        if (!classNode.isScript()) {
            sink.reject(STATEMENT_FORBIDDEN, "禁止在脚本中定义类",
                    "class declarations are outside the allowed AST", classNode);
            return;
        }
        var run = classNode.getMethod("run", Parameter.EMPTY_ARRAY);
        if (run == null || run.getCode() == null) {
            return;
        }
        new ScriptAstValidator(sink, false, Set.of(), null).validateStatement(run.getCode());
    }

    private void validateStatement(Statement statement) {
        if (statement instanceof BlockStatement block) {
            block.getStatements().forEach(this::validateStatement);
        } else if (statement instanceof ExpressionStatement expression) {
            validateExpression(expression.getExpression());
        } else if (statement instanceof ReturnStatement returned) {
            validateExpression(returned.getExpression());
        } else if (statement instanceof IfStatement conditional) {
            validateExpression(conditional.getBooleanExpression());
            validateStatement(conditional.getIfBlock());
            validateStatement(conditional.getElseBlock());
        } else if (statement instanceof WhileStatement loop) {
            // 只放行 while（受线程中断与操作预算约束）；for / do-while 均不在白名单。
            if (closureMode) {
                reject(CLOSURE_FORBIDDEN, "集合闭包内只能写一个标量表达式",
                        "collection closures may only contain one scalar expression", loop);
            }
            validateExpression(loop.getBooleanExpression());
            validateStatement(loop.getLoopBlock());
        } else if (statement instanceof BreakStatement || statement instanceof ContinueStatement) {
            if (closureMode) {
                reject(CLOSURE_FORBIDDEN, "集合闭包内不能使用 break/continue",
                        "collection closures may not alter control flow", statement);
            }
        } else if (!(statement instanceof EmptyStatement)) {
            reject(STATEMENT_FORBIDDEN, "不支持的语句 " + statementName(statement)
                    + "：只允许 def、赋值、if/else、while、return",
                    "statement type is outside the allowed AST", statement);
        }
    }

    private void validateExpression(Expression expression) {
        if (expression == null || expression instanceof EmptyExpression) {
            return;
        }
        if (expression instanceof ConstantExpression constant) {
            validateConstant(constant);
        } else if (expression instanceof GStringExpression interpolated) {
            interpolated.getValues().forEach(this::validateExpression);
        } else if (expression instanceof VariableExpression variable) {
            validateVariable(variable);
        } else if (expression instanceof DeclarationExpression declaration) {
            validateDeclaration(declaration);
        } else if (expression instanceof BinaryExpression binary) {
            validateBinary(binary);
        } else if (expression instanceof BooleanExpression bool) {
            validateExpression(bool.getExpression());
        } else if (expression instanceof NotExpression not) {
            validateExpression(not.getExpression());
        } else if (expression instanceof UnaryMinusExpression unary) {
            validateExpression(unary.getExpression());
        } else if (expression instanceof UnaryPlusExpression unary) {
            validateExpression(unary.getExpression());
        } else if (expression instanceof TernaryExpression ternary) {
            // ElvisOperatorExpression 是 TernaryExpression 子类，同样三段校验。
            validateExpression(ternary.getBooleanExpression());
            validateExpression(ternary.getTrueExpression());
            validateExpression(ternary.getFalseExpression());
        } else if (expression instanceof ListExpression list) {
            list.getExpressions().forEach(this::validateExpression);
        } else if (expression instanceof MapExpression map) {
            map.getMapEntryExpressions().forEach(this::validateExpression);
        } else if (expression instanceof MapEntryExpression entry) {
            validateExpression(entry.getKeyExpression());
            validateExpression(entry.getValueExpression());
        } else if (expression instanceof PropertyExpression property) {
            validateProperty(property);
        } else if (expression instanceof MethodCallExpression methodCall) {
            validateMethodCall(methodCall);
        } else if (expression instanceof TupleExpression tuple) {
            // ArgumentListExpression 是 TupleExpression 子类，逐个校验参数。
            tuple.getExpressions().forEach(this::validateExpression);
        } else if (expression instanceof ClosureExpression) {
            reject(CLOSURE_FORBIDDEN, "闭包只能作为 find/findAll/collect/any/every 的唯一参数",
                    "expression type is outside the allowed AST", expression);
        } else {
            reject(EXPRESSION_FORBIDDEN, "不支持的表达式 " + expressionName(expression)
                    + "：禁止 as/强制转型、new、区间、自增自减、位运算、方法引用等写法",
                    "expression type is outside the allowed AST", expression);
        }
    }

    private void validateConstant(ConstantExpression constant) {
        Object value = constant.getValue();
        if (value != null && !(value instanceof String) && !(value instanceof Boolean)
                && !(value instanceof Number)) {
            reject(EXPRESSION_FORBIDDEN, "常量只能是字符串、数字、布尔或 null",
                    "constant type is outside the JSON allowlist", constant);
        }
    }

    private void validateDeclaration(DeclarationExpression declaration) {
        if (closureMode) {
            reject(CLOSURE_FORBIDDEN, "集合闭包内不能声明局部变量",
                    "collection closures may not declare local state", declaration);
        }
        if (declaration.isMultipleAssignmentDeclaration()) {
            reject(EXPRESSION_FORBIDDEN, "一次只能声明一个局部变量，不支持 def (a, b) = ...",
                    "only one local variable may be declared at a time", declaration);
            return;
        }
        if (!(declaration.getLeftExpression() instanceof VariableExpression variable)) {
            reject(EXPRESSION_FORBIDDEN, "局部变量声明只能指向一个变量",
                    "local declaration must target one variable", declaration);
            return;
        }
        validateVariable(variable);
        validateExpression(declaration.getRightExpression());
        // 按整个脚本作用域判重：if/else 两个分支各写 def x 也会被拒绝。
        if (!localVariables.add(variable.getName())) {
            reject(EXPRESSION_FORBIDDEN, "局部变量 " + variable.getName()
                    + " 只能声明一次（分支内也不能重复 def），后续请直接赋值",
                    "local variables may only be declared once", declaration);
        }
    }

    private void validateVariable(VariableExpression variable) {
        if (RESERVED_VARIABLES.contains(variable.getName()) || variable.isThisExpression()
                || variable.isSuperExpression()) {
            reject(EXPRESSION_FORBIDDEN, "变量名 " + variable.getName() + " 是脚本宿主保留名，请换一个名字",
                    "host script variables are outside the receiver allowlist", variable);
        }
        if (!variable.isDynamicTyped()) {
            reject(EXPRESSION_FORBIDDEN, "不能声明类型，请用 def " + variable.getName(),
                    "typed variables are outside the allowed AST", variable);
        }
        if (closureMode && !closureParameters.contains(variable.getName())) {
            ScriptAstValidator declaredScope = findDeclaringOuterScope(variable.getName());
            if (declaredScope != null) {
                // 闭包按值捕获外层局部变量；捕获后再赋值会让闭包与外层看到不同的值。
                if (declaredScope.assignedLocalVariables.contains(variable.getName())) {
                    reject(CLOSURE_FORBIDDEN, "集合闭包只能引用未被重新赋值的局部变量 " + variable.getName(),
                            "collection closures may only capture immutable local values", variable);
                }
                declaredScope.capturedLocalVariables.add(variable.getName());
            }
        }
    }

    private ScriptAstValidator findDeclaringOuterScope(String name) {
        for (ScriptAstValidator scope = outerScope; scope != null; scope = scope.outerScope) {
            if (scope.localVariables.contains(name)) {
                return scope;
            }
        }
        return null;
    }

    private void validateBinary(BinaryExpression binary) {
        int token = binary.getOperation().getType();
        if (!ALLOWED_BINARY_TOKENS.contains(token)) {
            reject(OPERATOR_FORBIDDEN, "不支持的运算符 " + binary.getOperation().getText()
                    + "：只允许 = 比较 && || + - * / % 与下标 []（禁止 <<、+=、in、instanceof、==~、位运算等）",
                    "operator is outside the token allowlist", binary);
        }
        if (token == Types.ASSIGN) {
            validateAssignment(binary);
        }
        validateExpression(binary.getLeftExpression());
        validateExpression(binary.getRightExpression());
    }

    private void validateAssignment(BinaryExpression binary) {
        if (closureMode) {
            reject(CLOSURE_FORBIDDEN, "集合闭包内不能赋值",
                    "collection closures may not assign state", binary);
        } else if (!(binary.getLeftExpression() instanceof VariableExpression variable)
                || !localVariables.contains(variable.getName())) {
            reject(ASSIGNMENT_FORBIDDEN, "只能给已 def 声明的局部变量赋值，禁止修改 Map/List 的元素或属性",
                    "assignment targets must be declared local variables", binary);
        } else if (capturedLocalVariables.contains(variable.getName())) {
            reject(CLOSURE_FORBIDDEN, "局部变量 " + variable.getName() + " 已被集合闭包引用，之后不能再赋值",
                    "captured local variables must remain immutable", binary);
        } else {
            assignedLocalVariables.add(variable.getName());
        }
    }

    private void validateProperty(PropertyExpression property) {
        String name = property.getPropertyAsString();
        if (property instanceof AttributeExpression || name == null || property.isSpreadSafe()
                || RESERVED_PROPERTIES.contains(name)) {
            reject(PROPERTY_FORBIDDEN, "不支持的属性访问：禁止 .@、*.、动态属性名与 class/metaClass 等保留属性",
                    "property is outside the JSON receiver allowlist", property);
        } else if (!isExternalJsonPath(property.getObjectExpression())) {
            reject(LOCAL_PROPERTY_ACCESS, localPropertyMessage(property, name),
                    "property receiver must originate from an external JSON scope", property);
        }
        validateExpression(property.getObjectExpression());
    }

    /** 运行时只允许从外部 JSON 根（ctx/play 等未声明变量、集合闭包参数）沿点号/下标/get 取值。 */
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
            return !(property instanceof AttributeExpression) && name != null && !property.isSpreadSafe()
                    && !RESERVED_PROPERTIES.contains(name) && isExternalJsonPath(property.getObjectExpression());
        }
        if (expression instanceof BinaryExpression binary) {
            return binary.getOperation().getType() == Types.LEFT_SQUARE_BRACKET
                    && isExternalJsonPath(binary.getLeftExpression());
        }
        if (expression instanceof MethodCallExpression call) {
            return !call.isImplicitThis() && !call.isSpreadSafe() && "get".equals(call.getMethodAsString())
                    && isExternalJsonPath(call.getObjectExpression());
        }
        return false;
    }

    private static String localPropertyMessage(PropertyExpression property, String name) {
        if (!(property.getObjectExpression() instanceof VariableExpression variable)) {
            return "不能对字面量或方法结果用点号取属性 ." + name + "，请改写为 ['" + name + "']";
        }
        String receiver = variable.getName();
        return "不能对局部变量用点号取属性: " + receiver + "." + name + "，请改写为 " + receiver + "['" + name
                + "']，或直接读取 ctx./play. 完整路径";
    }

    private void validateMethodCall(MethodCallExpression call) {
        String method = call.getMethodAsString();
        if (method == null || call.isSpreadSafe()) {
            reject(METHOD_FORBIDDEN, "不支持动态方法名或 *. 展开调用",
                    "method dispatch is outside the method allowlist", call);
            return;
        }
        if (COLLECTION_METHODS.contains(method)) {
            validateCollectionCall(call);
        } else if (STRING_METHODS.contains(method)) {
            validateStringCall(call);
        } else if (NUMERIC_METHODS.contains(method)) {
            validateNumericCall(call);
        } else {
            if (call.isImplicitThis()) {
                if (closureMode || !LOG_METHODS.contains(method)) {
                    reject(METHOD_FORBIDDEN, "不支持直接调用 " + method + "()：脚本内只允许 print/println",
                            "implicit host dispatch is outside the method allowlist", call);
                }
            } else {
                if (!ALLOWED_METHODS.contains(method)) {
                    reject(METHOD_FORBIDDEN, "方法 " + method + "() 不在沙箱白名单（禁止 toString/split/add/put/"
                            + "each/sum/join/parseInt 等）", "method is outside the method allowlist", call);
                }
                validateReceiver(call.getObjectExpression());
            }
            validateExpression(call.getArguments());
        }
    }

    private void validateStringCall(MethodCallExpression call) {
        String method = call.getMethodAsString();
        if (call.isImplicitThis() || !(call.getArguments() instanceof ArgumentListExpression arguments)) {
            reject(METHOD_FORBIDDEN, "字符串方法 " + method + "() 必须写明接收者",
                    "string operations require an explicit receiver", call);
            return;
        }
        int size = arguments.getExpressions().size();
        if (("substring".equals(method) && (size < 1 || size > 2)) || (!"substring".equals(method) && size != 0)) {
            reject(METHOD_FORBIDDEN, "字符串方法 " + method + "() 参数个数不正确（substring 1~2 个，其余 0 个）",
                    "string operation argument count is outside the allowlist", call);
        }
        validateReceiver(call.getObjectExpression());
        validateExpression(arguments);
    }

    private void validateNumericCall(MethodCallExpression call) {
        if (call.isImplicitThis() || !(call.getArguments() instanceof ArgumentListExpression arguments)
                || !arguments.getExpressions().isEmpty()) {
            reject(METHOD_FORBIDDEN, "abs() 必须写明接收者且不带参数",
                    "numeric operations require an explicit receiver and no arguments", call);
        }
        validateReceiver(call.getObjectExpression());
    }

    private void validateCollectionCall(MethodCallExpression call) {
        String method = call.getMethodAsString();
        if (closureMode || call.isImplicitThis()) {
            reject(CLOSURE_FORBIDDEN, "集合方法 " + method + " 不能嵌套在集合闭包内或省略接收者",
                    "nested or implicit collection dispatch is outside the allowlist", call);
        }
        validateReceiver(call.getObjectExpression());
        if (!(call.getArguments() instanceof ArgumentListExpression arguments)
                || arguments.getExpressions().size() != 1
                || !(arguments.getExpression(0) instanceof ClosureExpression closure)) {
            reject(CLOSURE_FORBIDDEN, "集合方法 " + method + " 必须且只能传入一个闭包",
                    "collection methods require exactly one controlled closure", call);
            return;
        }
        validateCollectionClosure(closure);
    }

    private void validateCollectionClosure(ClosureExpression closure) {
        Parameter[] parameters = closure.getParameters();
        String parameterName;
        if (!closure.isParameterSpecified()) {
            parameterName = "it";
        } else if (parameters != null && parameters.length == 1
                && parameters[0].getType().equals(ClassHelper.DYNAMIC_TYPE)
                && !RESERVED_VARIABLES.contains(parameters[0].getName())) {
            parameterName = parameters[0].getName();
        } else {
            reject(CLOSURE_FORBIDDEN, "集合闭包只能有一个无类型参数，例如 { item -> ... } 或省略参数用 it",
                    "collection closures accept one untyped element parameter", closure);
            return;
        }
        new ScriptAstValidator(sink, true, Set.of(parameterName), this).validateClosureBody(closure.getCode());
    }

    private void validateClosureBody(Statement statement) {
        if (statement instanceof BlockStatement block) {
            if (block.getStatements().size() != 1) {
                reject(CLOSURE_FORBIDDEN, "集合闭包内只能写一个标量表达式",
                        "collection closures may only contain one scalar expression", block);
                return;
            }
            validateClosureBody(block.getStatements().getFirst());
        } else if (statement instanceof ExpressionStatement expression) {
            validateExpression(expression.getExpression());
        } else if (statement instanceof ReturnStatement returned) {
            validateExpression(returned.getExpression());
        } else {
            reject(CLOSURE_FORBIDDEN, "集合闭包内只能写一个标量表达式，不能使用 if/while 等语句",
                    "collection closures may only contain one scalar expression", statement);
        }
    }

    private void validateReceiver(Expression receiver) {
        if (!(receiver instanceof VariableExpression) && !(receiver instanceof ConstantExpression)
                && !(receiver instanceof PropertyExpression) && !(receiver instanceof MethodCallExpression)
                && !(receiver instanceof BinaryExpression) && !(receiver instanceof ListExpression)
                && !(receiver instanceof MapExpression) && !(receiver instanceof GStringExpression)
                && !(receiver instanceof TernaryExpression) && !(receiver instanceof UnaryMinusExpression)
                && !(receiver instanceof UnaryPlusExpression)) {
            reject(METHOD_FORBIDDEN, "方法接收者只能是 JSON 值，禁止调用 Math/Integer 等宿主类的方法",
                    "receiver shape is outside the JSON receiver allowlist", receiver);
            return;
        }
        validateExpression(receiver);
    }

    private void reject(String code, String message, String sandboxReason, ASTNode node) {
        sink.reject(code, message, sandboxReason, node);
    }

    private static String statementName(Statement statement) {
        return statement.getClass().getSimpleName().replace("Statement", "").toLowerCase();
    }

    private static String expressionName(Expression expression) {
        return expression.getClass().getSimpleName().replace("Expression", "");
    }
}
