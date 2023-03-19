package org.apache.pinot.core.query.optimizer.filter;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;

import javax.annotation.Nullable;
import java.util.List;

public abstract class BaseAndOrBooleanFilterOptimizer implements FilterOptimizer {

    protected static final Expression TRUE = RequestUtils.getLiteralExpression(true);
    protected static final Expression FALSE = RequestUtils.getLiteralExpression(false);

    @Override
    public abstract Expression optimize(Expression filterExpression, @Nullable Schema schema);

    /**
     * If any of the operands of AND function is "false", then the AND function itself is false and can be replaced with
     * "false" literal. Otherwise, remove all the "true" operands of the AND function. Similarly, if any of the operands
     * of OR function is "true", then the OR function itself is true and can be replaced with "true" literal. Otherwise,
     * remove all the "false" operands of the OR function.
     */
    protected Expression optimizeCurrent(Expression expression) {
        Function function = expression.getFunctionCall();
        String operator = function.getOperator();
        List<Expression> operands = function.getOperands();
        if (operator.equals(FilterKind.AND.name())) {
            // If any of the literal operands are FALSE, then replace AND function with FALSE.
            for (Expression operand : operands) {
                if (isAlwaysFalse(operand)) {
                    return FALSE;
                }
            }

            // Remove all Literal operands that are TRUE.
            operands.removeIf(this::isAlwaysTrue);
            if (operands.isEmpty()) {
                return TRUE;
            }
        } else if (operator.equals(FilterKind.OR.name())) {
            // If any of the literal operands are TRUE, then replace OR function with TRUE
            for (Expression operand : operands) {
                if (isAlwaysTrue(operand)) {
                    return TRUE;
                }
            }

            // Remove all Literal operands that are FALSE.
            operands.removeIf(this::isAlwaysFalse);
            if (operands.isEmpty()) {
                return FALSE;
            }
        } else if (operator.equals(FilterKind.NOT.name())) {
            assert operands.size() == 1;
            Expression operand = operands.get(0);
            if (isAlwaysTrue(operand)) {
                return FALSE;
            }
            if (isAlwaysFalse(operand)) {
                return TRUE;
            }
        }
        return expression;
    }

    protected boolean isAlwaysFalse(Expression operand) {
        return operand.equals(FALSE);
    }

    protected boolean isAlwaysTrue(Expression operand) {
        return operand.equals(TRUE);
    }
}
