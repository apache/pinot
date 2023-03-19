package org.apache.pinot.core.query.optimizer.filter;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.pinot.sql.FilterKind.EQUALS;
import static org.apache.pinot.sql.FilterKind.NOT_EQUALS;

/**
 * This optimizer converts all predicates where the left hand side == right hand side to
 * a simple TRUE/FALSE literal value. While filters like, WHERE 1=1 OR "col1"="col1" are not
 * typical, they end up expensive in Pinot because they are rewritten as A-A==0.
 */
public class IdenticalPredicateFilterOptimizer extends BaseAndOrBooleanFilterOptimizer {

    @Override
    public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
        Function function = filterExpression.getFunctionCall();
        if (function == null) {
            return filterExpression;
        }

        List<Expression> operands = function.getOperands();
        FilterKind kind = FilterKind.valueOf(function.getOperator());
        switch (kind) {
            case AND:
            case OR:
            case NOT:
                // Recursively traverse the expression tree to find an operator node that can be rewritten.
                operands.forEach(operand -> optimize(operand, schema));

                // We have rewritten the child operands, so rewrite the parent if needed.
                return optimizeCurrent(filterExpression);
            case EQUALS:
                if (hasIdenticalLhsAndRhs(filterExpression)) {
                    return TRUE;
                }
                return filterExpression;
            case NOT_EQUALS:
                if (hasIdenticalLhsAndRhs(filterExpression)) {
                    return FALSE;
                }
                return filterExpression;
            default:
                return filterExpression;
        }
    }

    @Override
    protected boolean isAlwaysFalse(Expression operand) {
        if (super.isAlwaysFalse(operand)) {
            return true;
        } else if (hasIdenticalLhsAndRhs(operand)) {
            return operand.getFunctionCall().getOperator().equals(NOT_EQUALS.name());
        }
        return false;
    }

    @Override
    protected boolean isAlwaysTrue(Expression operand) {
        if (super.isAlwaysTrue(operand)) {
            return true;
        } else if (hasIdenticalLhsAndRhs(operand)) {
            return operand.getFunctionCall().getOperator().equals(EQUALS.name());
        }
        return false;
    }

    /**
     * Pinot queries of the WHERE 1 != 1 AND "col1" = "col2" variety are rewritten as
     * 1-1 != 0 AND "col1"-"col2" = 0. Therefore, we check specifically for the case where
     * the operand is set up in this fashion.
     */
    private boolean hasIdenticalLhsAndRhs(Expression operand) {
        List<Expression> children = operand.getFunctionCall().getOperands();
        boolean hasTwoChildren = children.size() == 2;
        Expression firstChild = children.get(0);
        if (firstChild.getFunctionCall() == null) {
            return false;
        }
        boolean firstChildIsMinusOperator = firstChild.getFunctionCall().getOperator().equals("minus");
        boolean firstChildHasTwoOperands = firstChild.getFunctionCall().getOperandsSize() == 2;
        Expression minusOperandFirstChild = firstChild.getFunctionCall().getOperands().get(0);
        Expression minusOperandSecondChild = firstChild.getFunctionCall().getOperands().get(1);
        boolean bothOperandsAreEqual = minusOperandFirstChild.equals(minusOperandSecondChild);
        Expression secondChild = children.get(1);
        boolean isSecondChildLiteralZero = isLiteralZero(secondChild);

        return hasTwoChildren &&
                firstChildIsMinusOperator &&
                firstChildHasTwoOperands &&
                bothOperandsAreEqual &&
                isSecondChildLiteralZero;
    }

    private boolean isLiteralZero(Expression expression) {
        if (!expression.isSetLiteral()) {
            return false;
        }
        Object literalValue = expression.getLiteral().getFieldValue();
        return literalValue.equals(0) || literalValue.equals(0L) || literalValue.equals(0d);
    }
}
