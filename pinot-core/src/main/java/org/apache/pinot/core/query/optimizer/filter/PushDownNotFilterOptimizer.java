package org.apache.pinot.core.query.optimizer.filter;

import com.google.common.annotations.VisibleForTesting;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code PushdownNotFilterOptimizer} pushes down NOT expressions to simplify and flatten filter expressions,
 * applying DeMorgan's law recursively. For example:
 * <ul>
 *   <li>NOT(X AND Y) becomes NOT(X) OR NOT(Y)</li>
 *   <li>NOT(X OR Y) becomes NOT(X) AND NOT(Y)</li>
 *   <li>Handles nested cases like NOT(X AND (Y OR Z))</li>
 * </ul>
 */
public class PushDownNotFilterOptimizer implements FilterOptimizer {

  @Override
  public Expression optimize(Expression filterExpression, Schema schema) {
    System.out.println(optimize(filterExpression));
    return optimize(filterExpression);
  }

  @VisibleForTesting
  Expression optimize(Expression expression) {
    if (expression.getType() != ExpressionType.FUNCTION) {
      return expression;
    }

    Function function = expression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();

    if (filterKind == FilterKind.NOT) {
      Expression operand = operands.get(0);
      if (operand.getType() == ExpressionType.FUNCTION) {
        Function innerFunction = operand.getFunctionCall();
        FilterKind innerFilterKind = FilterKind.valueOf(innerFunction.getOperator());

        if (innerFilterKind == FilterKind.AND) {
          return optimize(createOrExpression(innerFunction.getOperands()));
        } else if (innerFilterKind == FilterKind.OR) {
          return optimize(createAndExpression(innerFunction.getOperands()));
        }
      }
      return expression;
    }

    for (int i = 0; i < operands.size(); i++) {
      operands.set(i, optimize(operands.get(i)));
    }

    expression.setType(ExpressionType.FUNCTION);
    return expression;
  }

  /**
   * Creates an OR expression by applying NOT to each operand and connecting them with OR.
   */
  private Expression createOrExpression(List<Expression> operands) {
    Function orFunction = new Function();
    orFunction.setOperator(FilterKind.OR.name());
    List<Expression> orOperands = new ArrayList<>();
    for (Expression operand : operands) {
      orOperands.add(createNotExpression(operand));
    }
    orFunction.setOperands(orOperands);
    Expression orExpression = new Expression();
    orExpression.setFunctionCall(orFunction);
    orExpression.setType(ExpressionType.FUNCTION);
    return orExpression;
  }

  /**
   * Creates an AND expression by applying NOT to each operand and connecting them with AND.
   */
  private Expression createAndExpression(List<Expression> operands) {
    Function andFunction = new Function();
    andFunction.setOperator(FilterKind.AND.name());
    List<Expression> andOperands = new ArrayList<>();
    for (Expression operand : operands) {
      andOperands.add(createNotExpression(operand));
    }
    andFunction.setOperands(andOperands);
    Expression andExpression = new Expression();
    andExpression.setFunctionCall(andFunction);
    andExpression.setType(ExpressionType.FUNCTION);
    return andExpression;
  }

  /**
   * Wraps an operand in a NOT expression.
   */
  private Expression createNotExpression(Expression operand) {
    Function notFunction = new Function();
    notFunction.setOperator(FilterKind.NOT.name());
    List<Expression> notOperands = new ArrayList<>();
    notOperands.add(operand);
    notFunction.setOperands(notOperands);
    Expression notExpression = new Expression();
    notExpression.setFunctionCall(notFunction);
    notExpression.setType(ExpressionType.FUNCTION);
    return notExpression;
  }
}
