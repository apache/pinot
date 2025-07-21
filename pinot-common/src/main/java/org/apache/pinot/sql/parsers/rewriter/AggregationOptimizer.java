/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.sql.parsers.rewriter;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;


/**
 * AggregationOptimizer optimizes aggregation functions by leveraging mathematical properties.
 * Currently supports:
 * - sum(column + constant) → sum(column) + constant * count(1)
 * - sum(column - constant) → sum(column) - constant * count(1)
 * - sum(constant + column) → sum(column) + constant * count(1)
 * - sum(constant - column) → constant * count(1) - sum(column)
 */
public class AggregationOptimizer implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    if (selectList != null) {
      for (int i = 0; i < selectList.size(); i++) {
        Expression expression = selectList.get(i);
        Expression optimized = optimizeExpression(expression);
        if (optimized != null) {
          selectList.set(i, optimized);
        }
      }
    }
    return pinotQuery;
  }

  /**
   * Optimizes an expression if it matches supported patterns.
   * Returns the optimized expression or null if no optimization is possible.
   */
  private Expression optimizeExpression(Expression expression) {
    if (expression.getType() != ExpressionType.FUNCTION) {
      return null;
    }

    Function function = expression.getFunctionCall();
    if (function == null) {
      return null;
    }

    String operator = function.getOperator().toLowerCase();
    List<Expression> operands = function.getOperands();

    if (operands == null || operands.size() != 1) {
      return null;
    }

    Expression operand = operands.get(0);

    switch (operator) {
      case "sum":
        return optimizeSumExpression(operand);
      case "avg":
        return optimizeAvgExpression(operand);
      case "min":
        return optimizeMinExpression(operand);
      case "max":
        return optimizeMaxExpression(operand);
      default:
        return null;
    }
  }

  /**
   * Optimizes sum(expression) based on the expression type.
   */
  private Expression optimizeSumExpression(Expression sumOperand) {
    return optimizeArithmeticExpression(sumOperand, "sum");
  }

  /**
   * Optimizes avg(expression) based on the expression type.
   * AVG(column + constant) = AVG(column) + constant
   * AVG(column - constant) = AVG(column) - constant
   * AVG(constant - column) = constant - AVG(column)
   * AVG(column * constant) = AVG(column) * constant
   */
  private Expression optimizeAvgExpression(Expression avgOperand) {
    return optimizeArithmeticExpression(avgOperand, "avg");
  }

  /**
   * Optimizes min(expression) based on the expression type.
   * MIN(column + constant) = MIN(column) + constant
   * MIN(column - constant) = MIN(column) - constant
   * MIN(constant - column) = constant - MAX(column)
   * MIN(column * constant) = MIN(column) * constant (if constant > 0)
   *                        = MAX(column) * constant (if constant < 0)
   */
  private Expression optimizeMinExpression(Expression minOperand) {
    return optimizeArithmeticExpression(minOperand, "min");
  }

  /**
   * Optimizes max(expression) based on the expression type.
   * MAX(column + constant) = MAX(column) + constant
   * MAX(column - constant) = MAX(column) - constant
   * MAX(constant - column) = constant - MIN(column)
   * MAX(column * constant) = MAX(column) * constant (if constant > 0)
   *                        = MIN(column) * constant (if constant < 0)
   */
  private Expression optimizeMaxExpression(Expression maxOperand) {
    return optimizeArithmeticExpression(maxOperand, "max");
  }

  /**
   * Generic method to optimize arithmetic expressions for different aggregation functions.
   */
  private Expression optimizeArithmeticExpression(Expression operand, String aggregationFunction) {
    if (operand.getType() != ExpressionType.FUNCTION) {
      return null;
    }

    Function innerFunction = operand.getFunctionCall();
    if (innerFunction == null) {
      return null;
    }

    String operator = innerFunction.getOperator();
    List<Expression> operands = innerFunction.getOperands();

    // Handle direct arithmetic operations (used by sum)
    if (operands != null && operands.size() == 2) {
      Expression left = operands.get(0);
      Expression right = operands.get(1);

      switch (operator.toLowerCase()) {
        case "add":
        case "plus":
          return optimizeAdditionForFunction(left, right, aggregationFunction);
        case "sub":
        case "minus":
          return optimizeSubtractionForFunction(left, right, aggregationFunction);
        case "mul":
        case "mult":
        case "multiply":
          return optimizeMultiplicationForFunction(left, right, aggregationFunction);
        default:
          break;
      }
    }

    // Handle values wrapper function (used by avg, min, max)
    if ("values".equals(operator.toLowerCase()) && operands != null && operands.size() == 1) {
      Expression valuesOperand = operands.get(0);
      if (valuesOperand.getType() == ExpressionType.FUNCTION) {
        Function rowFunction = valuesOperand.getFunctionCall();
        if (rowFunction != null && "row".equals(rowFunction.getOperator().toLowerCase())
            && rowFunction.getOperands() != null && rowFunction.getOperands().size() == 1) {
          Expression rowOperand = rowFunction.getOperands().get(0);
          return optimizeArithmeticExpression(rowOperand, aggregationFunction);
        }
      }
    }

    return null;
  }

  /**
   * Optimizes aggregation(a + b) where one operand is a column and the other is a constant.
   */
  private Expression optimizeAdditionForFunction(Expression left, Expression right, String aggregationFunction) {
    if (isColumn(left) && isConstant(right)) {
      // AGG(column + constant) → AGG(column) + constant (for avg/min/max)
      // or AGG(column) + constant * count(1) (for sum)
      return createOptimizedAddition(left, right, aggregationFunction);
    } else if (isConstant(left) && isColumn(right)) {
      // AGG(constant + column) → AGG(column) + constant (for avg/min/max)
      // or AGG(column) + constant * count(1) (for sum)
      return createOptimizedAddition(right, left, aggregationFunction);
    }
    return null;
  }

  /**
   * Optimizes aggregation(a - b) where one operand is a column and the other is a constant.
   */
  private Expression optimizeSubtractionForFunction(Expression left, Expression right, String aggregationFunction) {
    if (isColumn(left) && isConstant(right)) {
      // AGG(column - constant) → AGG(column) - constant (for avg/min/max)
      // or AGG(column) - constant * count(1) (for sum)
      return createOptimizedSubtraction(left, right, aggregationFunction);
    } else if (isConstant(left) && isColumn(right)) {
      // Special cases: constant - AGG(column)
      return createOptimizedSubtractionReversed(left, right, aggregationFunction);
    }
    return null;
  }

  /**
   * Optimizes aggregation(a * b) where one operand is a column and the other is a constant.
   * AGG(column * constant) = AGG(column) * constant (for avg, and min/max when constant > 0)
   * For min/max with negative constants, the order flips:
   * MIN(col * neg) = MAX(col) * neg
   */
  private Expression optimizeMultiplicationForFunction(Expression left, Expression right, String aggregationFunction) {
    if (isColumn(left) && isConstant(right)) {
      return createOptimizedMultiplication(left, right, aggregationFunction);
    } else if (isConstant(left) && isColumn(right)) {
      return createOptimizedMultiplication(right, left, aggregationFunction);
    }
    return null;
  }

  /**
   * Creates the optimized expression for addition based on aggregation function.
   * For sum: AGG(column) + constant * count(1)
   * For avg/min/max: AGG(column) + constant
   */
  private Expression createOptimizedAddition(Expression column, Expression constant, String aggregationFunction) {
    Expression aggColumn = createAggregationExpression(column, aggregationFunction);
    Expression rightOperand;

    if ("sum".equals(aggregationFunction)) {
      rightOperand = createConstantTimesCount(constant);
    } else {
      rightOperand = constant;
    }

    return RequestUtils.getFunctionExpression("add", aggColumn, rightOperand);
  }

  /**
   * Creates the optimized expression for subtraction based on aggregation function.
   * For sum: AGG(column) - constant * count(1)
   * For avg/min/max: AGG(column) - constant
   */
  private Expression createOptimizedSubtraction(Expression column, Expression constant, String aggregationFunction) {
    Expression aggColumn = createAggregationExpression(column, aggregationFunction);
    Expression rightOperand;

    if ("sum".equals(aggregationFunction)) {
      rightOperand = createConstantTimesCount(constant);
    } else {
      rightOperand = constant;
    }

    return RequestUtils.getFunctionExpression("sub", aggColumn, rightOperand);
  }

  /**
   * Creates the optimized expression for reversed subtraction based on aggregation function.
   * For sum: constant * count(1) - sum(column)
   * For avg: constant - avg(column)
   * For min: constant - max(column)
   * For max: constant - min(column)
   */
  private Expression createOptimizedSubtractionReversed(Expression constant, Expression column,
      String aggregationFunction) {
    Expression leftOperand;
    Expression aggColumn;

    if ("sum".equals(aggregationFunction)) {
      leftOperand = createConstantTimesCount(constant);
      aggColumn = createAggregationExpression(column, "sum");
    } else if ("min".equals(aggregationFunction)) {
      leftOperand = constant;
      aggColumn = createAggregationExpression(column, "max");  // min(c - col) = c - max(col)
    } else if ("max".equals(aggregationFunction)) {
      leftOperand = constant;
      aggColumn = createAggregationExpression(column, "min");  // max(c - col) = c - min(col)
    } else {  // avg
      leftOperand = constant;
      aggColumn = createAggregationExpression(column, "avg");
    }

    return RequestUtils.getFunctionExpression("sub", leftOperand, aggColumn);
  }

  /**
   * Creates optimized multiplication expression based on aggregation function.
   * For avg: avg(column) * constant
   * For sum: sum(column) * constant
   * For min/max with positive constant: min/max(column) * constant
   * For min/max with negative constant: max/min(column) * constant (order flips)
   */
  private Expression createOptimizedMultiplication(Expression column, Expression constant, String aggregationFunction) {
    Expression aggColumn;

    if ("min".equals(aggregationFunction) && isNegativeConstant(constant)) {
      aggColumn = createAggregationExpression(column, "max");  // min(col * neg) = max(col) * neg
    } else if ("max".equals(aggregationFunction) && isNegativeConstant(constant)) {
      aggColumn = createAggregationExpression(column, "min");  // max(col * neg) = min(col) * neg
    } else {
      aggColumn = createAggregationExpression(column, aggregationFunction);
    }

    return RequestUtils.getFunctionExpression("mult", aggColumn, constant);
  }

  /**
   * Creates aggregation function expression for the given column.
   */
  private Expression createAggregationExpression(Expression column, String aggregationFunction) {
    return RequestUtils.getFunctionExpression(aggregationFunction, column);
  }

  /**
   * Creates constant * count(1) expression
   */
  private Expression createConstantTimesCount(Expression constant) {
    Expression countOne = createCountOneExpression();
    return RequestUtils.getFunctionExpression("mult", constant, countOne);
  }

  /**
   * Creates count(1) expression
   */
  private Expression createCountOneExpression() {
    Literal oneLiteral = new Literal();
    oneLiteral.setIntValue(1);
    Expression oneExpression = new Expression(ExpressionType.LITERAL);
    oneExpression.setLiteral(oneLiteral);
    return RequestUtils.getFunctionExpression("count", oneExpression);
  }

  /**
   * Checks if an expression is a column (identifier)
   */
  private boolean isColumn(Expression expression) {
    return expression.getType() == ExpressionType.IDENTIFIER;
  }

  /**
   * Checks if an expression is a numeric constant (literal)
   */
  private boolean isConstant(Expression expression) {
    if (expression.getType() != ExpressionType.LITERAL) {
      return false;
    }

    Literal literal = expression.getLiteral();
    if (literal == null) {
      return false;
    }

    // Check if it's a numeric literal
    return literal.isSetIntValue() || literal.isSetLongValue()
        || literal.isSetFloatValue() || literal.isSetDoubleValue();
  }

  /**
   * Checks if the expression is a negative numeric constant.
   */
  private boolean isNegativeConstant(Expression expression) {
    if (!isConstant(expression)) {
      return false;
    }

    Literal literal = expression.getLiteral();
    if (literal.isSetIntValue()) {
      return literal.getIntValue() < 0;
    } else if (literal.isSetLongValue()) {
      return literal.getLongValue() < 0;
    } else if (literal.isSetFloatValue()) {
      return literal.getFloatValue() < 0;
    } else if (literal.isSetDoubleValue()) {
      return literal.getDoubleValue() < 0;
    }
    return false;
  }
}
