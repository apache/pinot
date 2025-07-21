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
    if (function == null || !function.getOperator().equalsIgnoreCase("sum")) {
      return null;
    }

    List<Expression> operands = function.getOperands();
    if (operands == null || operands.size() != 1) {
      return null;
    }

    Expression sumOperand = operands.get(0);
    return optimizeSumExpression(sumOperand);
  }

  /**
   * Optimizes sum(expression) based on the expression type.
   */
  private Expression optimizeSumExpression(Expression sumOperand) {
    if (sumOperand.getType() != ExpressionType.FUNCTION) {
      return null;
    }

    Function innerFunction = sumOperand.getFunctionCall();
    if (innerFunction == null) {
      return null;
    }

    String operator = innerFunction.getOperator();
    List<Expression> operands = innerFunction.getOperands();

    if (operands == null || operands.size() != 2) {
      return null;
    }

    Expression left = operands.get(0);
    Expression right = operands.get(1);

    switch (operator.toLowerCase()) {
      case "add":
      case "plus":
        return optimizeAddition(left, right);
      case "sub":
      case "minus":
        return optimizeSubtraction(left, right);
      default:
        return null;
    }
  }

  /**
   * Optimizes sum(a + b) where one operand is a column and the other is a constant.
   * Returns sum(column) + constant * count(1)
   */
  private Expression optimizeAddition(Expression left, Expression right) {
    if (isColumn(left) && isConstant(right)) {
      // sum(column + constant) → sum(column) + constant * count(1)
      return createOptimizedAddition(left, right);
    } else if (isConstant(left) && isColumn(right)) {
      // sum(constant + column) → sum(column) + constant * count(1)
      return createOptimizedAddition(right, left);
    }
    return null;
  }

  /**
   * Optimizes sum(a - b) where one operand is a column and the other is a constant.
   */
  private Expression optimizeSubtraction(Expression left, Expression right) {
    if (isColumn(left) && isConstant(right)) {
      // sum(column - constant) → sum(column) - constant * count(1)
      return createOptimizedSubtraction(left, right);
    } else if (isConstant(left) && isColumn(right)) {
      // sum(constant - column) → constant * count(1) - sum(column)
      return createOptimizedSubtractionReversed(left, right);
    }
    return null;
  }

  /**
   * Creates the optimized expression: sum(column) + constant * count(1)
   */
  private Expression createOptimizedAddition(Expression column, Expression constant) {
    Expression sumColumn = createSumExpression(column);
    Expression constantTimesCount = createConstantTimesCount(constant);
    return RequestUtils.getFunctionExpression("add", sumColumn, constantTimesCount);
  }

  /**
   * Creates the optimized expression: sum(column) - constant * count(1)
   */
  private Expression createOptimizedSubtraction(Expression column, Expression constant) {
    Expression sumColumn = createSumExpression(column);
    Expression constantTimesCount = createConstantTimesCount(constant);
    return RequestUtils.getFunctionExpression("sub", sumColumn, constantTimesCount);
  }

  /**
   * Creates the optimized expression: constant * count(1) - sum(column)
   */
  private Expression createOptimizedSubtractionReversed(Expression constant, Expression column) {
    Expression constantTimesCount = createConstantTimesCount(constant);
    Expression sumColumn = createSumExpression(column);
    return RequestUtils.getFunctionExpression("sub", constantTimesCount, sumColumn);
  }

  /**
   * Creates sum(column) expression
   */
  private Expression createSumExpression(Expression column) {
    return RequestUtils.getFunctionExpression("sum", column);
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
}
