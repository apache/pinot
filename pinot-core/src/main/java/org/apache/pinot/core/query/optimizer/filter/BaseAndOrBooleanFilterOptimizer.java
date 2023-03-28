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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * This base class acts as a helper for any optimizer that is effectively removing filter conditions.
 * It provides TRUE/FALSE literal classes that can be used to replace filter expressions that are always true/false.
 * It provides an optimization implementation for AND/OR/NOT expressions.
 */
public abstract class BaseAndOrBooleanFilterOptimizer implements FilterOptimizer {

  protected static final Expression TRUE = RequestUtils.getLiteralExpression(true);
  protected static final Expression FALSE = RequestUtils.getLiteralExpression(false);

  /**
   * This recursively optimizes each part of the filter expression. For any AND/OR/NOT,
   * we optimize each child, then we optimize the remaining statement. If there is only
   * a child statement, we optimize that.
   */
  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    if (!canBeOptimized(filterExpression, schema)) {
      return filterExpression;
    }

    Function function = filterExpression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    FilterKind kind = FilterKind.valueOf(function.getOperator());
    switch (kind) {
      case AND:
      case OR:
      case NOT:
        // Recursively traverse the expression tree to find an operator node that can be rewritten.
        operands.replaceAll(operand -> optimize(operand, schema));

        // We have rewritten the child operands, so rewrite the parent if needed.
        return optimizeCurrent(filterExpression);
      default:
        return optimizeChild(filterExpression, schema);
    }
  }

  abstract boolean canBeOptimized(Expression filterExpression, @Nullable Schema schema);

  /**
   * Optimize any cases that are not AND/OR/NOT. This should be done by converting any cases
   * that are always true to TRUE or always false to FALSE.
   */
  abstract Expression optimizeChild(Expression filterExpression, @Nullable Schema schema);

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
      // If any of the literal operands are always false, then replace AND function with FALSE.
      for (Expression operand : operands) {
        if (operand.equals(FALSE)) {
          return FALSE;
        }
      }

      // Remove all Literal operands that are always true.
      operands.removeIf(operand -> operand.equals(TRUE));
      if (operands.isEmpty()) {
        return TRUE;
      }
    } else if (operator.equals(FilterKind.OR.name())) {
      // If any of the literal operands are always true, then replace OR function with TRUE
      for (Expression operand : operands) {
        if (operand.equals(TRUE)) {
          return TRUE;
        }
      }

      // Remove all Literal operands that are always false.
      operands.removeIf(operand -> operand.equals(FALSE));
      if (operands.isEmpty()) {
        return FALSE;
      }
    } else if (operator.equals(FilterKind.NOT.name())) {
      assert operands.size() == 1;
      Expression operand = operands.get(0);
      if (operand.equals(TRUE)) {
        return FALSE;
      }
      if (operand.equals(FALSE)) {
        return TRUE;
      }
    }
    return expression;
  }

  /** Change the expression value to boolean literal with given value. */
  protected static Expression getExpressionFromBoolean(boolean value) {
    return value ? TRUE : FALSE;
  }
}
