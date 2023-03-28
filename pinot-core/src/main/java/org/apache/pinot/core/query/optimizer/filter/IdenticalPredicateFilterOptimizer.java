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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * This optimizer converts all predicates where the left hand side == right hand side to
 * a simple TRUE/FALSE literal value. While filters like, WHERE 1=1 OR "col1"="col1" are not
 * typical, they end up expensive in Pinot because they are rewritten as A-A==0.
 */
public class IdenticalPredicateFilterOptimizer extends BaseAndOrBooleanFilterOptimizer {

  @Override
  boolean canBeOptimized(Expression filterExpression, @Nullable Schema schema) {
    // if there's no function call, there's no lhs or rhs
    return filterExpression.getFunctionCall() != null;
  }

  @Override
  Expression optimizeChild(Expression filterExpression, @Nullable Schema schema) {
    Function function = filterExpression.getFunctionCall();
    FilterKind kind = FilterKind.valueOf(function.getOperator());
    switch (kind) {
      case EQUALS:
        if (hasIdenticalLhsAndRhs(function.getOperands())) {
          return TRUE;
        }
        break;
      case NOT_EQUALS:
        if (hasIdenticalLhsAndRhs(function.getOperands())) {
          return FALSE;
        }
        break;
      default:
        break;
    }
    return filterExpression;
  }

  /**
   * Pinot queries of the WHERE 1 != 1 AND "col1" = "col2" variety are rewritten as
   * 1-1 != 0 AND "col1"-"col2" = 0. Therefore, we check specifically for the case where
   * the operand is set up in this fashion.
   *
   * We return false specifically after every check to ensure we're only continuing when
   * the input looks as expected. Otherwise, it's easy to for one of the operand functions
   * to return null and fail the query.
   *
   * TODO: The rewrite is already happening in PredicateComparisonRewriter.updateFunctionExpression(),
   * so we might just compare the lhs and rhs there.
   */
  private boolean hasIdenticalLhsAndRhs(List<Expression> operands) {
    boolean hasTwoChildren = operands.size() == 2;
    Expression firstChild = operands.get(0);
    if (firstChild.getFunctionCall() == null || !hasTwoChildren) {
      return false;
    }
    boolean firstChildIsMinusOperator = firstChild.getFunctionCall().getOperator().equals("minus");
    if (!firstChildIsMinusOperator) {
      return false;
    }
    boolean firstChildHasTwoOperands = firstChild.getFunctionCall().getOperandsSize() == 2;
    if (!firstChildHasTwoOperands) {
      return false;
    }
    Expression minusOperandFirstChild = firstChild.getFunctionCall().getOperands().get(0);
    Expression minusOperandSecondChild = firstChild.getFunctionCall().getOperands().get(1);
    if (minusOperandFirstChild == null || minusOperandSecondChild == null || !minusOperandFirstChild.equals(
        minusOperandSecondChild)) {
      return false;
    }
    Expression secondChild = operands.get(1);
    return isLiteralZero(secondChild);
  }

  private boolean isLiteralZero(Expression expression) {
    if (!expression.isSetLiteral()) {
      return false;
    }
    Object literalValue = expression.getLiteral().getFieldValue();
    return literalValue.equals(0) || literalValue.equals(0L) || literalValue.equals(0d);
  }
}
