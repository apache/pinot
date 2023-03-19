/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.optimizer.filter;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;

import javax.annotation.Nullable;
import java.util.List;


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
      return operand.getFunctionCall().getOperator().equals(FilterKind.NOT_EQUALS.name());
    }
    return false;
  }

  @Override
  protected boolean isAlwaysTrue(Expression operand) {
    if (super.isAlwaysTrue(operand)) {
      return true;
    } else if (hasIdenticalLhsAndRhs(operand)) {
      return operand.getFunctionCall().getOperator().equals(FilterKind.EQUALS.name());
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

    return hasTwoChildren && firstChildIsMinusOperator && firstChildHasTwoOperands && bothOperandsAreEqual
        && isSecondChildLiteralZero;
  }

  private boolean isLiteralZero(Expression expression) {
    if (!expression.isSetLiteral()) {
      return false;
    }
    Object literalValue = expression.getLiteral().getFieldValue();
    return literalValue.equals(0) || literalValue.equals(0L) || literalValue.equals(0d);
  }
}
