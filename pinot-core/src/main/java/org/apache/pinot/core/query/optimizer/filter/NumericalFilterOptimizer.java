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

import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Numerical expressions of form "column = literal" or "column != literal" can compare a column of one datatype
 * (say INT) with a literal of different datatype (say DOUBLE) can not be evaluated on the Server. Hence, this class
 * will rewrite such expressions into an equivalent expression whose LHS and RHS are of the same datatype.
 *
 * Simple predicate examples:
 *  1) WHERE "intColumn = 5.0"  gets rewritten to "WHERE intColumn = 5"
 *  2) WHERE "intColumn != 5.0" gets rewritten to "WHERE intColumn != 5"
 *  3) WHERE "intColumn = 5.5"  gets rewritten to "WHERE false" because 5.5 will never match any value in an INT column.
 *
 * Compound predicate examples:
 *  4) WHERE "intColumn1 = 5.5 AND intColumn2 = intColumn3"
 *       rewrite to "WHERE false AND intColumn2 = intColumn3"
 *       rewrite to "WHERE intColumn2 = intColumn3"
 *  5) WHERE "intColumn1 != 5.5 OR intColumn2 = intColumn3"
 *       rewrite to "WHERE true OR intColumn2 = intColumn3"
 *       rewrite to "WHERE true"
 *       rewrite to query without any WHERE clause.
 *
 * Short-circuit evaluation:
 * In the 3rd example above, the entire predicate has been rewritten into "false". This basically means that the query
 * will not return any data if it is evaluated on the server side, so its better that the Broker itself returns an empty
 * result back to client instead of sending the query to brokers for evaluation.
 */
public class NumericalFilterOptimizer implements FilterOptimizer {

  private static final Literal TRUE = Literal.boolValue(true);
  private static final Literal FALSE = Literal.boolValue(false);

  @Override
  public FilterQueryTree optimize(FilterQueryTree filterQueryTree, @Nullable Schema schema) {
    // Don't do anything here since this is for PQL queries which we no longer support.
    return filterQueryTree;
  }

  @Override
  public Expression optimize(Expression expression, @Nullable Schema schema) {
    ExpressionType type = expression.getType();
    if (type != ExpressionType.FUNCTION) {
      // Not a function, so we have nothing to rewrite.
      return expression;
    }

    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    String operator = function.getOperator();
    if (!(operator.equals(FilterKind.EQUALS.name()) || operator.equals(FilterKind.NOT_EQUALS.name()))) {
      // This is not an EQUALS or NOT_EQUALS function, but one of its operands may be an EQUALS function so
      // recursively traverse the expression tree to see if we find an EQUALS function to rewrite.
      operands.forEach(operand -> optimize(operand, schema));

      // Process the current node to clean up any operands that got set to true/false literal.
      return optimizeCurrent(expression);
    }

    // If we are here, then this expression must have EQUALS operator. Verify that LHS is a numeric column and RHS is
    // a numeric literal before proceeding further.
    Expression lhs = operands.get(0), rhs = operands.get(1);
    if (!(isNumericColumn(lhs, schema) && isNumericLiteral(rhs))) {
      return expression;
    }

    // Rewrite the expression.
    return rewrite(expression, lhs, rhs, schema);
  }

  /**
   * If any of the operands of AND function is "false", then the AND function itself is false and can be replaced with
   * "false" literal. Otherwise, remove all the "true" operands of the AND function. Similarly, if any of the operands
   * of OR function is "true", then the OR function itself is true and can be replaced with "true" literal. Otherwise,
   * remove all the "false" operands of the OR function.
   */
  private static Expression optimizeCurrent(Expression expression) {
    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    if (function.getOperator().equals(FilterKind.AND.name())) {
      // If any of the literal operands are FALSE, then replace AND function with FALSE.
      if (operands.stream().anyMatch(operand -> operand.getType() == ExpressionType.LITERAL && operand.getLiteral().equals(FALSE))) {
        return setExpressionToBoolean(expression, false);
      }

      // Remove all Literal operands that are TRUE.
      operands.removeIf(x -> x.getType() == ExpressionType.LITERAL && x.getLiteral().getBoolValue());
      if (operands.size() == 0) {
        return setExpressionToBoolean(expression, true);
      }
    } else if (function.getOperator().equals(FilterKind.OR.name())) {
      // If any of the literal operands are TRUE, then replace OR function with TRUE
      if (operands.stream().anyMatch(operand -> operand.getType() == ExpressionType.LITERAL && operand.getLiteral().equals(TRUE))) {
        return setExpressionToBoolean(expression, true);
      }

      // Remove all Literal operands that are FALSE.
      operands.removeIf(x -> x.getType() == ExpressionType.LITERAL && !x.getLiteral().getBoolValue());
      if (operands.size() == 0) {
        return setExpressionToBoolean(expression, false);
      }
    }

    return expression;
  }

  private boolean isNumericColumn(Expression expression, Schema schema) {
    if (expression.getType() != ExpressionType.IDENTIFIER) {
      // Expression can not be a column.
      return false;
    }

    String column = expression.getIdentifier().getName();
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    if (fieldSpec == null || !fieldSpec.isSingleValueField()) {
      // Expression can not be a column name.
      return false;
    }

    return schema.getFieldSpecFor(column).getDataType().isNumeric();
  }

  private boolean isNumericLiteral(Expression expression) {
    if (expression.getType() == ExpressionType.LITERAL) {
      Literal._Fields type = expression.getLiteral().getSetField();
      switch (type) {
        case SHORT_VALUE:
        case INT_VALUE:
        case LONG_VALUE:
        case DOUBLE_VALUE:
          return true;
      }
    }
    return false;
  }

  /** Change the expression value to boolean literal with given value. */
  private static Expression setExpressionToBoolean(Expression expression, boolean value) {
    expression.unsetFunctionCall();
    expression.setType(ExpressionType.LITERAL);
    expression.setLiteral(Literal.boolValue(value));

    return expression;
  }

  private Expression rewrite(Expression equals, Expression lhs, Expression rhs, Schema schema) {
    // Get expression operator
    boolean result = equals.getFunctionCall().getOperator().equals(FilterKind.NOT_EQUALS.name());

    // Get column data type.
    FieldSpec.DataType dataType = schema.getFieldSpecFor(lhs.getIdentifier().getName()).getDataType();

    switch (rhs.getLiteral().getSetField()) {
      case SHORT_VALUE:
      case INT_VALUE:
        // No rewrites needed since SHORT and INT conversion to numeric column types (INT, LONG, FLOAT, and DOUBLE) is
        // lossless and will be implicitly handled on the server side.
        break;
      case LONG_VALUE: {
        long actual = rhs.getLiteral().getLongValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            if (converted != actual) {
              // Long value does not fall within the bounds of INT column.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace long value with converted int value.
              rhs.setLiteral(Literal.intValue(converted));
            }
            break;
          }
          case FLOAT: {
            float converted = (float) actual;
            if (BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted)) != 0) {
              // Long to float conversion is lossy.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace long value with converted float value.
              rhs.setLiteral(Literal.doubleValue(converted));
            }
            break;
          }
          case DOUBLE: {
            double converted = (double) actual;
            if (BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted)) != 0) {
              // Long to double conversion is lossy.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace long value with converted double value.
              rhs.setLiteral(Literal.doubleValue(converted));
            }
            break;
          }
        }
        break;
      }
      case DOUBLE_VALUE:
        double actual = rhs.getLiteral().getDoubleValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            if (converted != actual) {
              // Double value does not fall within the bounds of INT column.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace double value with converted int value.
              rhs.setLiteral(Literal.intValue(converted));
            }
            break;
          }
          case LONG: {
            long converted = (long) actual;
            if (BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted)) != 0) {
              // Double to long conversion is lossy.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace double value with converted long value.
              rhs.setLiteral(Literal.longValue(converted));
            }
            break;
          }
          case FLOAT: {
            float converted = (float) actual;
            if (converted != actual) {
              // Double to float conversion is lossy.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace double value with converted float value.
              rhs.setLiteral(Literal.doubleValue(converted));
            }
            break;
          }
        }
    }
    return equals;
  }
}
