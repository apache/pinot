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
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Numerical expressions of form "column <operator> literal", where operator can be '=', '!=', '>', '>=', '<', or '<=',
 * can compare a column of one datatype (say INT) with a literal of different datatype (say DOUBLE). These expressions
 * can not be evaluated on the Server. Hence, we rewrite such expressions into an equivalent expression whose LHS and
 * RHS are of the same datatype.
 *
 * Simple predicate examples:
 *  1) WHERE "intColumn = 5.0"  gets rewritten to "WHERE intColumn = 5"
 *  2) WHERE "intColumn != 5.0" gets rewritten to "WHERE intColumn != 5"
 *  3) WHERE "intColumn = 5.5"  gets rewritten to "WHERE false" because INT values can not match 5.5.
 *  4) WHERE "intColumn = 3000000000" gets rewritten to "WHERE false" because INT values can not match 3000000000.
 *  5) WHERE "intColumn != 3000000000" gets rewritten to "WHERE true" because INT values always not equal to 3000000000.
 *  6) WHERE "intColumn < 5.1" gets rewritten to "WHERE intColumn <= 5"
 *  7) WHERE "intColumn > -3E9" gets rewritten to "WHERE true" because int values are always greater than -3E9.
 *
 * Compound predicate examples:
 *  8) WHERE "intColumn1 = 5.5 AND intColumn2 = intColumn3"
 *       rewrite to "WHERE false AND intColumn2 = intColumn3"
 *       rewrite to "WHERE intColumn2 = intColumn3"
 *  9) WHERE "intColumn1 != 5.5 OR intColumn2 = 5000000000" (5000000000 is out of bounds for integer column)
 *       rewrite to "WHERE true OR false"
 *       rewrite to "WHERE true"
 *       rewrite to query without any WHERE clause.
 *
 * When entire predicate gets rewritten to false (Example 3 above), the query will not return any data. Hence, it is
 * better for the Broker itself to return an empty response rather than sending the query to servers for further
 * evaluation.
 *
 * TODO: Add support for BETWEEN, IN, and NOT IN operators.
 */
public class NumericalFilterOptimizer implements FilterOptimizer {

  private static final Expression TRUE = RequestUtils.getLiteralExpression(true);
  private static final Expression FALSE = RequestUtils.getLiteralExpression(false);

  @Override
  public FilterQueryTree optimize(FilterQueryTree filterQueryTree, @Nullable Schema schema) {
    // Don't do anything here since this is for PQL queries which we no longer support.
    return filterQueryTree;
  }

  @Override
  public Expression optimize(Expression expression, @Nullable Schema schema) {
    ExpressionType type = expression.getType();
    if (type != ExpressionType.FUNCTION || schema == null) {
      // We have nothing to rewrite if expression is not a function or schema is null
      return expression;
    }

    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    FilterKind kind = FilterKind.valueOf(function.getOperator());
    switch (kind) {
      case AND:
      case OR:
        // Recursively traverse the expression tree to find an operator node that can be rewritten.
        operands.forEach(operand -> optimize(operand, schema));

        // We have rewritten the child operands, so rewrite the parent if needed.
        return optimizeCurrent(expression);
      case IS_NULL:
      case IS_NOT_NULL:
        // No need to try to optimize IS_NULL and IS_NOT_NULL operations on numerical columns.
        break;
      default:
        // Verify that LHS is a numeric column and RHS is a numeric literal before rewriting.
        Expression lhs = operands.get(0);
        Expression rhs = operands.get(1);
        if (isNumericColumn(lhs, schema) && isNumericLiteral(rhs)) {
          switch (kind) {
            case EQUALS:
            case NOT_EQUALS:
              return rewriteEqualsExpression(expression, kind, lhs, rhs, schema);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
              return rewriteRangeExpression(expression, kind, lhs, rhs, schema);
            default:
              break;
          }
        }
        break;
    }
    return expression;
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
      for (Expression operand : operands) {
        if (operand.equals(FALSE)) {
          return setExpressionToBoolean(expression, false);
        }
      }

      // Remove all Literal operands that are TRUE.
      operands.removeIf(x -> x.equals(TRUE));
      if (operands.isEmpty()) {
        return setExpressionToBoolean(expression, true);
      }
    } else if (function.getOperator().equals(FilterKind.OR.name())) {
      // If any of the literal operands are TRUE, then replace OR function with TRUE
      for (Expression operand : operands) {
        if (operand.equals(TRUE)) {
          return setExpressionToBoolean(expression, true);
        }
      }

      // Remove all Literal operands that are FALSE.
      operands.removeIf(x -> x.equals(FALSE));
      if (operands.isEmpty()) {
        return setExpressionToBoolean(expression, false);
      }
    }

    return expression;
  }

  // TODO DDC do we need to add cases to this method?
  /**
   * Rewrite expressions of form "column = literal" or "column != literal" to ensure that RHS literal is the same
   * datatype as LHS column.
   */
  private static Expression rewriteEqualsExpression(Expression equals, FilterKind kind, Expression lhs, Expression rhs,
      Schema schema) {
    // Get expression operator
    boolean result = kind == FilterKind.NOT_EQUALS;

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
              rhs.getLiteral().setLongValue(converted);
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
              rhs.getLiteral().setDoubleValue(converted);
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
              rhs.getLiteral().setDoubleValue(converted);
            }
            break;
          }
          default:
            break;
        }
        break;
      }
      case DOUBLE_VALUE: {
        double actual = rhs.getLiteral().getDoubleValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            if (converted != actual) {
              // Double value does not fall within the bounds of INT column.
              setExpressionToBoolean(equals, result);
            } else {
              // Replace double value with converted int value.
              rhs.getLiteral().setLongValue(converted);
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
              rhs.getLiteral().setLongValue(converted);
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
              rhs.getLiteral().setDoubleValue(converted);
            }
            break;
          }
          default:
            break;
        }
      }
      break;
      default:
        break;
    }
    return equals;
  }

  // TODO DDC do we need to add cases to this method?
  /**
   * Rewrite expressions of form "column > literal", "column >= literal", "column < literal", and "column <= literal"
   * to ensure that RHS literal is the same datatype as LHS column.
   */
  private static Expression rewriteRangeExpression(Expression range, FilterKind kind, Expression lhs, Expression rhs,
      Schema schema) {
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
            int comparison = Long.compare(actual, converted);
            if (comparison > 0) {
              // Literal value is greater than the bounds of INT. > and >= expressions will always be false because an
              // INT column can never have a value greater than Integer.MAX_VALUE. < and <= expressions will always be
              // true, because an INT column will always have values greater than or equal to Integer.MIN_VALUE and less
              // than or equal to Integer.MAX_VALUE.
              setExpressionToBoolean(range, kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (comparison < 0) {
              // Literal value is less than the bounds of INT. > and >= expressions will always be true because an
              // INT column will always have a value greater than or equal to Integer.MIN_VALUE. < and <= expressions
              // will always be false, because an INT column will never have values less than Integer.MIN_VALUE.
              setExpressionToBoolean(range,
                  kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL);
            } else {
              // Long literal value falls within the bounds of INT column, server will successfully convert the literal
              // value when needed.
            }
            break;
          }
          case FLOAT: {
            // Since we are converting a long value to float, float value will never be out of bounds (i.e -Infinity
            // or +Infinity).
            float converted = (float) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));

            // Rewrite range operator
            rewriteRangeOperator(range, kind, comparison);

            // Rewrite range literal
            rhs.getLiteral().setDoubleValue(converted);
            break;
          }
          case DOUBLE: {
            // long to double conversion is always within bounds of double datatype, but the conversion can be lossy.
            // Example:
            //   Original long value   : 9223372036854775807 (Long.MAX_VALUE)
            //   Converted double value: 9.223372036854776E18
            //   This conversion is lossy because the last four digits of the long value (5807) had to be rounded off
            //   to 6. After conversion we lost all information about the existence of last four digits. Converting the
            //   double value back to long will not result in original long value.
            double converted = (double) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));

            // Rewrite range operator
            rewriteRangeOperator(range, kind, comparison);

            // Rewrite range literal
            rhs.getLiteral().setDoubleValue(converted);
            break;
          }
          default:
            break;
        }
        break;
      }
      case DOUBLE_VALUE: {
        double actual = rhs.getLiteral().getDoubleValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            int comparison = Double.compare(actual, converted);
            if (comparison > 0 && converted == Integer.MAX_VALUE) {
              // Literal value is greater than the bounds of INT.
              setExpressionToBoolean(range, kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (comparison < 0 && converted == Integer.MIN_VALUE) {
              // Literal value is less than the bounds of INT.
              setExpressionToBoolean(range,
                  kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL);
            } else {
              // Literal value falls within the bounds of INT.
              rewriteRangeOperator(range, kind, comparison);

              // Rewrite range literal
              rhs.getLiteral().setLongValue(converted);
            }
            break;
          }
          case LONG: {
            long converted = (long) actual;
            int comparison = BigDecimal.valueOf(actual).compareTo(BigDecimal.valueOf(converted));

            if (comparison > 0 && converted == Long.MAX_VALUE) {
              // Literal value is greater than the bounds of LONG.
              setExpressionToBoolean(range, kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (comparison < 0 && converted == Long.MIN_VALUE) {
              // Literal value is less than the bounds of LONG.
              setExpressionToBoolean(range,
                  kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL);
            } else {
              // Rewrite range operator
              rewriteRangeOperator(range, kind, comparison);

              // Rewrite range literal
              rhs.getLiteral().setLongValue(converted);
            }
            break;
          }
          case FLOAT: {
            float converted = (float) actual;
            if (converted == Float.POSITIVE_INFINITY) {
              // Literal value is greater than the bounds of FLOAT
              setExpressionToBoolean(range, kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (converted == Float.NEGATIVE_INFINITY) {
              // Literal value is less than the bounds of LONG.
              setExpressionToBoolean(range,
                  kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL);
            } else {
              int comparison = Double.compare(actual, converted);
              // Rewrite range operator
              rewriteRangeOperator(range, kind, comparison);

              // Rewrite range literal
              rhs.getLiteral().setDoubleValue(converted);
            }
            break;
          }
          default:
            break;
        }
      }
      break;
      default:
        break;
    }
    return range;
  }

  /**
   * Helper function to rewrite range operator of a range expression.
   * @param range Range expression.
   * @param kind The kind of range filter being used in the range expression.
   * @param comparison -1 (literal < converted value), 0 (literal == converted value), 1 (literal > converted value).
   */
  private static void rewriteRangeOperator(Expression range, FilterKind kind, int comparison) {
    if (comparison > 0) {
      // Literal value is greater than the converted value, so rewrite:
      //   "column >  literal" to "column >  converted"
      //   "column >= literal" to "column >= converted"
      //   "column <  literal" to "column <= converted"
      //   "column <= literal" to "column <  converted"
      if (kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL) {
        range.getFunctionCall().setOperator(FilterKind.GREATER_THAN.name());
      } else if (kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL) {
        range.getFunctionCall().setOperator(FilterKind.LESS_THAN_OR_EQUAL.name());
      }
    } else if (comparison < 0) {
      // Literal value is less than the converted value, so rewrite:
      //   "column >  literal" to "column >= converted"
      //   "column >= literal" to "column >= converted"
      //   "column <  literal" to "column <  converted"
      //   "column <= literal" to "column <  converted"
      if (kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL) {
        range.getFunctionCall().setOperator(FilterKind.GREATER_THAN_OR_EQUAL.name());
      } else if (kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL) {
        range.getFunctionCall().setOperator(FilterKind.LESS_THAN.name());
      }
    } else {
      // No need to rewrite range expression since conversion of literal value was lossless.
    }
  }

  /** @return true if expression is a column of numeric type */
  private static boolean isNumericColumn(Expression expression, Schema schema) {
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

  /** @return true if expression is a numeric literal; otherwise, false. */
  private static boolean isNumericLiteral(Expression expression) {
    if (expression.getType() == ExpressionType.LITERAL) {
      Literal._Fields type = expression.getLiteral().getSetField();
      switch (type) {
        case SHORT_VALUE:
        case INT_VALUE:
        case LONG_VALUE:
        case DOUBLE_VALUE:
          return true;
        default:
          break;
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
}
