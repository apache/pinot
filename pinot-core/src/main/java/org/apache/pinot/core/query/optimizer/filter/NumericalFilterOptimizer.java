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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * Numerical expressions of the form "column [operator] literal", where operator can be '=', '!=', '>', '>=', '<', '<=',
 * or 'BETWEEN' can compare a column of one datatype (say INT) with a literal of different datatype (say DOUBLE). These
 * expressions can not be evaluated on the Server. Hence, we rewrite such expressions into an equivalent expression
 * whose LHS and RHS are of the same datatype.
 * <p>
 * Simple predicate examples:
 * <ol>
 *  <li> "WHERE intColumn = 5.0"  gets rewritten to "WHERE intColumn = 5"
 *  <li> "WHERE intColumn != 5.0" gets rewritten to "WHERE intColumn != 5"
 *  <li> "WHERE intColumn = 5.5"  gets rewritten to "WHERE false" because INT values can not match 5.5.
 *  <li> "WHERE intColumn = 3000000000" gets rewritten to "WHERE false" because INT values can not match 3000000000.
 *  <li> "WHERE intColumn != 3000000000" gets rewritten to "WHERE true" because INT values always not equal to
 *  3000000000.
 *  <li> "WHERE intColumn < 5.1" gets rewritten to "WHERE intColumn <= 5"
 *  <li> "WHERE intColumn > -3E9" gets rewritten to "WHERE true" because int values are always greater than -3E9.
 *  <li> "WHERE intColumn BETWEEN 2.5 AND 7.5" gets rewritten to "WHERE intColumn BETWEEN 3 AND 7"
 *  <li> "WHERE intColumn BETWEEN 5.5 AND 3000000000" gets rewritten to "WHERE intColumn BETWEEN 6 AND 2147483647" since
 *  3000000000 is greater than Integer.MAX_VALUE.
 *  <li> "WHERE intColumn BETWEEN 10 AND 0" gets rewritten to "WHERE false" because lower bound is greater than upper
 *  bound.
 * </ol>
 * <p>
 * Compound predicate examples:
 * <ol>
 *  <li> "WHERE intColumn1 = 5.5 AND intColumn2 = intColumn3"
 *       rewrite to "WHERE false AND intColumn2 = intColumn3"
 *       rewrite to "WHERE intColumn2 = intColumn3"
 *  <li> "WHERE intColumn1 != 5.5 OR intColumn2 = 5000000000" (5000000000 is out of bounds for integer column)
 *       rewrite to "WHERE true OR false"
 *       rewrite to "WHERE true"
 *       rewrite to query without any WHERE clause.
 * </ol>
 * <p>
 * When entire predicate gets rewritten to false (Example 3 above), the query will not return any data. Hence, it is
 * better for the Broker itself to return an empty response rather than sending the query to servers for further
 * evaluation.
 * <p>
 * TODO: Add support for IN, and NOT IN operators.
 */
public class NumericalFilterOptimizer extends BaseAndOrBooleanFilterOptimizer {

  @Override
  boolean canBeOptimized(Expression filterExpression, @Nullable Schema schema) {
    ExpressionType type = filterExpression.getType();
    // We have nothing to rewrite if expression is not a function or schema is null
    return type == ExpressionType.FUNCTION && schema != null;
  }

  @Override
  Expression optimizeChild(Expression filterExpression, @Nullable Schema schema) {
    Function function = filterExpression.getFunctionCall();
    FilterKind kind = FilterKind.valueOf(function.getOperator());

    if (!kind.isRange() && kind != FilterKind.EQUALS && kind != FilterKind.NOT_EQUALS) {
      return filterExpression;
    }

    List<Expression> operands = function.getOperands();
    // Verify that LHS is a numeric column and RHS is a literal before rewriting.
    Expression lhs = operands.get(0);
    Expression rhs = operands.get(1);

    DataType dataType = getDataType(lhs, schema);
    if (dataType == null || !dataType.isNumeric() || !rhs.isSetLiteral()) {
      // No rewrite here
      return filterExpression;
    }

    switch (kind) {
      case BETWEEN: {
        return rewriteBetweenExpression(filterExpression, dataType);
      }
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL: {
        if (kind.isRange()) {
          return rewriteRangeExpression(filterExpression, kind, dataType, rhs);
        } else {
          return rewriteEqualsExpression(filterExpression, kind, dataType, rhs);
        }
      }
      default:
        break;
    }
    return filterExpression;
  }

  /**
   * Rewrite expressions of form "column = literal" or "column != literal" to ensure that RHS literal is the same
   * datatype as LHS column.
   */
  private static Expression rewriteEqualsExpression(Expression equals, FilterKind kind, DataType dataType,
      Expression rhs) {
    // Get expression operator
    boolean result = kind == FilterKind.NOT_EQUALS;

    switch (rhs.getLiteral().getSetField()) {
      case INT_VALUE:
        // No rewrites needed since INT conversion to numeric column types can be handled on the server side.
        break;
      case LONG_VALUE: {
        long actual = rhs.getLiteral().getLongValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            if (converted != actual) {
              // Long value does not fall within the bounds of INT column.
              return getExpressionFromBoolean(result);
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
              return getExpressionFromBoolean(result);
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
              return getExpressionFromBoolean(result);
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
      // TODO: Add support for FLOAT_VALUE
//      case FLOAT_VALUE: {
//        float actual = Float.intBitsToFloat(rhs.getLiteral().getFloatValue());
//        break;
//      }
      case DOUBLE_VALUE: {
        double actual = rhs.getLiteral().getDoubleValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            if (converted != actual) {
              // Double value does not fall within the bounds of INT column.
              return getExpressionFromBoolean(result);
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
              return getExpressionFromBoolean(result);
            } else {
              // Replace double value with converted long value.
              rhs.getLiteral().setLongValue(converted);
            }
            break;
          }
          default:
            break;
        }
        break;
      }
      default:
        break;
    }
    return equals;
  }

  /**
   * Rewrite expressions of form "column > literal", "column >= literal", "column < literal", and "column <= literal"
   * to ensure that RHS literal is the same datatype as LHS column.
   */
  private static Expression rewriteRangeExpression(Expression range, FilterKind kind, DataType dataType,
      Expression rhs) {
    switch (rhs.getLiteral().getSetField()) {
      case INT_VALUE:
        // No rewrites needed since INT conversion to numeric column types can be handled on the server side.
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
              return getExpressionFromBoolean(kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (comparison < 0) {
              // Literal value is less than the bounds of INT. > and >= expressions will always be true because an
              // INT column will always have a value greater than or equal to Integer.MIN_VALUE. < and <= expressions
              // will always be false, because an INT column will never have values less than Integer.MIN_VALUE.
              return getExpressionFromBoolean(
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
      // TODO: Add support for FLOAT_VALUE
//      case FLOAT_VALUE: {
//        float actual = Float.intBitsToFloat(rhs.getLiteral().getFloatValue());
//        break;
//      }
      case DOUBLE_VALUE: {
        double actual = rhs.getLiteral().getDoubleValue();
        switch (dataType) {
          case INT: {
            int converted = (int) actual;
            int comparison = Double.compare(actual, converted);
            if (comparison > 0 && converted == Integer.MAX_VALUE) {
              // Literal value is greater than the bounds of INT.
              return getExpressionFromBoolean(kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (comparison < 0 && converted == Integer.MIN_VALUE) {
              // Literal value is less than the bounds of INT.
              return getExpressionFromBoolean(
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
              return getExpressionFromBoolean(kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (comparison < 0 && converted == Long.MIN_VALUE) {
              // Literal value is less than the bounds of LONG.
              return getExpressionFromBoolean(
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
              return getExpressionFromBoolean(kind == FilterKind.LESS_THAN || kind == FilterKind.LESS_THAN_OR_EQUAL);
            } else if (converted == Float.NEGATIVE_INFINITY) {
              // Literal value is less than the bounds of LONG.
              return getExpressionFromBoolean(
                  kind == FilterKind.GREATER_THAN || kind == FilterKind.GREATER_THAN_OR_EQUAL);
            }
            // Do not rewrite range operator since double has higher precision than float
            // If we do, we may introduce problems.
            // For example, in the previous logic, "> 0.05" will be converted into ">= 0.05000000074505806". When the
            // query reaches a server, the server will convert it to ">= 0.05" in
            // ColumnValueSegmentPruner#pruneRangePredicate, which is incorrect.
            break;
          }
          default:
            break;
        }
        break;
      }
      default:
        break;
    }
    return range;
  }

  /**
   * Rewrite expressions of the form "column BETWEEN lower AND upper" to ensure that lower and upper bounds are the same
   * datatype as the column (or can be cast to the same datatype in the server).
   */
  private static Expression rewriteBetweenExpression(Expression between, DataType dataType) {
    // TODO: Consider unifying logic with rewriteRangeExpression
    List<Expression> operands = between.getFunctionCall().getOperands();
    Expression lower = operands.get(1);
    Expression upper = operands.get(2);

    // The BETWEEN filter predicate currently only supports literals as lower and upper bounds, but we're still checking
    // here just in case.
    if (lower.isSetLiteral()) {
      switch (lower.getLiteral().getSetField()) {
        case LONG_VALUE: {
          long actual = lower.getLiteral().getLongValue();
          // Other data types can be converted on the server side.
          if (dataType == DataType.INT) {
            if (actual > Integer.MAX_VALUE) {
              // Lower bound literal value is greater than the bounds of INT.
              return getExpressionFromBoolean(false);
            }
            if (actual < Integer.MIN_VALUE) {
              lower.getLiteral().setIntValue(Integer.MIN_VALUE);
            }
          }
          break;
        }
        case DOUBLE_VALUE: {
          double actual = lower.getLiteral().getDoubleValue();

          switch (dataType) {
            case INT: {
              if (actual > Integer.MAX_VALUE) {
                // Lower bound literal value is greater than the bounds of INT.
                return getExpressionFromBoolean(false);
              }
              if (actual < Integer.MIN_VALUE) {
                lower.getLiteral().setIntValue(Integer.MIN_VALUE);
              } else {
                // Double value is in int range
                int converted = (int) actual;
                int comparison = BigDecimal.valueOf(converted).compareTo(BigDecimal.valueOf(actual));
                if (comparison >= 0) {
                  lower.getLiteral().setIntValue(converted);
                } else {
                  lower.getLiteral().setIntValue(converted + 1);
                }
              }
              break;
            }
            case LONG: {
              if (actual > Long.MAX_VALUE) {
                // Lower bound literal value is greater than the bounds of LONG.
                return getExpressionFromBoolean(false);
              }
              if (actual < Long.MIN_VALUE) {
                lower.getLiteral().setLongValue(Long.MIN_VALUE);
              } else {
                // Double value is in long range
                long converted = (long) actual;
                int comparison = BigDecimal.valueOf(converted).compareTo(BigDecimal.valueOf(actual));
                if (comparison >= 0) {
                  lower.getLiteral().setLongValue(converted);
                } else {
                  lower.getLiteral().setLongValue(converted + 1);
                }
              }
              break;
            }
            default:
              // For other numeric data types, the double literal can be converted on the server side.
              break;
          }
          break;
        }
        default:
          break;
      }
    }

    if (upper.isSetLiteral()) {
      switch (upper.getLiteral().getSetField()) {
        case LONG_VALUE: {
          long actual = upper.getLiteral().getLongValue();
          // Other data types can be converted on the server side.
          if (dataType == DataType.INT) {
            if (actual < Integer.MIN_VALUE) {
              // Upper bound literal value is lesser than the bounds of INT.
              return getExpressionFromBoolean(false);
            }
            if (actual > Integer.MAX_VALUE) {
              upper.getLiteral().setIntValue(Integer.MAX_VALUE);
            }
          }
          break;
        }
        case DOUBLE_VALUE: {
          double actual = upper.getLiteral().getDoubleValue();

          switch (dataType) {
            case INT: {
              if (actual < Integer.MIN_VALUE) {
                // Upper bound literal value is lesser than the bounds of INT.
                return getExpressionFromBoolean(false);
              }
              if (actual > Integer.MAX_VALUE) {
                upper.getLiteral().setIntValue(Integer.MAX_VALUE);
              } else {
                // Double value is in int range
                int converted = (int) actual;
                int comparison = BigDecimal.valueOf(converted).compareTo(BigDecimal.valueOf(actual));
                if (comparison <= 0) {
                  upper.getLiteral().setIntValue(converted);
                } else {
                  upper.getLiteral().setIntValue(converted - 1);
                }
              }
              break;
            }
            case LONG: {
              if (actual < Long.MIN_VALUE) {
                // Upper bound literal value is lesser than the bounds of LONG.
                return getExpressionFromBoolean(false);
              }
              if (actual > Long.MAX_VALUE) {
                upper.getLiteral().setLongValue(Long.MAX_VALUE);
              } else {
                // Double value is in long range
                long converted = (long) actual;
                int comparison = BigDecimal.valueOf(converted).compareTo(BigDecimal.valueOf(actual));
                if (comparison <= 0) {
                  upper.getLiteral().setLongValue(converted);
                } else {
                  upper.getLiteral().setLongValue(converted - 1);
                }
              }
              break;
            }
            default:
              // For other numeric data types, the double literal can be converted on the server side.
              break;
          }
          break;
        }
        default:
          break;
      }
    }

    return between;
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
      //   "column >= literal" to "column >  converted"
      //   "column <  literal" to "column <= converted"
      //   "column <= literal" to "column <= converted"
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

  /** @return field data type extracted from the expression. null if we can't determine the type. */
  @Nullable
  private static DataType getDataType(Expression expression, Schema schema) {
    if (expression.getType() == ExpressionType.IDENTIFIER) {
      String column = expression.getIdentifier().getName();
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      if (fieldSpec != null && fieldSpec.isSingleValueField()) {
        return fieldSpec.getDataType();
      }
    } else if (expression.getType() == ExpressionType.FUNCTION && "cast".equalsIgnoreCase(
        expression.getFunctionCall().getOperator())) {
      // expression is not identifier but we can also determine the data type.
      String targetTypeLiteral =
          expression.getFunctionCall().getOperands().get(1).getLiteral().getStringValue().toUpperCase();
      DataType dataType;

      // Strip out _ARRAY suffix that can be used to represent an MV field type since the semantics here will be the
      // same as that for the equivalent SV field of the same type
      if (targetTypeLiteral.endsWith("_ARRAY")) {
        targetTypeLiteral = targetTypeLiteral.substring(0, targetTypeLiteral.length() - 6);
      }

      if ("INTEGER".equals(targetTypeLiteral)) {
        dataType = DataType.INT;
      } else if ("VARCHAR".equals(targetTypeLiteral)) {
        dataType = DataType.STRING;
      } else if ("TIMESTAMP_WITH_LOCAL_TIME_ZONE".equals(targetTypeLiteral)) {
        dataType = DataType.TIMESTAMP;
      } else if ("TIMESTAMP".equals(targetTypeLiteral)) {
        dataType = DataType.TIMESTAMP_NTZ;
      } else {
        dataType = DataType.valueOf(targetTypeLiteral);
      }
      return dataType;
    }
    return null;
  }
}
