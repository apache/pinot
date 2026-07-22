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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class PredicateComparisonRewriter implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      pinotQuery.setFilterExpression(updatePredicate(filterExpression));
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      pinotQuery.setHavingExpression(updatePredicate(havingExpression));
    }
    return pinotQuery;
  }

  /**
   * This method converts an expression to what Pinot could evaluate.
   * 1. For comparison expressions, the right operand should be a literal. If the left operand is a
   *    literal and the right is not, they are swapped. If the right operand is still non-literal
   *    (column-to-column comparison), the predicate is rewritten using a comparison transform
   *    function: e.g. 'WHERE a = b' becomes 'WHERE equals(a, b) = true'.
   * 2. Updates boolean predicates (literals and scalar functions) that are missing an EQUALS filter.
   *    E.g. 1:  'WHERE a' will be updated to 'WHERE a = true'
   *    E.g. 2: "WHERE startsWith(col, 'str')" will be updated to "WHERE startsWith(col, 'str') = true"
   *
   * @param expression current expression in the expression tree
   * @return re-written expression.
   */
  private static Expression updatePredicate(Expression expression) {
    ExpressionType type = expression.getType();

    switch (type) {
      case FUNCTION:
        return updateFunctionExpression(expression);
      case IDENTIFIER:
        return convertPredicateToEqualsBooleanExpression(expression);
      case LITERAL:
        // TODO: Convert literals to boolean expressions
        return expression;
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Rewrites a function expression.
   *
   * @param expression
   * @return re-written expression
   */
  private static Expression updateFunctionExpression(Expression expression) {
    Function function = expression.getFunctionCall();
    String functionOperator = function.getOperator();

    if (!EnumUtils.isValidEnum(FilterKind.class, functionOperator)) {
      // If the function is not of FilterKind, we have to rewrite the function.
      // Example: A query like "select col1 from table where startsWith(col1, 'myStr') AND col2 > 10;" should be
      //          rewritten to "select col1 from table where startsWith(col1, 'myStr') = true AND col2 > 10;".
      expression = convertPredicateToEqualsBooleanExpression(expression);
      return expression;
    } else {
      FilterKind filterKind = FilterKind.valueOf(function.getOperator());
      List<Expression> operands = function.getOperands();
      switch (filterKind) {
        case AND:
        case OR:
        case NOT:
          operands.replaceAll(PredicateComparisonRewriter::updatePredicate);
          break;
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          Expression firstOperand = operands.get(0);
          Expression secondOperand = operands.get(1);
          boolean firstOperandIsLiteral = RequestContextUtils.canEvaluateLiteral(firstOperand);
          boolean secondOperandIsLiteral = RequestContextUtils.canEvaluateLiteral(secondOperand);

          // Handle predicate like '10 = a' -> 'a = 10'
          if (firstOperandIsLiteral) {
            if (!secondOperandIsLiteral) {
              function.setOperator(getOppositeOperator(filterKind).name());
              operands.set(0, secondOperand);
              operands.set(1, firstOperand);
            }
            break;
          }

          // Handle predicate like 'a > b' -> 'comparison(a, b) = true'
          if (!secondOperandIsLiteral) {
            Expression comparisonExpression = RequestUtils.getFunctionExpression(
                getComparisonFunctionName(filterKind), firstOperand, secondOperand);
            function.setOperator(FilterKind.EQUALS.name());
            operands.set(0, comparisonExpression);
            operands.set(1, RequestUtils.getLiteralExpression(true));
            break;
          }
          break;
        case SEMANTIC_MATCH: {
          // SEMANTIC_MATCH(column, 'query text', topK) — pass through, rewritten by semantic rewriter
          Preconditions.checkArgument(operands.size() >= 2 && operands.size() <= 3,
              "For %s predicate, the number of operands must be 2 or 3, got: %s", filterKind, expression);
          break;
        }
        case VECTOR_SIMILARITY: {
          Preconditions.checkArgument(operands.size() >= 2 && operands.size() <= 3,
              "For %s predicate, the number of operands must be at either 2 or 3, got: %s", filterKind, expression);
          /*
           * Array Literal could be either:
           * 1. a function of type 'ARRAYVALUECONSTRUCTOR' with operands of float/double
           * 2. a float/double array literals
           * Also check in {@link org.apache.pinot.sql.parsers.CalciteSqlParser#validateFilter(Expression)}}
           */
          if ((operands.get(1).getFunctionCall() != null && !operands.get(1).getFunctionCall().getOperator()
              .equalsIgnoreCase("arrayvalueconstructor"))
              || (operands.get(1).getLiteral() != null && !operands.get(1).getLiteral().isSetFloatArrayValue()
                  && !operands.get(1).getLiteral().isSetDoubleArrayValue())) {
            throw new SqlCompilationException(
                String.format("For %s predicate, the second operand must be a float/double array literal, got: %s",
                    filterKind,
                    expression));
          }
          if (operands.size() == 3 && operands.get(2).getLiteral() == null) {
            throw new SqlCompilationException(
                String.format("For %s predicate, the third operand must be a literal, got: %s", filterKind,
                    expression));
          }
          break;
        }
        case VECTOR_SIMILARITY_RADIUS: {
          Preconditions.checkArgument(operands.size() >= 2 && operands.size() <= 3,
              "For %s predicate, the number of operands must be at either 2 or 3, got: %s", filterKind, expression);
          if ((operands.get(1).getFunctionCall() != null && !operands.get(1).getFunctionCall().getOperator()
              .equalsIgnoreCase("arrayvalueconstructor"))
              || (operands.get(1).getLiteral() != null && !operands.get(1).getLiteral().isSetFloatArrayValue()
                  && !operands.get(1).getLiteral().isSetDoubleArrayValue())) {
            throw new SqlCompilationException(
                String.format("For %s predicate, the second operand must be a float/double array literal, got: %s",
                    filterKind,
                    expression));
          }
          if (operands.size() == 3 && operands.get(2).getLiteral() == null) {
            throw new SqlCompilationException(
                String.format("For %s predicate, the third operand must be a literal, got: %s", filterKind,
                    expression));
          }
          break;
        }
        default:
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (!RequestContextUtils.canEvaluateLiteral(operands.get(i))) {
              throw new SqlCompilationException(
                  String.format("For %s predicate, the operands except for the first one must be literal, got: %s",
                      filterKind, expression));
            }
          }
          break;
      }
    }

    return expression;
  }

  /**
   * Rewrite predicates to boolean expressions with EQUALS operator
   *     Example1: "select * from table where col1" converts to
   *               "select * from table where col1 = true"
   *     Example2: "select * from table where startsWith(col1, 'str')" converts to
   *               "select * from table where startsWith(col1, 'str') = true"
   *
   * @param expression Expression
   * @return Rewritten expression
   */
  private static Expression convertPredicateToEqualsBooleanExpression(Expression expression) {
    return RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), expression,
        RequestUtils.getLiteralExpression(true));
  }

  private static String getComparisonFunctionName(FilterKind filterKind) {
    switch (filterKind) {
      case EQUALS:
        return TransformFunctionType.EQUALS.getName();
      case NOT_EQUALS:
        return TransformFunctionType.NOT_EQUALS.getName();
      case GREATER_THAN:
        return TransformFunctionType.GREATER_THAN.getName();
      case GREATER_THAN_OR_EQUAL:
        return TransformFunctionType.GREATER_THAN_OR_EQUAL.getName();
      case LESS_THAN:
        return TransformFunctionType.LESS_THAN.getName();
      case LESS_THAN_OR_EQUAL:
        return TransformFunctionType.LESS_THAN_OR_EQUAL.getName();
      default:
        throw new IllegalStateException("Unsupported comparison operator: " + filterKind);
    }
  }

  /**
   * The purpose of this method is to convert expression "0 < columnA" to "columnA > 0".
   * The conversion would be:
   *  from ">" to "<",
   *  from "<" to ">",
   *  from ">=" to "<=",
   *  from "<=" to ">=".
   */
  private static FilterKind getOppositeOperator(FilterKind filterKind) {
    switch (filterKind) {
      case GREATER_THAN:
        return FilterKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return FilterKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return FilterKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return FilterKind.GREATER_THAN_OR_EQUAL;
      default:
        // Do nothing
        return filterKind;
    }
  }
}
