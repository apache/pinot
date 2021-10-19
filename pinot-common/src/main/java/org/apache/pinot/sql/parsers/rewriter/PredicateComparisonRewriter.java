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

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class PredicateComparisonRewriter implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      pinotQuery.setFilterExpression(updateComparisonPredicate(filterExpression));
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      pinotQuery.setHavingExpression(updateComparisonPredicate(havingExpression));
    }
    return pinotQuery;
  }

  // This method converts a predicate expression to the what Pinot could evaluate.
  // For comparison expression, left operand could be any expression, but right operand only
  // supports literal.
  // E.g. 'WHERE a > b' will be updated to 'WHERE a - b > 0'
  private static Expression updateComparisonPredicate(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function != null) {
      String operator = function.getOperator().toUpperCase();
      FilterKind filterKind;
      try {
        filterKind = FilterKind.valueOf(operator);
      } catch (Exception e) {
        throw new SqlCompilationException("Unsupported filter kind: " + operator);
      }
      List<Expression> operands = function.getOperands();
      switch (filterKind) {
        case AND:
        case OR:
          operands.replaceAll(PredicateComparisonRewriter::updateComparisonPredicate);
          break;
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          Expression firstOperand = operands.get(0);
          Expression secondOperand = operands.get(1);

          // Handle predicate like '10 = a' -> 'a = 10'
          if (firstOperand.isSetLiteral()) {
            if (!secondOperand.isSetLiteral()) {
              function.setOperator(getOppositeOperator(filterKind).name());
              operands.set(0, secondOperand);
              operands.set(1, firstOperand);
            }
            break;
          }

          // Handle predicate like 'a > b' -> 'a - b > 0'
          if (!secondOperand.isSetLiteral()) {
            Expression minusExpression = RequestUtils.getFunctionExpression(SqlKind.MINUS.name());
            minusExpression.getFunctionCall().setOperands(Arrays.asList(firstOperand, secondOperand));
            operands.set(0, minusExpression);
            operands.set(1, RequestUtils.getLiteralExpression(0));
            break;
          }
          break;
        default:
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (!operands.get(i).isSetLiteral()) {
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
