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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.sql.parsers.SqlCompilationException;

public class PredicateComparisonRewriter implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    if (pinotQuery.getFilterExpression() != null) {
      pinotQuery.setFilterExpression(updateBooleanPredicates(pinotQuery.getFilterExpression(), null));
      pinotQuery.setFilterExpression(updateComparisonPredicate(pinotQuery.getFilterExpression()));
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      pinotQuery.setHavingExpression(updateComparisonPredicate(havingExpression));
    }
    return pinotQuery;
  }

  /**
   * Updates boolean predicates that are missing an EQUALS filter.
   * E.g. 1:  'WHERE a' will be updated to 'WHERE a = true'
   * E.g. 2: "WHERE startsWith(col, 'str')" will be updated to "WHERE startsWith(col, 'str') = true"
   *
   * @param expression current expression in the expression tree
   * @param parentFunction parent expression
   * @return re-written expression.
   */
  private static Expression updateBooleanPredicates(Expression expression, Function parentFunction) {
    ExpressionType type = expression.getType();
    if (type == ExpressionType.FUNCTION) {
      Function function = expression.getFunctionCall();

      // If the function is not of FilterKind, we might have to rewrite the function if parentFunction is AND, OR or
      // NOT. Example: A query like "select col1 from table where startsWith(col1, 'myStr') AND col2 > 10;" should be
      // rewritten to "select col1 from table where startsWith(col1, 'myStr') = true AND col2 > 10;".
      if (!EnumUtils.isValidEnum(FilterKind.class, function.getOperator())) {

        // Rewrite non FilterKind functions if one of the two conditions are satified:
        // 1. parent function is empty. Example: "select * from table where startsWith(col, 'myStr');
        // 2. Separator is not a predicate (non predicates are AND, OR, NOT).
        //    Example: "select * from table where startsWith(col, 'myStr') AND col2 > 10";
        if (parentFunction == null || (EnumUtils.isValidEnum(FilterKind.class, parentFunction.getOperator())
            && !FilterKind.valueOf(parentFunction.getOperator()).isPredicate())) {
          Expression currExpression = expression.deepCopy();
          expression = RequestUtils.getFunctionExpression(FilterKind.EQUALS.name());
          List<Expression> operands = new ArrayList<>();
          operands.add(currExpression);
          operands.add(RequestUtils.getLiteralExpression(true));
          expression.getFunctionCall().setOperands(operands);

          return expression;
        }
      }

      List<Expression> operands = expression.getFunctionCall().getOperands();

      if (operands.size() > 0) {
        // unset operands only within the "if" condition because 'no argument' functions expect operand list to
        // be initialized.
        expression.getFunctionCall().unsetOperands();

        for (Expression exp : operands) {
          expression.getFunctionCall().addToOperands(updateBooleanPredicates(exp, expression.getFunctionCall()));
        }
      }
    } else if (type == ExpressionType.IDENTIFIER) {

      // Rewrite identifiers  if one of the two conditions are satified:
      // 1. parent function is empty. Example: "select * from table where col1";
      // 2. Separator is not a predicate (non predicates are AND, OR, NOT).
      //    Example: "select * from table where col1 AND col2 > 10";
      if (parentFunction == null || (EnumUtils.isValidEnum(FilterKind.class, parentFunction.getOperator())
          && !FilterKind.valueOf(parentFunction.getOperator()).isPredicate())) {
        Expression currExpression = expression.deepCopy();
        expression = RequestUtils.getFunctionExpression(FilterKind.EQUALS.name());
        List<Expression> operands = new ArrayList<>();
        operands.add(currExpression);
        operands.add(RequestUtils.getLiteralExpression(true));
        expression.getFunctionCall().setOperands(operands);
      }
    }

    return expression;
  }

  // This method converts a predicate expression to the what Pinot could evaluate.
  // For comparison expression, left operand could be any expression, but right operand only
  // supports literal.
  // E.g. 'WHERE a > b' will be updated to 'WHERE a - b > 0'
  private static Expression updateComparisonPredicate(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function != null) {
      FilterKind filterKind;
      try {
        filterKind = FilterKind.valueOf(function.getOperator());
      } catch (Exception e) {
        throw new SqlCompilationException("Unsupported filter kind: " + function.getOperator());
      }
      List<Expression> operands = function.getOperands();
      switch (filterKind) {
        case AND:
        case OR:
        case NOT:
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
            Expression minusExpression = RequestUtils.getFunctionExpression("minus");
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
