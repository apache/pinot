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
package org.apache.pinot.core.query.request.context.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class QueryContextConverterUtils {
  private QueryContextConverterUtils() {
  }

  /**
   * Converts the given query into a {@link QueryContext}.
   */
  public static QueryContext getQueryContext(String query) {
    return getQueryContext(CalciteSqlParser.compileToPinotQuery(query));
  }

  /**
   * Converts the given {@link PinotQuery} into a {@link QueryContext}.
   */
  public static QueryContext getQueryContext(PinotQuery pinotQuery) {
    // FROM
    String tableName;
    DataSource dataSource = pinotQuery.getDataSource();
    tableName = dataSource.getTableName();
    QueryContext subquery = null;
    if (dataSource.getSubquery() != null) {
      subquery = getQueryContext(dataSource.getSubquery());
    }

    // SELECT
    List<ExpressionContext> selectExpressions;
    List<Expression> selectList = pinotQuery.getSelectList();
    List<String> aliasList = new ArrayList<>(selectList.size());
    selectExpressions = new ArrayList<>(selectList.size());
    for (Expression thriftExpression : selectList) {
      // Handle alias
      Expression expressionWithoutAlias = thriftExpression;
      if (thriftExpression.getType() == ExpressionType.FUNCTION) {
        Function function = thriftExpression.getFunctionCall();
        List<Expression> operands = function.getOperands();
        switch (function.getOperator().toUpperCase()) {
          case "AS":
            expressionWithoutAlias = operands.get(0);
            aliasList.add(operands.get(1).getIdentifier().getName());
            break;
          case "DISTINCT":
            int numOperands = operands.size();
            for (int i = 0; i < numOperands; i++) {
              Expression operand = operands.get(i);
              Function operandFunction = operand.getFunctionCall();
              if (operandFunction != null && operandFunction.getOperator().equalsIgnoreCase("AS")) {
                operands.set(i, operandFunction.getOperands().get(0));
                aliasList.add(operandFunction.getOperands().get(1).getIdentifier().getName());
              } else {
                aliasList.add(null);
              }
            }
            break;
          default:
            // Add null as a placeholder for alias.
            aliasList.add(null);
            break;
        }
      } else {
        // Add null as a placeholder for alias.
        aliasList.add(null);
      }
      selectExpressions.add(RequestContextUtils.getExpression(expressionWithoutAlias));
    }

    // WHERE
    FilterContext filter = null;
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      filter = RequestContextUtils.getFilter(filterExpression);
    }

    // GROUP BY
    List<ExpressionContext> groupByExpressions = null;
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (CollectionUtils.isNotEmpty(groupByList)) {
      groupByExpressions = new ArrayList<>(groupByList.size());
      for (Expression thriftExpression : groupByList) {
        groupByExpressions.add(RequestContextUtils.getExpression(thriftExpression));
      }
    }

    // ORDER BY
    List<OrderByExpressionContext> orderByExpressions = null;
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (CollectionUtils.isNotEmpty(orderByList)) {
      orderByExpressions = new ArrayList<>(orderByList.size());
      Set<Expression> seen = new HashSet<>();
      for (Expression orderBy : orderByList) {
        boolean isAsc = isAsc(orderBy);
        Boolean isNullsLast = isNullsLast(orderBy);
        Expression orderByFunctionsRemoved = CalciteSqlParser.removeOrderByFunctions(orderBy);
        // Deduplicate the order-by expressions
        if (seen.add(orderByFunctionsRemoved)) {
          ExpressionContext expressionContext = RequestContextUtils.getExpression(orderByFunctionsRemoved);
          if (isNullsLast != null) {
            orderByExpressions.add(new OrderByExpressionContext(expressionContext, isAsc, isNullsLast));
          } else {
            orderByExpressions.add(new OrderByExpressionContext(expressionContext, isAsc));
          }
        }
      }
    }

    // HAVING
    FilterContext havingFilter = null;
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      havingFilter = RequestContextUtils.getFilter(havingExpression);
    }

    // EXPRESSION OVERRIDE HINTS
    Map<ExpressionContext, ExpressionContext> expressionContextOverrideHints = new HashMap<>();
    Map<Expression, Expression> expressionOverrideHints = pinotQuery.getExpressionOverrideHints();
    if (expressionOverrideHints != null) {
      for (Map.Entry<Expression, Expression> entry : expressionOverrideHints.entrySet()) {
        expressionContextOverrideHints.put(RequestContextUtils.getExpression(entry.getKey()),
            RequestContextUtils.getExpression(entry.getValue()));
      }
    }

    return new QueryContext.Builder().setTableName(tableName).setSubquery(subquery)
        .setSelectExpressions(selectExpressions).setAliasList(aliasList).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions)
        .setHavingFilter(havingFilter).setLimit(pinotQuery.getLimit()).setOffset(pinotQuery.getOffset())
        .setQueryOptions(pinotQuery.getQueryOptions()).setExpressionOverrideHints(expressionContextOverrideHints)
        .setExplain(pinotQuery.isExplain()).build();
  }

  private static boolean isAsc(Expression expression) {
    while (expression != null && expression.isSetFunctionCall()) {
      if (expression.getFunctionCall().getOperator().equals(CalciteSqlParser.ASC)) {
        return true;
      }
      expression = expression.getFunctionCall().getOperands().get(0);
    }
    return false;
  }

  @Nullable
  private static Boolean isNullsLast(Expression expression) {
    while (expression != null && expression.isSetFunctionCall()) {
      String operator = expression.getFunctionCall().getOperator();
      if (operator.equals(CalciteSqlParser.NULLS_LAST)) {
        return true;
      } else if (operator.equals(CalciteSqlParser.NULLS_FIRST)) {
        return false;
      }
      expression = expression.getFunctionCall().getOperands().get(0);
    }
    return null;
  }
}
