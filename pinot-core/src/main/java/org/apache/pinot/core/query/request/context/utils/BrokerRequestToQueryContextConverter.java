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
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.query.request.context.QueryContext;


public class BrokerRequestToQueryContextConverter {
  private BrokerRequestToQueryContextConverter() {
  }

  /**
   * Converts the given {@link BrokerRequest} into a {@link QueryContext}.
   */
  public static QueryContext convert(BrokerRequest brokerRequest) {
    return convert(brokerRequest.getPinotQuery(), brokerRequest);
  }

  private static QueryContext convert(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    QueryContext subquery = null;
    if (pinotQuery.getDataSource().getSubquery() != null) {
      subquery = convert(pinotQuery.getDataSource().getSubquery(), brokerRequest);
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
      // Deduplicate the order-by expressions
      orderByExpressions = new ArrayList<>(orderByList.size());
      Set<ExpressionContext> expressionSet = new HashSet<>();
      for (Expression orderBy : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        Function thriftFunction = orderBy.getFunctionCall();
        ExpressionContext expression = RequestContextUtils.getExpression(thriftFunction.getOperands().get(0));
        if (expressionSet.add(expression)) {
          boolean isAsc = thriftFunction.getOperator().equalsIgnoreCase("ASC");
          orderByExpressions.add(new OrderByExpressionContext(expression, isAsc));
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

    return new QueryContext.Builder().setTableName(pinotQuery.getDataSource().getTableName())
        .setSelectExpressions(selectExpressions).setAliasList(aliasList).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions)
        .setHavingFilter(havingFilter).setLimit(pinotQuery.getLimit()).setOffset(pinotQuery.getOffset())
        .setQueryOptions(pinotQuery.getQueryOptions()).setDebugOptions(pinotQuery.getDebugOptions())
        .setSubquery(subquery).setExpressionOverrideHints(expressionContextOverrideHints)
        .setBrokerRequest(brokerRequest).build();
  }
}
