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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class BrokerRequestToQueryContextConverter {
  private BrokerRequestToQueryContextConverter() {
  }

  /**
   * Converts the given {@link BrokerRequest} into a {@link QueryContext}.
   */
  public static QueryContext convert(BrokerRequest brokerRequest) {
    if (brokerRequest.getPinotQuery() != null) {
      QueryContext queryContext = convertSQL(brokerRequest.getPinotQuery(), brokerRequest);
      return queryContext;
    } else {
      return convertPQL(brokerRequest);
    }
  }

  private static QueryContext convertSQL(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    QueryContext subquery = null;
    if (pinotQuery.getDataSource().getSubquery() != null) {
      subquery = convertSQL(pinotQuery.getDataSource().getSubquery(), brokerRequest);
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

  private static QueryContext convertPQL(BrokerRequest brokerRequest) {
    List<ExpressionContext> selectExpressions;
    List<ExpressionContext> groupByExpressions = null;
    int limit = brokerRequest.getLimit();
    int offset = 0;
    Selection selections = brokerRequest.getSelections();
    if (selections != null) {
      // Selection query
      List<String> selectionColumns = selections.getSelectionColumns();
      selectExpressions = new ArrayList<>(selectionColumns.size());
      for (String expression : selectionColumns) {
        selectExpressions.add(RequestContextUtils.getExpressionFromPQL(expression));
      }

      // NOTE: Some old Pinot clients (E.g. Presto segment level query) set LIMIT in Selection object.
      if (limit == 0) {
        limit = selections.getSize();
      }

      offset = selections.getOffset();
    } else {
      // Aggregation query
      List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
      selectExpressions = new ArrayList<>(aggregationsInfo.size());
      for (AggregationInfo aggregationInfo : aggregationsInfo) {
        String functionName = StringUtils.remove(aggregationInfo.getAggregationType(), '_');
        List<String> stringExpressions = aggregationInfo.getExpressions();
        int numArguments = stringExpressions.size();
        List<ExpressionContext> arguments = new ArrayList<>(numArguments);
        if (functionName.equalsIgnoreCase(AggregationFunctionType.DISTINCT.getName())) {
          // For DISTINCT query, all arguments are expressions
          for (String expression : stringExpressions) {
            arguments.add(RequestContextUtils.getExpressionFromPQL(expression));
          }
        } else if (functionName.equalsIgnoreCase(AggregationFunctionType.LASTWITHTIME.getName())) {
          // For LASTWITHTIME query, only the first two arguments are expression, third one is literal if available
          arguments.add(RequestContextUtils.getExpressionFromPQL(stringExpressions.get(0)));
          arguments.add(RequestContextUtils.getExpressionFromPQL(stringExpressions.get(1)));
          for (int i = 2; i < numArguments; i++) {
            arguments.add(ExpressionContext.forLiteral(stringExpressions.get(i)));
          }
        } else {
          // For non-DISTINCT query, only the first argument is expression, others are literals
          // NOTE: We directly use the string as the literal value because of the legacy behavior of PQL compiler
          //       treating string literal as identifier in the aggregation function.
          arguments.add(RequestContextUtils.getExpressionFromPQL(stringExpressions.get(0)));
          for (int i = 1; i < numArguments; i++) {
            arguments.add(ExpressionContext.forLiteral(stringExpressions.get(i)));
          }
        }
        FunctionContext function = new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments);
        selectExpressions.add(ExpressionContext.forFunction(function));
      }

      GroupBy groupBy = brokerRequest.getGroupBy();
      if (groupBy != null) {
        // Aggregation group-by query
        List<String> stringExpressions = groupBy.getExpressions();
        groupByExpressions = new ArrayList<>(stringExpressions.size());
        for (String stringExpression : stringExpressions) {
          groupByExpressions.add(RequestContextUtils.getExpressionFromPQL(stringExpression));
        }

        // NOTE: Use TOP in GROUP-BY clause as LIMIT for backward-compatibility.
        limit = (int) groupBy.getTopN();
      }
    }

    FilterContext filter = null;
    FilterQueryTree rootFilterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (rootFilterQueryTree != null) {
      filter = RequestContextUtils.getFilter(rootFilterQueryTree);
    }

    List<OrderByExpressionContext> orderByExpressions = null;
    List<SelectionSort> orderBy = brokerRequest.getOrderBy();
    if (CollectionUtils.isNotEmpty(orderBy)) {
      // Deduplicate the order-by expressions
      orderByExpressions = new ArrayList<>(orderBy.size());
      Set<ExpressionContext> expressionSet = new HashSet<>();
      for (SelectionSort selectionSort : orderBy) {
        ExpressionContext expression = RequestContextUtils.getExpressionFromPQL(selectionSort.getColumn());
        if (expressionSet.add(expression)) {
          orderByExpressions.add(new OrderByExpressionContext(expression, selectionSort.isIsAsc()));
        }
      }
    }

    return new QueryContext.Builder().setTableName(brokerRequest.getQuerySource().getTableName())
        .setSelectExpressions(selectExpressions).setAliasList(Collections.emptyList()).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions).setLimit(limit)
        .setOffset(offset).setQueryOptions(brokerRequest.getQueryOptions())
        .setDebugOptions(brokerRequest.getDebugOptions()).setBrokerRequest(brokerRequest).build();
  }
}
