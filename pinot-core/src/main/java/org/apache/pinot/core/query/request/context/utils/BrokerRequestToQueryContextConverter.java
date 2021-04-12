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
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class BrokerRequestToQueryContextConverter {
  private BrokerRequestToQueryContextConverter() {
  }

  /**
   * Converts the given {@link BrokerRequest} into a {@link QueryContext}.
   */
  public static QueryContext convert(BrokerRequest brokerRequest) {
    return brokerRequest.getPinotQuery() != null ? convertSQL(brokerRequest) : convertPQL(brokerRequest);
  }

  private static QueryContext convertSQL(BrokerRequest brokerRequest) {
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();

    // SELECT
    List<ExpressionContext> selectExpressions;
    Map<ExpressionContext, String> aliasMap = new HashMap<>();
    List<Expression> selectList = pinotQuery.getSelectList();
    selectExpressions = new ArrayList<>(selectList.size());
    for (Expression thriftExpression : selectList) {
      ExpressionContext expression;
      if (thriftExpression.getType() == ExpressionType.FUNCTION && thriftExpression.getFunctionCall().getOperator()
          .equalsIgnoreCase("AS")) {
        // Handle alias
        List<Expression> operands = thriftExpression.getFunctionCall().getOperands();
        expression = QueryContextConverterUtils.getExpression(operands.get(0));
        aliasMap.put(expression, operands.get(1).getIdentifier().getName());
      } else {
        expression = QueryContextConverterUtils.getExpression(thriftExpression);
      }
      selectExpressions.add(expression);
    }

    // WHERE
    FilterContext filter = null;
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      filter = QueryContextConverterUtils.getFilter(pinotQuery.getFilterExpression());
    }

    // GROUP BY
    List<ExpressionContext> groupByExpressions = null;
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (CollectionUtils.isNotEmpty(groupByList)) {
      groupByExpressions = new ArrayList<>(groupByList.size());
      for (Expression thriftExpression : groupByList) {
        groupByExpressions.add(QueryContextConverterUtils.getExpression(thriftExpression));
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
        ExpressionContext expression = QueryContextConverterUtils.getExpression(thriftFunction.getOperands().get(0));
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
      havingFilter = QueryContextConverterUtils.getFilter(havingExpression);
    }

    return new QueryContext.Builder().setTableName(pinotQuery.getDataSource().getTableName())
        .setSelectExpressions(selectExpressions).setAliasMap(aliasMap).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions)
        .setHavingFilter(havingFilter).setLimit(pinotQuery.getLimit()).setOffset(pinotQuery.getOffset())
        .setQueryOptions(pinotQuery.getQueryOptions()).setDebugOptions(pinotQuery.getDebugOptions())
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
        selectExpressions.add(QueryContextConverterUtils.getExpression(expression));
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
            arguments.add(QueryContextConverterUtils.getExpression(expression));
          }
        } else {
          // For non-DISTINCT query, only the first argument is expression, others are literals
          // NOTE: We directly use the string as the literal value because of the legacy behavior of PQL compiler
          //       treating string literal as identifier in the aggregation function.
          arguments.add(QueryContextConverterUtils.getExpression(stringExpressions.get(0)));
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
          groupByExpressions.add(QueryContextConverterUtils.getExpression(stringExpression));
        }

        // NOTE: Use TOP in GROUP-BY clause as LIMIT for backward-compatibility.
        limit = (int) groupBy.getTopN();
      }
    }

    FilterContext filter = null;
    FilterQueryTree rootFilterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (rootFilterQueryTree != null) {
      filter = QueryContextConverterUtils.getFilter(rootFilterQueryTree);
    }

    List<OrderByExpressionContext> orderByExpressions = null;
    List<SelectionSort> orderBy = brokerRequest.getOrderBy();
    if (CollectionUtils.isNotEmpty(orderBy)) {
      // Deduplicate the order-by expressions
      orderByExpressions = new ArrayList<>(orderBy.size());
      Set<ExpressionContext> expressionSet = new HashSet<>();
      for (SelectionSort selectionSort : orderBy) {
        ExpressionContext expression = QueryContextConverterUtils.getExpression(selectionSort.getColumn());
        if (expressionSet.add(expression)) {
          orderByExpressions.add(new OrderByExpressionContext(expression, selectionSort.isIsAsc()));
        }
      }
    }

    return new QueryContext.Builder().setTableName(brokerRequest.getQuerySource().getTableName())
        .setSelectExpressions(selectExpressions).setAliasMap(Collections.emptyMap()).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions).setLimit(limit)
        .setOffset(offset).setQueryOptions(brokerRequest.getQueryOptions())
        .setDebugOptions(brokerRequest.getDebugOptions()).setBrokerRequest(brokerRequest).build();
  }
}
