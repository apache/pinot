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
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.function.AggregationFunctionType;
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


public class BrokerRequestToQueryContextConverter {
  private BrokerRequestToQueryContextConverter() {
  }

  /**
   * Converts the given {@link BrokerRequest} into a {@link QueryContext}.
   * <p>Use {@link PinotQuery} if available to avoid the unnecessary parsing of the expressions.
   * <p>TODO: We cannot use PinotQuery to generate the WHERE clause filter because {@code BrokerRequestOptimizer} only
   *          optimizes the BrokerRequest but not the PinotQuery.
   */
  public static QueryContext convert(BrokerRequest brokerRequest) {
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();

    List<ExpressionContext> selectExpressions;
    Map<ExpressionContext, String> aliasMap;
    List<ExpressionContext> groupByExpressions = null;
    int limit;
    int offset = 0;
    if (pinotQuery != null) {
      aliasMap = new HashMap<>();
      List<Expression> selectList = pinotQuery.getSelectList();
      int numExpressions = selectList.size();
      List<ExpressionContext> aggregationExpressions = new ArrayList<>(numExpressions);
      List<ExpressionContext> nonAggregationExpressions = new ArrayList<>(numExpressions);
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
        if (expression.getType() == ExpressionContext.Type.FUNCTION
            && expression.getFunction().getType() == FunctionContext.Type.AGGREGATION) {
          aggregationExpressions.add(expression);
        } else {
          nonAggregationExpressions.add(expression);
        }
      }
      if (aggregationExpressions.isEmpty()) {
        // NOTE: Pinot ignores the GROUP-BY clause when there is no aggregation expressions in the SELECT clause.
        selectExpressions = nonAggregationExpressions;
      } else {
        // NOTE: Pinot ignores the non-aggregation expressions when there are aggregation expressions in the SELECT
        //       clause. E.g. SELECT a, SUM(b) -> SELECT SUM(b).
        selectExpressions = aggregationExpressions;

        List<Expression> groupByList = pinotQuery.getGroupByList();
        if (CollectionUtils.isNotEmpty(groupByList)) {
          groupByExpressions = new ArrayList<>(groupByList.size());
          for (Expression thriftExpression : groupByList) {
            groupByExpressions.add(QueryContextConverterUtils.getExpression(thriftExpression));
          }
        }
      }
      limit = pinotQuery.getLimit();
      offset = pinotQuery.getOffset();
    } else {
      // NOTE: Alias is not supported for PQL queries.
      aliasMap = Collections.emptyMap();
      Selection selections = brokerRequest.getSelections();
      if (selections != null) {
        // Selection query
        List<String> selectionColumns = selections.getSelectionColumns();
        selectExpressions = new ArrayList<>(selectionColumns.size());
        for (String expression : selectionColumns) {
          selectExpressions.add(QueryContextConverterUtils.getExpression(expression));
        }
        // NOTE: Pinot ignores the GROUP-BY clause for selection queries.
        groupByExpressions = null;
        limit = brokerRequest.getLimit();
        offset = selections.getOffset();
      } else {
        // Aggregation query
        List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
        selectExpressions = new ArrayList<>(aggregationsInfo.size());
        for (AggregationInfo aggregationInfo : aggregationsInfo) {
          String functionName = aggregationInfo.getAggregationType();
          List<String> stringExpressions = aggregationInfo.getExpressions();
          int numArguments = stringExpressions.size();
          List<ExpressionContext> arguments = new ArrayList<>(numArguments);
          if (functionName.equalsIgnoreCase(AggregationFunctionType.DISTINCTCOUNTTHETASKETCH.getName())) {
            // NOTE: For DistinctCountThetaSketch, because of the legacy behavior of PQL compiler treating string
            //       literal as identifier in aggregation, here we treat all expressions except for the first one as
            //       string literal.
            arguments.add(QueryContextConverterUtils.getExpression(stringExpressions.get(0)));
            for (int i = 1; i < numArguments; i++) {
              arguments.add(ExpressionContext.forLiteral(stringExpressions.get(i)));
            }
          } else {
            for (String expression : stringExpressions) {
              arguments.add(QueryContextConverterUtils.getExpression(expression));
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
        } else {
          limit = brokerRequest.getLimit();
        }
      }
    }

    List<OrderByExpressionContext> orderByExpressions = null;
    if (pinotQuery != null) {
      List<Expression> orderByList = pinotQuery.getOrderByList();
      if (CollectionUtils.isNotEmpty(orderByList)) {
        orderByExpressions = new ArrayList<>(orderByList.size());
        for (Expression orderBy : orderByList) {
          // NOTE: Order-by is always a Function with the ordering of the Expression
          Function thriftFunction = orderBy.getFunctionCall();
          boolean isAsc = thriftFunction.getOperator().equalsIgnoreCase("ASC");
          ExpressionContext expression = QueryContextConverterUtils.getExpression(thriftFunction.getOperands().get(0));
          orderByExpressions.add(new OrderByExpressionContext(expression, isAsc));
        }
      }
    } else {
      List<SelectionSort> orderBy = brokerRequest.getOrderBy();
      if (CollectionUtils.isNotEmpty(orderBy)) {
        orderByExpressions = new ArrayList<>(orderBy.size());
        for (SelectionSort selectionSort : orderBy) {
          orderByExpressions.add(
              new OrderByExpressionContext(QueryContextConverterUtils.getExpression(selectionSort.getColumn()),
                  selectionSort.isIsAsc()));
        }
      }
    }

    // NOTE: Always use BrokerRequest to generate filter because BrokerRequestOptimizer only optimizes the BrokerRequest
    //       but not the PinotQuery.
    FilterContext filter = null;
    FilterQueryTree root = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (root != null) {
      filter = QueryContextConverterUtils.getFilter(root);
    }

    // NOTE: Always use PinotQuery to generate HAVING filter because PQL does not support HAVING clause.
    FilterContext havingFilter = null;
    if (pinotQuery != null) {
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        havingFilter = QueryContextConverterUtils.getFilter(havingExpression);
      }
    }

    return new QueryContext.Builder().setSelectExpressions(selectExpressions).setAliasMap(aliasMap).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions)
        .setHavingFilter(havingFilter).setLimit(limit).setOffset(offset)
        .setQueryOptions(brokerRequest.getQueryOptions()).setDebugOptions(brokerRequest.getDebugOptions())
        .setBrokerRequest(brokerRequest).build();
  }
}
