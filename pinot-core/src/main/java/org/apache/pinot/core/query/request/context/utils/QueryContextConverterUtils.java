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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.request.context.ExplainMode;
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
    boolean distinct = false;
    List<Expression> selectList = pinotQuery.getSelectList();
    // Handle DISTINCT
    if (selectList.size() == 1) {
      Function function = selectList.get(0).getFunctionCall();
      if (function != null && function.getOperator().equals("distinct")) {
        distinct = true;
        selectList = function.getOperands();
      }
    }
    List<String> aliasList = new ArrayList<>(selectList.size());
    selectExpressions = new ArrayList<>(selectList.size());
    for (Expression thriftExpression : selectList) {
      // Handle alias
      Function function = thriftExpression.getFunctionCall();
      Expression expressionWithoutAlias;
      if (function != null && function.getOperator().equals("as")) {
        List<Expression> operands = function.getOperands();
        expressionWithoutAlias = operands.get(0);
        aliasList.add(operands.get(1).getIdentifier().getName());
      } else {
        expressionWithoutAlias = thriftExpression;
        // Add null as a placeholder for alias
        aliasList.add(null);
      }
      selectExpressions.add(RequestContextUtils.getExpression(expressionWithoutAlias));
    }

    // WHERE
    FilterContext filter = null;
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      filter = RequestContextUtils.getFilter(filterExpression);
      // Remove the filter if it is always true
      if (filter.isConstantTrue()) {
        filter = null;
      }
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

    /// GROUP BY GROUPING SETS / ROLLUP / CUBE: the wire carries, per grouping set (in ordinal order), the
    /// list of participating column indexes into groupByExpressions (the union of all grouping columns). An
    /// empty inner list is the grand-total set (). Null when this is a plain GROUP BY query.
    List<int[]> groupingSets = null;
    List<List<Integer>> thriftGroupingSets = pinotQuery.getGroupingSets();
    if (thriftGroupingSets != null) {
      // Guard against malformed requests: an empty set list would silently produce empty results, and an
      // out-of-range column index would cause out-of-bounds access deep in the group key generator.
      if (thriftGroupingSets.isEmpty()) {
        throw new IllegalStateException("Grouping sets must not be empty");
      }
      int numGroupByExpressions = groupByExpressions != null ? groupByExpressions.size() : 0;
      groupingSets = new ArrayList<>(thriftGroupingSets.size());
      for (List<Integer> set : thriftGroupingSets) {
        int[] indexes = new int[set.size()];
        int idx = 0;
        for (int columnIndex : set) {
          if (columnIndex < 0 || columnIndex >= numGroupByExpressions) {
            throw new IllegalStateException(
                "Invalid grouping set column index: " + columnIndex + " for " + numGroupByExpressions
                    + " group-by expressions");
          }
          indexes[idx++] = columnIndex;
        }
        groupingSets.add(indexes);
      }
    }

    // ORDER BY
    List<OrderByExpressionContext> orderByExpressions = null;
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (CollectionUtils.isNotEmpty(orderByList)) {
      orderByExpressions = new ArrayList<>(orderByList.size());
      Set<Expression> seen = new HashSet<>();
      for (Expression orderBy : orderByList) {
        Boolean isNullsLast = CalciteSqlParser.isNullsLast(orderBy);
        boolean isAsc = CalciteSqlParser.isAsc(orderBy, isNullsLast);
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
      // Remove the filter if it is always true
      if (havingFilter.isConstantTrue()) {
        havingFilter = null;
      }
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

    ExplainMode explainMode;
    if (!pinotQuery.isExplain()) {
      explainMode = ExplainMode.NONE;
    } else if (isMultiStage(pinotQuery)) {
      explainMode = ExplainMode.NODE;
    } else {
      explainMode = ExplainMode.DESCRIPTION;
    }

    return new QueryContext.Builder()
        .setTableName(tableName)
        .setSubquery(subquery)
        .setSelectExpressions(selectExpressions)
        .setDistinct(distinct)
        .setAliasList(aliasList)
        .setFilter(filter)
        .setGroupByExpressions(groupByExpressions)
        .setGroupingSets(groupingSets)
        .setOrderByExpressions(orderByExpressions)
        .setHavingFilter(havingFilter)
        .setLimit(pinotQuery.getLimit())
        .setOffset(pinotQuery.getOffset())
        .setQueryOptions(pinotQuery.getQueryOptions())
        .setExpressionOverrideHints(expressionContextOverrideHints)
        .setExplain(explainMode)
        .build();
  }

  private static boolean isMultiStage(PinotQuery pinotQuery) {
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    return queryOptions != null && QueryOptionsUtils.isUseMultistageEngine(queryOptions);
  }
}
