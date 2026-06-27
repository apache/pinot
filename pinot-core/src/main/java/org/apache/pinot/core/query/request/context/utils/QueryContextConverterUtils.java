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

    /// GROUP BY GROUPING SETS / ROLLUP / CUBE: the wire carries one membership bitmask per set over
    /// groupByExpressions (the union of all grouping columns). Decode each mask into the sorted list of
    /// participating column indexes; a mask of 0 yields the empty (grand-total) set (). Null when this is a
    /// plain GROUP BY query.
    List<int[]> groupingSets = null;
    List<Integer> groupingSetMasks = pinotQuery.getGroupingSetMasks();
    if (groupingSetMasks != null) {
      // Guard against malformed requests (the parser always emits at least one set over a union of at most 31
      // columns): an empty set list would silently produce empty results, and a union of more than 31 columns
      // cannot be represented in the synthetic INT grouping-id bitmask (bit 31 is the sign bit).
      if (groupingSetMasks.isEmpty()) {
        throw new IllegalStateException("Grouping set masks must not be empty");
      }
      int numGroupByExpressions = groupByExpressions != null ? groupByExpressions.size() : 0;
      if (numGroupByExpressions > CalciteSqlParser.MAX_GROUPING_SETS_COLUMNS) {
        throw new IllegalStateException(
            "Cannot use grouping sets over " + numGroupByExpressions + " group-by expressions (max: "
                + CalciteSqlParser.MAX_GROUPING_SETS_COLUMNS + ")");
      }
      groupingSets = new ArrayList<>(groupingSetMasks.size());
      for (int mask : groupingSetMasks) {
        // Every bit must reference an existing union column, otherwise the decoded indexes would cause
        // out-of-bounds access deep in the group key generator. The union size is capped at 31 above, so the
        // unsigned shift also rejects negative masks (bit 31 set).
        if ((mask >>> numGroupByExpressions) != 0) {
          throw new IllegalStateException(
              "Invalid grouping set mask: " + mask + " for " + numGroupByExpressions + " group-by expressions");
        }
        int[] indexes = new int[Integer.bitCount(mask)];
        int idx = 0;
        for (int bit = 0; bit < Integer.SIZE && idx < indexes.length; bit++) {
          if ((mask & (1 << bit)) != 0) {
            indexes[idx++] = bit;
          }
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
