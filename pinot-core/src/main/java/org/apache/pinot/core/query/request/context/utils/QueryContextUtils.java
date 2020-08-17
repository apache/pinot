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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;


public class QueryContextUtils {
  private QueryContextUtils() {
  }

  /**
   * Returns all the columns (IDENTIFIER expressions) in the given query.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Set<String> getAllColumns(QueryContext query) {
    Set<String> columns = new HashSet<>();

    for (ExpressionContext expression : query.getSelectExpressions()) {
      expression.getColumns(columns);
    }
    FilterContext filter = query.getFilter();
    if (filter != null) {
      filter.getColumns(columns);
    }
    List<ExpressionContext> groupByExpressions = query.getGroupByExpressions();
    if (groupByExpressions != null) {
      for (ExpressionContext expression : groupByExpressions) {
        expression.getColumns(columns);
      }
    }
    FilterContext havingFilter = query.getHavingFilter();
    if (havingFilter != null) {
      havingFilter.getColumns(columns);
    }
    List<OrderByExpressionContext> orderByExpressions = query.getOrderByExpressions();
    if (orderByExpressions != null) {
      for (OrderByExpressionContext orderByExpression : orderByExpressions) {
        orderByExpression.getColumns(columns);
      }
    }

    // NOTE: Also gather columns from the input expressions of the aggregation functions because for certain types of
    //       aggregation (e.g. DistinctCountThetaSketch), some input expressions are compiled while constructing the
    //       aggregation function.
    AggregationFunction[] aggregationFunctions = query.getAggregationFunctions();
    if (aggregationFunctions != null) {
      for (AggregationFunction aggregationFunction : aggregationFunctions) {
        List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
        for (ExpressionContext expression : inputExpressions) {
          expression.getColumns(columns);
        }
      }
    }

    return columns;
  }

  /**
   * Returns {@code true} if the given query is an aggregation query, {@code false} otherwise.
   */
  public static boolean isAggregationQuery(QueryContext query) {
    return query.getAggregationFunctions() != null;
  }
}
