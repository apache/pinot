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

import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;


public class QueryContextUtils {
  private QueryContextUtils() {
  }

  /**
   * Returns {@code true} if the given query is a selection query, {@code false} otherwise.
   */
  public static boolean isSelectionQuery(QueryContext query) {
    return !query.isDistinct() && query.getAggregationFunctions() == null;
  }

  /**
   * Returns {@code true} if the given query is a simple selection-only query, {@code false} otherwise.
   *
   * Selection-only query at this moment means selection query without order-by.
   */
  public static boolean isSelectionOnlyQuery(QueryContext query) {
    return isSelectionQuery(query) && query.getOrderByExpressions() == null;
  }

  /**
   * Returns {@code true} if the given query is an aggregation query, {@code false} otherwise.
   */
  public static boolean isAggregationQuery(QueryContext query) {
    return query.getAggregationFunctions() != null;
  }

  /**
   * Returns {@code true} if the given query is a distinct query, {@code false} otherwise.
   */
  public static boolean isDistinctQuery(QueryContext query) {
    return query.isDistinct();
  }

  /** Collect aggregation functions (except for the ones in filter). */
  public static void collectPostAggregations(QueryContext queryContext, Set<String> postAggregations) {

    // select
    for (ExpressionContext selectExpression : queryContext.getSelectExpressions()) {
      collectPostAggregations(selectExpression, postAggregations);
    }

    // having
    if (queryContext.getHavingFilter() != null) {
      collectPostAggregations(queryContext.getHavingFilter(), postAggregations);
    }

    // order-by
    if (queryContext.getOrderByExpressions() != null) {
      for (OrderByExpressionContext orderByExpression : queryContext.getOrderByExpressions()) {
        collectPostAggregations(orderByExpression.getExpression(), postAggregations);
      }
    }

    // group-by
    if (queryContext.getGroupByExpressions() != null) {
      for (ExpressionContext groupByExpression : queryContext.getGroupByExpressions()) {
        collectPostAggregations(groupByExpression, postAggregations);
      }
    }
  }

  /** Collect aggregation functions from an ExpressionContext. */
  public static void collectPostAggregations(ExpressionContext expression, Set<String> postAggregations) {
    FunctionContext function = expression.getFunction();
    if (function != null) {
      if (function.getType() == FunctionContext.Type.TRANSFORM) {
        // transform
        if (isPostAggregation(function)) {
          postAggregations.add(function.toString());
        }
      } else {
        // aggregation
        for (ExpressionContext argument : function.getArguments()) {
          collectPostAggregations(argument, postAggregations);
        }
      }
    }
  }

  public static boolean isPostAggregation(FunctionContext function) {
    if (function != null) {
      if (function.getType() == FunctionContext.Type.AGGREGATION) {
        return true;
      }

      // transform function
      for (ExpressionContext argument : function.getArguments()) {
        if (isPostAggregation(argument.getFunction())) {
          return true;
        }
      }
    }
    return false;
  }

  /** Collect aggregation functions from a FilterContext. */
  public static void collectPostAggregations(FilterContext filter, Set<String> postAggregations) {
    List<FilterContext> children = filter.getChildren();
    if (children != null) {
      for (FilterContext child : children) {
        collectPostAggregations(child, postAggregations);
      }
    } else {
      collectPostAggregations(filter.getPredicate().getLhs(), postAggregations);
    }
  }
}
