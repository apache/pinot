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
package org.apache.pinot.core.startree;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.core.startree.v2.StarTreeV2Metadata;


public class StarTreeUtils {
  private StarTreeUtils() {
  }

  public static final String USE_STAR_TREE_KEY = "useStarTree";

  /**
   * Returns whether star-tree is disabled for the query.
   */
  public static boolean isStarTreeDisabled(QueryContext queryContext) {
    Map<String, String> debugOptions = queryContext.getDebugOptions();
    return debugOptions != null && "false".equalsIgnoreCase(debugOptions.get(USE_STAR_TREE_KEY));
  }

  /**
   * Returns whether the query is fit for star tree index.
   * <p>The query is fit for star tree index if the following conditions are met:
   * <ul>
   *   <li>Star-tree contains all aggregation function column pairs</li>
   *   <li>All predicate columns and group-by columns are star-tree dimensions</li>
   *   <li>All predicates are conjoined by AND</li>
   * </ul>
   */
  public static boolean isFitForStarTree(StarTreeV2Metadata starTreeV2Metadata,
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs,
      @Nullable TransformExpressionTree[] groupByExpressions, @Nullable FilterQueryTree rootFilterNode) {
    // Check aggregations
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      if (!starTreeV2Metadata.containsFunctionColumnPair(aggregationFunctionColumnPair)) {
        return false;
      }
    }

    // Check group-by expressions
    Set<String> starTreeDimensions = new HashSet<>(starTreeV2Metadata.getDimensionsSplitOrder());
    if (groupByExpressions != null) {
      Set<String> groupByColumns = new HashSet<>();
      for (TransformExpressionTree groupByExpression : groupByExpressions) {
        groupByExpression.getColumns(groupByColumns);
      }
      if (!starTreeDimensions.containsAll(groupByColumns)) {
        return false;
      }
    }

    // Check filters
    return rootFilterNode == null || checkFilters(rootFilterNode, starTreeDimensions);
  }

  /**
   * Helper method to check whether all columns in predicates are star-tree dimensions, and all predicates are
   * conjoined by AND.
   */
  private static boolean checkFilters(FilterQueryTree filterNode, Set<String> starTreeDimensions) {
    FilterOperator operator = filterNode.getOperator();
    if (operator == FilterOperator.OR) {
      return false;
    }
    if (operator == FilterOperator.AND) {
      for (FilterQueryTree child : filterNode.getChildren()) {
        if (!checkFilters(child, starTreeDimensions)) {
          return false;
        }
      }
      return true;
    }
    String column = filterNode.getColumn();
    return starTreeDimensions.contains(column);
  }
}
