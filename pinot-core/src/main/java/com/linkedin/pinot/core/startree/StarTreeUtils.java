/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import com.linkedin.pinot.core.startree.v2.StarTreeV2Metadata;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class StarTreeUtils {
  private StarTreeUtils() {
  }

  public static final String USE_STAR_TREE_KEY = "useStarTree";

  /**
   * Returns whether star-tree is disabled in broker request.
   */
  public static boolean isStarTreeDisabled(@Nonnull BrokerRequest brokerRequest) {
    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
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
  public static boolean isFitForStarTree(@Nonnull StarTreeV2Metadata starTreeV2Metadata,
      @Nonnull Set<AggregationFunctionColumnPair> aggregationFunctionColumnPairs,
      @Nullable Set<TransformExpressionTree> groupByExpressions, @Nullable FilterQueryTree rootFilterNode) {
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
  private static boolean checkFilters(@Nonnull FilterQueryTree filterNode, @Nonnull Set<String> starTreeDimensions) {
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

  /**
   * Creates a {@link AggregationFunctionContext} from the given context but replace the column with the function-column
   * pair.
   */
  public static AggregationFunctionContext createStarTreeFunctionContext(
      @Nonnull AggregationFunctionContext functionContext) {
    AggregationFunction function = functionContext.getAggregationFunction();
    AggregationFunctionColumnPair functionColumnPair =
        new AggregationFunctionColumnPair(function.getType(), functionContext.getColumn());
    return new AggregationFunctionContext(function, functionColumnPair.toColumnName());
  }

  /**
   * Creates an array of {@link AggregationFunctionContext}s from the given contexts but replace the column with the
   * function-column pair.
   */
  public static AggregationFunctionContext[] createStarTreeFunctionContexts(
      @Nonnull AggregationFunctionContext[] functionContexts) {
    int numContexts = functionContexts.length;
    AggregationFunctionContext[] starTreeFunctionContexts = new AggregationFunctionContext[numContexts];
    for (int i = 0; i < numContexts; i++) {
      starTreeFunctionContexts[i] = createStarTreeFunctionContext(functionContexts[i]);
    }
    return starTreeFunctionContexts;
  }
}
