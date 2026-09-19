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
package org.apache.pinot.core.startree.executor;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;


/// The `StarTreeGroupByExecutor` class is the group-by executor for star-tree index.
///
/// - The column in function context is function-column pair
/// - No transform function in aggregation
/// - For `COUNT` aggregation function, we need to aggregate on the pre-aggregated column
@SuppressWarnings({"rawtypes", "unchecked"})
public class StarTreeGroupByExecutor extends DefaultGroupByExecutor {
  private final AggregationFunctionColumnPair[] _aggregationFunctionColumnPairs;

  public StarTreeGroupByExecutor(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      BaseProjectOperator<?> projectOperator, AggregationFunctionColumnPair[] aggregationFunctionColumnPairs) {
    this(queryContext, queryContext.getAggregationFunctions(), groupByExpressions, projectOperator,
        aggregationFunctionColumnPairs, null);
  }

  /// Creates an executor over the pre-aggregated columns the query was routed to.
  ///
  /// `aggregationFunctionColumnPairs` must be the pairs the star-tree project operator was built with, because they
  /// depend on which star-tree was picked: a null-aware star-tree resolves `COUNT(column)` to `count__column` while a
  /// regular one resolves it to `count__*`.
  public StarTreeGroupByExecutor(QueryContext queryContext, AggregationFunction[] aggregationFunctions,
      ExpressionContext[] groupByExpressions, BaseProjectOperator<?> projectOperator,
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs, @Nullable GroupKeyGenerator groupKeyGenerator) {
    super(queryContext, aggregationFunctions, groupByExpressions, projectOperator, groupKeyGenerator);
    _aggregationFunctionColumnPairs = aggregationFunctionColumnPairs;
  }

  @Override
  protected void aggregate(ValueBlock valueBlock, int length, int functionIndex) {
    AggregationFunction aggregationFunction = _aggregationFunctions[functionIndex];
    GroupByResultHolder groupByResultHolder = _groupByResultHolders[functionIndex];
    Map<ExpressionContext, BlockValSet> blockValSetMap =
        AggregationFunctionUtils.getBlockValSetMap(_aggregationFunctionColumnPairs[functionIndex], valueBlock);
    if (_hasMVGroupByExpression) {
      aggregationFunction.aggregateGroupByMV(length, _mvGroupKeys, groupByResultHolder, blockValSetMap);
    } else {
      aggregationFunction.aggregateGroupBySV(length, _svGroupKeys, groupByResultHolder, blockValSetMap);
    }
  }
}
