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
package org.apache.pinot.core.query.aggregation.function.funnel;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * The {@code FunnelCountSortedAggregationFunction} calculates the number of conversions for a given correlation column
 * and a list of steps as boolean expressions.
 * It leverages a more efficient counting strategy for segments sorted by correlate_by column, falls back to a regular
 * counting strategy for unsorted segments (e.g. uncommitted segments).
 *
 * Example:
 *   SELECT
 *    dateTrunc('day', timestamp) AS ts,
 *    FUNNEL_COUNT(
 *      STEPS(url = '/addToCart', url = '/checkout', url = '/orderConfirmation'),
 *      CORRELATE_BY(user_id),
 *      SETTINGS('partitioned','sorted')
 *    ) as step_counts
 *    FROM user_log
 *    WHERE url in ('/addToCart', '/checkout', '/orderConfirmation')
 *    GROUP BY 1
 *
 */
public class FunnelCountSortedAggregationFunction<A> extends FunnelCountAggregationFunction<A, List<Long>> {
  private final ExpressionContext _primaryCorrelationCol;
  private final AggregationStrategy<SortedAggregationResult> _sortedAggregationStrategy;
  private final ResultExtractionStrategy<SortedAggregationResult, List<Long>> _sortedResultExtractionStrategy;

  public FunnelCountSortedAggregationFunction(List<ExpressionContext> expressions,
      List<ExpressionContext> stepExpressions, List<ExpressionContext> correlateByExpressions,
      AggregationStrategy<A> aggregationStrategy, ResultExtractionStrategy<A, List<Long>> resultExtractionStrategy,
      MergeStrategy<List<Long>> mergeStrategy) {
    super(expressions, stepExpressions, correlateByExpressions, aggregationStrategy, resultExtractionStrategy,
        mergeStrategy);
    _sortedAggregationStrategy = new SortedAggregationStrategy(stepExpressions, correlateByExpressions);
    _sortedResultExtractionStrategy = SortedAggregationResult::extractResult;;
    _primaryCorrelationCol = correlateByExpressions.get(0);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (isSortedDictionary(blockValSetMap)) {
      _sortedAggregationStrategy.aggregate(length, aggregationResultHolder, blockValSetMap);
    } else {
      super.aggregate(length, aggregationResultHolder, blockValSetMap);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (isSortedDictionary(blockValSetMap)) {
      _sortedAggregationStrategy.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
    } else {
      super.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (isSortedDictionary(blockValSetMap)) {
      _sortedAggregationStrategy.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
    } else {
      super.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
    }
  }

  @Override
  public List<Long> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (isSortedAggResult(aggregationResultHolder.getResult())) {
      return _sortedResultExtractionStrategy.extractAggregationResult(aggregationResultHolder);
    } else {
      return super.extractAggregationResult(aggregationResultHolder);
    }
  }

  @Override
  public List<Long> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    if (isSortedAggResult(groupByResultHolder.getResult(groupKey))) {
      return _sortedResultExtractionStrategy.extractGroupByResult(groupByResultHolder, groupKey);
    } else {
      return super.extractGroupByResult(groupByResultHolder, groupKey);
    }
  }

  private boolean isSortedDictionary(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return getDictionary(blockValSetMap).isSorted();
  }

  private boolean isSortedAggResult(Object aggResult) {
    return aggResult instanceof SortedAggregationResult;
  }

  private Dictionary getDictionary(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary primaryCorrelationDictionary = blockValSetMap.get(_primaryCorrelationCol).getDictionary();
    Preconditions.checkArgument(primaryCorrelationDictionary != null,
        "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported for sorted setting, "
            + "please use a dictionary encoded column.");
    return primaryCorrelationDictionary;
  }
}
