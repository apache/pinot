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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The {@code FunnelCountAggregationFunction} calculates the number of conversions for a given correlation column and
 * a list of steps as boolean expressions.
 *
 * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
 * @param <I> Intermediate result at segment level (extracted from aforementioned aggregation result).
 *
 * Example:
 *   SELECT
 *    dateTrunc('day', timestamp) AS ts,
 *    FUNNEL_COUNT(
 *      STEPS(url = '/addToCart', url = '/checkout', url = '/orderConfirmation'),
 *      CORRELATE_BY(user_id)
 *    ) as step_counts
 *    FROM user_log
 *    WHERE url in ('/addToCart', '/checkout', '/orderConfirmation')
 *    GROUP BY 1
 *
 *  Counting strategies can be controlled via optional SETTINGS options, for example:
 *
 *  FUNNEL_COUNT(
 *    STEPS(url = '/addToCart', url = '/checkout', url = '/orderConfirmation'),
 *    CORRELATE_BY(user_id),
 *    SETTINGS('theta_sketch','nominalEntries=4096')
 *  )
 *
 * Please refer to {@link FunnelCountAggregationFunctionFactory} to learn about counting strategies available.
 *
 * @see FunnelCountAggregationFunctionFactory
 * @see FunnelCountSortedAggregationFunction
 */
public class FunnelCountAggregationFunction<A, I> implements AggregationFunction<I, LongArrayList> {
  private final List<ExpressionContext> _expressions;
  private final List<ExpressionContext> _stepExpressions;
  private final List<ExpressionContext> _correlateByExpressions;
  private final int _numSteps;

  private final AggregationStrategy<A> _aggregationStrategy;
  private final ResultExtractionStrategy<A, I> _resultExtractionStrategy;
  private final MergeStrategy<I> _mergeStrategy;

  public FunnelCountAggregationFunction(List<ExpressionContext> expressions, List<ExpressionContext> stepExpressions,
      List<ExpressionContext> correlateByExpressions, AggregationStrategy<A> aggregationStrategy,
      ResultExtractionStrategy<A, I> resultExtractionStrategy, MergeStrategy<I> mergeStrategy) {
    _expressions = expressions;
    _stepExpressions = stepExpressions;
    _correlateByExpressions = correlateByExpressions;
    _aggregationStrategy = aggregationStrategy;
    _resultExtractionStrategy = resultExtractionStrategy;
    _mergeStrategy = mergeStrategy;
    _numSteps = _stepExpressions.size();
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expressions.stream().map(ExpressionContext::toString)
        .collect(Collectors.joining(",")) + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    final List<ExpressionContext> inputs = new ArrayList<>();
    inputs.addAll(_correlateByExpressions);
    inputs.addAll(_stepExpressions);
    return inputs;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELCOUNT;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _aggregationStrategy.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _aggregationStrategy.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _aggregationStrategy.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _resultExtractionStrategy.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _resultExtractionStrategy.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public I merge(I a, I b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return _mergeStrategy.merge(a, b);
  }

  @Override
  public LongArrayList extractFinalResult(I intermediateResult) {
    if (intermediateResult == null) {
      return new LongArrayList(_numSteps);
    }
    return _mergeStrategy.extractFinalResult(intermediateResult);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG_ARRAY;
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }
}
