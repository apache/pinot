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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.index.reader.Dictionary;


/**
 * Interface for within segment aggregation strategy.
 *
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads. The
 * result for each segment should be stored and passed in via the result holder.
 * There should be no assumptions beyond segment boundaries, different aggregation strategies may be utilized
 * across different segments for a given query.
 *
 * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
 */
@ThreadSafe
public abstract class AggregationStrategy<A> {

  protected final int _numSteps;
  private final List<ExpressionContext> _stepExpressions;
  private final List<ExpressionContext> _correlateByExpressions;

  public AggregationStrategy(List<ExpressionContext> stepExpressions, List<ExpressionContext> correlateByExpressions) {
    _stepExpressions = stepExpressions;
    _correlateByExpressions = correlateByExpressions;
    _numSteps = _stepExpressions.size();
  }

  /**
   * Returns an aggregation result for this aggregation strategy to be kept in a result holder (aggregation only).
   */
  abstract A createAggregationResult(Dictionary[] dictionaries);

  public A getAggregationResultGroupBy(Dictionary[] dictionaries, GroupByResultHolder groupByResultHolder,
      int groupKey) {
    A aggResult = groupByResultHolder.getResult(groupKey);
    if (aggResult == null) {
      aggResult = createAggregationResult(dictionaries);
      groupByResultHolder.setValueForKey(groupKey, aggResult);
    }
    return aggResult;
  }

  public A getAggregationResult(Dictionary[] dictionaries, AggregationResultHolder aggregationResultHolder) {
    A aggResult = aggregationResultHolder.getResult();
    if (aggResult == null) {
      aggResult = createAggregationResult(dictionaries);
      aggregationResultHolder.setValue(aggResult);
    }
    return aggResult;
  }

  /**
   * Performs aggregation on the given block value sets (aggregation only).
   */
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary[] dictionaries = getDictionaries(blockValSetMap);
    final int[][] allCorrelationIds = getAllCorrelationDictIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);
    final int numKeys = _correlateByExpressions.size();

    final A aggResult = getAggregationResult(dictionaries, aggregationResultHolder);
    final int[] rowDictIds = new int[numKeys];
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numKeys; k++) {
        rowDictIds[k] = allCorrelationIds[k][i];
      }
      for (int n = 0; n < _numSteps; n++) {
        if (steps[n][i] > 0) {
          add(aggResult, n, dictionaries, rowDictIds);
        }
      }
    }
  }

  /**
   * Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
   * columns).
   */
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary[] dictionaries = getDictionaries(blockValSetMap);
    final int[][] allCorrelationIds = getAllCorrelationDictIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);
    final int numKeys = _correlateByExpressions.size();

    final int[] rowDictIds = new int[numKeys];
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numKeys; k++) {
        rowDictIds[k] = allCorrelationIds[k][i];
      }
      final int groupKey = groupKeyArray[i];
      final A aggResult = getAggregationResultGroupBy(dictionaries, groupByResultHolder, groupKey);
      for (int n = 0; n < _numSteps; n++) {
        if (steps[n][i] > 0) {
          add(aggResult, n, dictionaries, rowDictIds);
        }
      }
    }
  }

  /**
   * Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
   * columns).
   */
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary[] dictionaries = getDictionaries(blockValSetMap);
    final int[][] allCorrelationIds = getAllCorrelationDictIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);
    final int numKeys = _correlateByExpressions.size();

    final int[] rowDictIds = new int[numKeys];
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numKeys; k++) {
        rowDictIds[k] = allCorrelationIds[k][i];
      }
      for (int groupKey : groupKeysArray[i]) {
        final A aggResult = getAggregationResultGroupBy(dictionaries, groupByResultHolder, groupKey);
        for (int n = 0; n < _numSteps; n++) {
          if (steps[n][i] > 0) {
            add(aggResult, n, dictionaries, rowDictIds);
          }
        }
      }
    }
  }

  /**
   * Adds a row's correlation identity to the aggregation counter for a given step in the funnel.
   *
   * @param aggResult    the aggregation result to update
   * @param step         the funnel step index
   * @param dictionaries one dictionary per correlate-by column
   * @param correlationDictIds one dictionary ID per correlate-by column for the current row
   *                           (this array is reused across rows; implementations must not hold a reference)
   */
  abstract void add(A aggResult, int step, Dictionary[] dictionaries, int[] correlationDictIds);

  protected Dictionary[] getDictionaries(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int numKeys = _correlateByExpressions.size();
    Dictionary[] dictionaries = new Dictionary[numKeys];
    for (int k = 0; k < numKeys; k++) {
      Dictionary d = blockValSetMap.get(_correlateByExpressions.get(k)).getDictionary();
      Preconditions.checkArgument(d != null,
          "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported, please use a dictionary encoded "
              + "column.");
      dictionaries[k] = d;
    }
    return dictionaries;
  }

  private int[][] getAllCorrelationDictIds(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int numKeys = _correlateByExpressions.size();
    int[][] allIds = new int[numKeys][];
    for (int k = 0; k < numKeys; k++) {
      allIds[k] = blockValSetMap.get(_correlateByExpressions.get(k)).getDictionaryIdsSV();
    }
    return allIds;
  }

  private int[][] getSteps(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = new int[_numSteps][];
    for (int n = 0; n < _numSteps; n++) {
      final BlockValSet stepBlockValSet = blockValSetMap.get(_stepExpressions.get(n));
      steps[n] = stepBlockValSet.getIntValuesSV();
    }
    return steps;
  }
}
