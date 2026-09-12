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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.NullSkippingUtils;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


/// Interface for within segment aggregation strategy.
///
/// The implementation should be stateless, and can be shared among multiple segments in multiple threads. The
/// result for each segment should be stored and passed in via the result holder.
/// There should be no assumptions beyond segment boundaries, different aggregation strategies may be utilized
/// across different segments for a given query.
///
/// Supports both single-key and multi-key CORRELATE_BY. The single-key path is kept as a zero-overhead fast path
/// (structurally identical to the original single-column implementation) to avoid any regression for existing queries.
///
/// @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
@ThreadSafe
public abstract class AggregationStrategy<A> {

  protected final int _numSteps;
  protected final int _numCorrelateByKeys;
  private final List<ExpressionContext> _stepExpressions;
  private final List<ExpressionContext> _correlateByExpressions;
  private final ExpressionContext _primaryCorrelationCol;

  protected final boolean _nullHandlingEnabled;

  public AggregationStrategy(List<ExpressionContext> stepExpressions, List<ExpressionContext> correlateByExpressions,
      boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
    _stepExpressions = stepExpressions;
    _correlateByExpressions = correlateByExpressions;
    _primaryCorrelationCol = _correlateByExpressions.get(0);
    _numSteps = _stepExpressions.size();
    _numCorrelateByKeys = _correlateByExpressions.size();
  }

  /// Creates an aggregation result for single-key correlation.
  abstract A createAggregationResult(Dictionary dictionary);

  /// Creates an aggregation result for multi-key correlation.
  abstract A createAggregationResultMultiKey(Dictionary[] dictionaries);

  public A getAggregationResult(Dictionary dictionary, AggregationResultHolder aggregationResultHolder) {
    A aggResult = aggregationResultHolder.getResult();
    if (aggResult == null) {
      aggResult = createAggregationResult(dictionary);
      aggregationResultHolder.setValue(aggResult);
    }
    return aggResult;
  }

  public A getAggregationResultMultiKey(Dictionary[] dictionaries,
      AggregationResultHolder aggregationResultHolder) {
    A aggResult = aggregationResultHolder.getResult();
    if (aggResult == null) {
      aggResult = createAggregationResultMultiKey(dictionaries);
      aggregationResultHolder.setValue(aggResult);
    }
    return aggResult;
  }

  public A getAggregationResultGroupBy(Dictionary dictionary, GroupByResultHolder groupByResultHolder, int groupKey) {
    A aggResult = groupByResultHolder.getResult(groupKey);
    if (aggResult == null) {
      aggResult = createAggregationResult(dictionary);
      groupByResultHolder.setValueForKey(groupKey, aggResult);
    }
    return aggResult;
  }

  public A getAggregationResultGroupByMultiKey(Dictionary[] dictionaries, GroupByResultHolder groupByResultHolder,
      int groupKey) {
    A aggResult = groupByResultHolder.getResult(groupKey);
    if (aggResult == null) {
      aggResult = createAggregationResultMultiKey(dictionaries);
      groupByResultHolder.setValueForKey(groupKey, aggResult);
    }
    return aggResult;
  }

  /// Performs aggregation on the given block value sets (aggregation only).
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = getSteps(blockValSetMap);
    if (_numCorrelateByKeys == 1) {
      aggregateSingleKey(length, aggregationResultHolder, blockValSetMap, steps);
    } else {
      aggregateMultiKey(length, aggregationResultHolder, blockValSetMap, steps);
    }
  }

  private void aggregateSingleKey(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap, int[][] steps) {
    final Dictionary dictionary = getPrimaryDictionary(blockValSetMap);
    final int[] correlationIds = getPrimaryCorrelationIds(blockValSetMap);
    final A aggResult = getAggregationResult(dictionary, aggregationResultHolder);
    forEachNotNullCorrelation(length, blockValSetMap, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int n = 0; n < _numSteps; n++) {
          if (steps[n][i] > 0) {
            add(dictionary, aggResult, n, correlationIds[i]);
          }
        }
      }
    });
  }

  private void aggregateMultiKey(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap, int[][] steps) {
    final Dictionary[] dictionaries = getAllDictionaries(blockValSetMap);
    final int[][] allCorrelationIds = getAllCorrelationDictIds(blockValSetMap);
    final A aggResult = getAggregationResultMultiKey(dictionaries, aggregationResultHolder);
    final int[] rowDictIds = new int[_numCorrelateByKeys];
    forEachNotNullCorrelation(length, blockValSetMap, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int k = 0; k < _numCorrelateByKeys; k++) {
          rowDictIds[k] = allCorrelationIds[k][i];
        }
        for (int n = 0; n < _numSteps; n++) {
          if (steps[n][i] > 0) {
            addMultiKey(aggResult, n, dictionaries, rowDictIds);
          }
        }
      }
    });
  }

  /// Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
  /// columns).
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = getSteps(blockValSetMap);
    if (_numCorrelateByKeys == 1) {
      aggregateGroupBySVSingleKey(length, groupKeyArray, groupByResultHolder, blockValSetMap, steps);
    } else {
      aggregateGroupBySVMultiKey(length, groupKeyArray, groupByResultHolder, blockValSetMap, steps);
    }
  }

  private void aggregateGroupBySVSingleKey(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap, int[][] steps) {
    final Dictionary dictionary = getPrimaryDictionary(blockValSetMap);
    final int[] correlationIds = getPrimaryCorrelationIds(blockValSetMap);
    forEachNotNullCorrelation(length, blockValSetMap, (from, to) -> {
      for (int i = from; i < to; i++) {
        final int groupKey = groupKeyArray[i];
        final A aggResult = getAggregationResultGroupBy(dictionary, groupByResultHolder, groupKey);
        for (int n = 0; n < _numSteps; n++) {
          if (steps[n][i] > 0) {
            add(dictionary, aggResult, n, correlationIds[i]);
          }
        }
      }
    });
  }

  private void aggregateGroupBySVMultiKey(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap, int[][] steps) {
    final Dictionary[] dictionaries = getAllDictionaries(blockValSetMap);
    final int[][] allCorrelationIds = getAllCorrelationDictIds(blockValSetMap);
    final int[] rowDictIds = new int[_numCorrelateByKeys];
    forEachNotNullCorrelation(length, blockValSetMap, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int k = 0; k < _numCorrelateByKeys; k++) {
          rowDictIds[k] = allCorrelationIds[k][i];
        }
        final int groupKey = groupKeyArray[i];
        final A aggResult = getAggregationResultGroupByMultiKey(dictionaries, groupByResultHolder, groupKey);
        for (int n = 0; n < _numSteps; n++) {
          if (steps[n][i] > 0) {
            addMultiKey(aggResult, n, dictionaries, rowDictIds);
          }
        }
      }
    });
  }

  /// Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
  /// columns).
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = getSteps(blockValSetMap);
    if (_numCorrelateByKeys == 1) {
      aggregateGroupByMVSingleKey(length, groupKeysArray, groupByResultHolder, blockValSetMap, steps);
    } else {
      aggregateGroupByMVMultiKey(length, groupKeysArray, groupByResultHolder, blockValSetMap, steps);
    }
  }

  private void aggregateGroupByMVSingleKey(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, Map<ExpressionContext, BlockValSet> blockValSetMap, int[][] steps) {
    final Dictionary dictionary = getPrimaryDictionary(blockValSetMap);
    final int[] correlationIds = getPrimaryCorrelationIds(blockValSetMap);
    forEachNotNullCorrelation(length, blockValSetMap, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int groupKey : groupKeysArray[i]) {
          final A aggResult = getAggregationResultGroupBy(dictionary, groupByResultHolder, groupKey);
          for (int n = 0; n < _numSteps; n++) {
            if (steps[n][i] > 0) {
              add(dictionary, aggResult, n, correlationIds[i]);
            }
          }
        }
      }
    });
  }

  private void aggregateGroupByMVMultiKey(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, Map<ExpressionContext, BlockValSet> blockValSetMap, int[][] steps) {
    final Dictionary[] dictionaries = getAllDictionaries(blockValSetMap);
    final int[][] allCorrelationIds = getAllCorrelationDictIds(blockValSetMap);
    final int[] rowDictIds = new int[_numCorrelateByKeys];
    forEachNotNullCorrelation(length, blockValSetMap, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int k = 0; k < _numCorrelateByKeys; k++) {
          rowDictIds[k] = allCorrelationIds[k][i];
        }
        for (int groupKey : groupKeysArray[i]) {
          final A aggResult = getAggregationResultGroupByMultiKey(dictionaries, groupByResultHolder, groupKey);
          for (int n = 0; n < _numSteps; n++) {
            if (steps[n][i] > 0) {
              addMultiKey(aggResult, n, dictionaries, rowDictIds);
            }
          }
        }
      }
    });
  }

  /// Adds a correlation id to the aggregation counter for a given step in the funnel.
  abstract void add(Dictionary dictionary, A aggResult, int step, int correlationId);

  /// Adds a row's composite correlation identity to the aggregation counter for a given step (multi-key path).
  ///
  /// @param aggResult          the aggregation result to update
  /// @param step               the funnel step index
  /// @param dictionaries       one dictionary per correlate-by column
  /// @param correlationDictIds one dictionary ID per correlate-by column for the current row
  ///                           (this array is reused across rows; implementations must not hold a reference)
  abstract void addMultiKey(A aggResult, int step, Dictionary[] dictionaries, int[] correlationDictIds);

  Dictionary getPrimaryDictionary(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final BlockValSet primaryCorrelationValSet = blockValSetMap.get(_primaryCorrelationCol);
    Preconditions.checkArgument(primaryCorrelationValSet.isDictionaryEncoded(),
        "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported, please use a dictionary encoded "
            + "column.");
    return primaryCorrelationValSet.getDictionary();
  }

  private Dictionary[] getAllDictionaries(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    Dictionary[] dictionaries = new Dictionary[_numCorrelateByKeys];
    for (int k = 0; k < _numCorrelateByKeys; k++) {
      BlockValSet valSet = blockValSetMap.get(_correlateByExpressions.get(k));
      Preconditions.checkArgument(valSet.isDictionaryEncoded(),
          "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported, please use a dictionary encoded "
              + "column.");
      dictionaries[k] = valSet.getDictionary();
    }
    return dictionaries;
  }

  private int[] getPrimaryCorrelationIds(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return blockValSetMap.get(_primaryCorrelationCol).getDictionaryIdsSV();
  }

  private int[][] getAllCorrelationDictIds(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int[][] allIds = new int[_numCorrelateByKeys][];
    for (int k = 0; k < _numCorrelateByKeys; k++) {
      allIds[k] = blockValSetMap.get(_correlateByExpressions.get(k)).getDictionaryIdsSV();
    }
    return allIds;
  }

  /// Runs the consumer over each range of rows whose correlation key is entirely non-null, or over the whole block
  /// when the option is disabled.
  ///
  /// Only the correlation key is consulted. A step expression is a predicate, and a predicate over a null operand is
  /// UNKNOWN, which SQL treats as not satisfied wherever a boolean is consumed, so a null step already means that
  /// step did not match. The correlation key is what the funnel counts distinct values of, and a row whose key is
  /// null belongs to no key at all; its dictionary id would otherwise be the default's, counted as a real one. With
  /// a composite key a null in any component leaves the whole key undefined, so the row is skipped.
  private void forEachNotNullCorrelation(int length, Map<ExpressionContext, BlockValSet> blockValSetMap,
      RoaringBitmapUtils.BatchConsumer consumer) {
    NullSkippingUtils.forEachNotNull(_nullHandlingEnabled, length, correlationNullBitmap(blockValSetMap), consumer);
  }

  /// Returns the union of the correlation columns' null bitmaps, or `null` when no row is null.
  @Nullable
  private RoaringBitmap correlationNullBitmap(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (!_nullHandlingEnabled) {
      return null;
    }
    RoaringBitmap merged = null;
    for (ExpressionContext correlateByExpression : _correlateByExpressions) {
      RoaringBitmap nullBitmap = blockValSetMap.get(correlateByExpression).getNullBitmap();
      if (nullBitmap == null) {
        continue;
      }
      // Copied before merging: the bitmap belongs to the block and must not be mutated
      if (merged == null) {
        merged = nullBitmap.clone();
      } else {
        merged.or(nullBitmap);
      }
    }
    return merged;
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
