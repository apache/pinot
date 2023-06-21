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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code FunnelCountAggregationFunction} calculates the number of step conversions for a given partition column and
 * a list of boolean expressions.
 * <p>IMPORTANT: This function relies on the partition column being partitioned for each segment, where there are no
 * common values across different segments.
 * <p>This function calculates the exact number of step matches per partition key within the segment, then sums up the
 * results from different segments.
 *
 * Example:
 *   SELECT
 *    dateTrunc('day', timestamp) AS ts,
 *    FUNNEL_COUNT(
 *      STEPS(url = '/addToCart', url = '/checkout', url = '/orderConfirmation'),
 *      CORRELATED_BY(user)
 *    ) as step_counts
 *    FROM user_log
 *    WHERE url in ('/addToCart', '/checkout', '/orderConfirmation')
 *    GROUP BY 1
 */
public class FunnelCountAggregationFunction implements AggregationFunction<List<Long>, LongArrayList> {
  final List<ExpressionContext> _expressions;
  final List<ExpressionContext> _stepExpressions;
  final List<ExpressionContext> _correlateByExpressions;
  final ExpressionContext _primaryCorrelationCol;
  final int _numSteps;

  final SegmentAggregationStrategy<?, List<Long>> _sortedAggregationStrategy;
  final SegmentAggregationStrategy<?, List<Long>> _bitmapAggregationStrategy;

  public FunnelCountAggregationFunction(List<ExpressionContext> expressions) {
    _expressions = expressions;
    _correlateByExpressions = Option.CORRELATE_BY.getInputExpressions(expressions);
    _primaryCorrelationCol = Option.CORRELATE_BY.getFirstInputExpression(expressions);
    _stepExpressions = Option.STEPS.getInputExpressions(expressions);
    _numSteps = _stepExpressions.size();
    _sortedAggregationStrategy = new SortedAggregationStrategy();
    _bitmapAggregationStrategy = new BitmapAggregationStrategy();
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
    getAggregationStrategyByBlockValSetMap(blockValSetMap).aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    getAggregationStrategyByBlockValSetMap(blockValSetMap).aggregateGroupBySV(length, groupKeyArray,
        groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    getAggregationStrategyByBlockValSetMap(blockValSetMap).aggregateGroupByMV(length, groupKeysArray,
        groupByResultHolder, blockValSetMap);
  }

  @Override
  public List<Long> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return getAggregationStrategyByAggregationResult(aggregationResultHolder.getResult()).extractAggregationResult(
        aggregationResultHolder);
  }

  @Override
  public List<Long> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return getAggregationStrategyByAggregationResult(groupByResultHolder.getResult(groupKey)).extractGroupByResult(
        groupByResultHolder, groupKey);
  }

  @Override
  public List<Long> merge(List<Long> a, List<Long> b) {
    int length = a.size();
    Preconditions.checkState(length == b.size(), "The two operand arrays are not of the same size! provided %s, %s",
        length, b.size());

    LongArrayList result = toLongArrayList(a);
    long[] elements = result.elements();
    for (int i = 0; i < length; i++) {
      elements[i] += b.get(i);
    }
    return result;
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
  public LongArrayList extractFinalResult(List<Long> result) {
    return toLongArrayList(result);
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

  private static LongArrayList toLongArrayList(List<Long> longList) {
    return longList instanceof LongArrayList ? ((LongArrayList) longList).clone() : new LongArrayList(longList);
  }

  private int[] getCorrelationIds(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return blockValSetMap.get(_primaryCorrelationCol).getDictionaryIdsSV();
  }

  private int[][] getSteps(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = new int[_numSteps][];
    for (int n = 0; n < _numSteps; n++) {
      final BlockValSet stepBlockValSet = blockValSetMap.get(_stepExpressions.get(n));
      steps[n] = stepBlockValSet.getIntValuesSV();
    }
    return steps;
  }

  private boolean isSorted(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary primaryCorrelationDictionary = blockValSetMap.get(_primaryCorrelationCol).getDictionary();
    if (primaryCorrelationDictionary == null) {
      throw new IllegalArgumentException(
          "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported, please use a dictionary encoded "
              + "column.");
    }
    return primaryCorrelationDictionary.isSorted();
  }

  private SegmentAggregationStrategy<?, List<Long>> getAggregationStrategyByBlockValSetMap(
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return isSorted(blockValSetMap) ? _sortedAggregationStrategy : _bitmapAggregationStrategy;
  }

  private SegmentAggregationStrategy<?, List<Long>> getAggregationStrategyByAggregationResult(Object aggResult) {
    return aggResult instanceof SortedAggregationResult ? _sortedAggregationStrategy : _bitmapAggregationStrategy;
  }

  enum Option {
    STEPS("steps"),
    CORRELATE_BY("correlateby");

    final String _name;

    Option(String name) {
      _name = name;
    }

    boolean matches(ExpressionContext expression) {
      if (expression.getType() != ExpressionContext.Type.FUNCTION) {
        return false;
      }
      return _name.equals(expression.getFunction().getFunctionName());
    }

    Optional<ExpressionContext> find(List<ExpressionContext> expressions) {
      return expressions.stream().filter(this::matches).findFirst();
    }

    public List<ExpressionContext> getInputExpressions(List<ExpressionContext> expressions) {
      return this.find(expressions).map(exp -> exp.getFunction().getArguments())
          .orElseThrow(() -> new IllegalStateException("FUNNELCOUNT requires " + _name));
    }

    public ExpressionContext getFirstInputExpression(List<ExpressionContext> expressions) {
      return this.find(expressions)
          .flatMap(exp -> exp.getFunction().getArguments().stream().findFirst())
          .orElseThrow(() -> new IllegalStateException("FUNNELCOUNT: " + _name + " requires an argument."));
    }
  }

  /**
   * Interface for segment aggregation strategy.
   *
   * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads. The
   * result for each segment should be stored and passed in via the result holder.
   * There should be no assumptions beyond segment boundaries, different aggregation strategies may be utilized
   * across different segments for a given query.
   *
   * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
   * @param <I> Intermediate result at segment level (extracted from aforementioned aggregation result).
   */
  @ThreadSafe
  static abstract class SegmentAggregationStrategy<A, I> {

    /**
     * Returns an aggregation result for this aggregation strategy to be kept in a result holder (aggregation only).
     */
    abstract A createAggregationResult();

    public A getAggregationResultGroupBy(GroupByResultHolder groupByResultHolder, int groupKey) {
      A aggResult = groupByResultHolder.getResult(groupKey);
      if (aggResult == null) {
        aggResult = createAggregationResult();
        groupByResultHolder.setValueForKey(groupKey, aggResult);
      }
      return aggResult;
    }

    public A getAggregationResult(AggregationResultHolder aggregationResultHolder) {
      A aggResult = aggregationResultHolder.getResult();
      if (aggResult == null) {
        aggResult = createAggregationResult();
        aggregationResultHolder.setValue(aggResult);
      }
      return aggResult;
    }

    /**
     * Performs aggregation on the given block value sets (aggregation only).
     */
    abstract void aggregate(int length, AggregationResultHolder aggregationResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap);

    /**
     * Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
     * columns).
     */
    abstract void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap);

    /**
     * Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
     * columns).
     */
    abstract void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap);

    /**
     * Extracts the intermediate result from the aggregation result holder (aggregation only).
     */
    public I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
      return extractIntermediateResult(aggregationResultHolder.getResult());
    }

    /**
     * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
     */
    public I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
      return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
    }

    abstract I extractIntermediateResult(A aggregationResult);
  }

  /**
   * Aggregation strategy leveraging roaring bitmap algebra (unions/intersections).
   */
  class BitmapAggregationStrategy extends SegmentAggregationStrategy<RoaringBitmap[], List<Long>> {

    @Override
    public RoaringBitmap[] createAggregationResult() {
      final RoaringBitmap[] stepsBitmaps = new RoaringBitmap[_numSteps];
      for (int n = 0; n < _numSteps; n++) {
        stepsBitmaps[n] = new RoaringBitmap();
      }
      return stepsBitmaps;
    }

    @Override
    public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      final RoaringBitmap[] stepsBitmaps = getAggregationResult(aggregationResultHolder);

      for (int n = 0; n < _numSteps; n++) {
        for (int i = 0; i < length; i++) {
          if (steps[n][i] > 0) {
            stepsBitmaps[n].add(correlationIds[i]);
          }
        }
      }
    }

    @Override
    public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      for (int n = 0; n < _numSteps; n++) {
        for (int i = 0; i < length; i++) {
          final int groupKey = groupKeyArray[i];
          if (steps[n][i] > 0) {
            getAggregationResultGroupBy(groupByResultHolder, groupKey)[n].add(correlationIds[i]);
          }
        }
      }
    }

    @Override
    public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      for (int n = 0; n < _numSteps; n++) {
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            if (steps[n][i] > 0) {
              getAggregationResultGroupBy(groupByResultHolder, groupKey)[n].add(correlationIds[i]);
            }
          }
        }
      }
    }

    @Override
    public List<Long> extractIntermediateResult(RoaringBitmap[] stepsBitmaps) {
      if (stepsBitmaps == null) {
        return new LongArrayList(_numSteps);
      }

      long[] result = new long[_numSteps];
      result[0] = stepsBitmaps[0].getCardinality();
      for (int i = 1; i < _numSteps; i++) {
        // intersect this step with previous step
        stepsBitmaps[i].and(stepsBitmaps[i - 1]);
        result[i] = stepsBitmaps[i].getCardinality();
      }
      return LongArrayList.wrap(result);
    }
  }

  /**
   * Aggregation strategy for segments sorted by the main correlation column.
   */
  class SortedAggregationStrategy extends SegmentAggregationStrategy<SortedAggregationResult, List<Long>> {

    @Override
    public SortedAggregationResult createAggregationResult() {
      return new SortedAggregationResult();
    }

    @Override
    public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      final SortedAggregationResult agg = getAggregationResult(aggregationResultHolder);

      for (int i = 0; i < length; i++) {
        agg.sortedCount(steps, i, correlationIds[i]);
      }
    }

    @Override
    public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      for (int i = 0; i < length; i++) {
        final int groupKey = groupKeyArray[i];
        final SortedAggregationResult agg = getAggregationResultGroupBy(groupByResultHolder, groupKey);

        agg.sortedCount(steps, i, correlationIds[i]);
      }
    }

    @Override
    public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          final SortedAggregationResult agg = getAggregationResultGroupBy(groupByResultHolder, groupKey);

          agg.sortedCount(steps, i, correlationIds[i]);
        }
      }
    }

    @Override
    public List<Long> extractIntermediateResult(SortedAggregationResult agg) {
      if (agg == null) {
        return new LongArrayList(_numSteps);
      }

      return LongArrayList.wrap(agg.extractResult());
    }
  }

  /**
   * Aggregation result data structure leveraged by sorted aggregation strategy.
   */
  class SortedAggregationResult {
    public long[] _stepCounters = new long[_numSteps];
    public int _lastCorrelationId = Integer.MIN_VALUE;
    public boolean[] _correlatedSteps = new boolean[_numSteps];

    public void sortedCount(int[][] steps, int i, int correlationId) {
      if (correlationId == _lastCorrelationId) {
        // same correlation as before, keep accumulating.
        for (int n = 0; n < _numSteps; n++) {
          _correlatedSteps[n] |= steps[n][i] > 0;
        }
      } else {
        // End of correlation group, calculate funnel conversion counts
        incrStepCounters();

        // initialize next correlation group
        for (int n = 0; n < _numSteps; n++) {
          _correlatedSteps[n] = steps[n][i] > 0;
        }
        _lastCorrelationId = correlationId;
      }
    }

    void incrStepCounters() {
      for (int n = 0; n < _numSteps; n++) {
        if (!_correlatedSteps[n]) {
          break;
        }
        _stepCounters[n]++;
      }
    }

    public long[] extractResult() {
      // count last correlation id left open
      incrStepCounters();

      return _stepCounters;
    }
  }
}
