/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.utils.DoubleVectorOpUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code FunnelAggregationFunction} calculates the number of step conversions for a given partition column and a
 * list of boolean expressions.
 * <p>IMPORTANT: This function relies on the partition column being partitioned for each segment, where there is no
 * common values within different segments.
 * <p>This function calculates the exact number of step matches per partition key within the segment, then simply
 * sums up the results from different segments to get the final result.
 */
public class FunnelAggregationFunction implements AggregationFunction<DoubleArrayList, DoubleArrayList> {
  final List<ExpressionContext> _expressions;

  final List<ExpressionContext> _sequenceByExpressions;
  final List<ExpressionContext> _correlateByExpressions;
  final ExpressionContext _primaryCorrelationCol;
  final List<ExpressionContext> _stepExpressions;
  final int _numSteps;

  public FunnelAggregationFunction(List<ExpressionContext> expressions) {
    _expressions = expressions;
    //_optionExpressions = FunnelUtils.optionInputExpressions(expressions);
    _sequenceByExpressions = Option.SEQUENCE_BY.getInputExpressions(expressions);
    _correlateByExpressions = Option.CORRELATE_BY.getInputExpressions(expressions);
    _primaryCorrelationCol = _correlateByExpressions.get(0);
    _stepExpressions = Option.STEPS.getInputExpressions(expressions);
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
    inputs.addAll(_sequenceByExpressions);
    inputs.addAll(_correlateByExpressions);
    inputs.addAll(_stepExpressions);
    return inputs;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNEL;
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
    assertPreconditions(blockValSetMap);
    /*
      if (primaryCorrelationDictionary.isSorted()) {
        // Its sorted, we are sorted!
        sortedCorrelationFunnel(length, aggregationResultHolder, blockValSetMap);
        return;
      }
    */

    final int[] correlationIds = getCorrelationIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);

    final RoaringBitmap[] stepsBitmaps = getStepsBitmaps(aggregationResultHolder);

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
    assertPreconditions(blockValSetMap);

    final int[] correlationIds = getCorrelationIds(blockValSetMap);
    final int[][] steps = getSteps(blockValSetMap);

    for (int n = 0; n < _numSteps; n++) {
      for (int i = 0; i < length; i++) {
        final int groupKey = groupKeyArray[i];
        if (steps[n][i] > 0) {
          getStepsBitmapsGroupBySV(groupByResultHolder, groupKey)[n].add(correlationIds[i]);
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    assertPreconditions(blockValSetMap);

    final int[] correlationIds = getCorrelationIds(blockValSetMap);

    int[][] steps = new int[_numSteps][];
    for (int n = 0; n < _numSteps; n++) {
      final BlockValSet stepBlockValSet = blockValSetMap.get(_stepExpressions.get(n));
      steps[n] = stepBlockValSet.getIntValuesSV();
    }

    for (int n = 0; n < _numSteps; n++) {
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          if (steps[n][i] > 0) {
            getStepsBitmapsGroupBySV(groupByResultHolder, groupKey)[n].add(correlationIds[i]);
          }
        }
      }
    }
  }

  private RoaringBitmap[] getStepsBitmaps(AggregationResultHolder aggregationResultHolder) {
    RoaringBitmap[] stepsBitmaps = aggregationResultHolder.getResult();
    if (stepsBitmaps == null) {
      stepsBitmaps = initStepsBitmaps();
      aggregationResultHolder.setValue(stepsBitmaps);
    }
    return stepsBitmaps;
  }

  /**
   * Method to extract segment level intermediate result from the inner segment result.
   */
  @Override
  public DoubleArrayList extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return convertStepsBitmapsToDoubleArrayList(aggregationResultHolder.getResult());
  }

  /**
   * Method to extract segment level intermediate result from the inner segment result.
   */
  @Override
  public DoubleArrayList extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return convertStepsBitmapsToDoubleArrayList(groupByResultHolder.getResult(groupKey));
  }

  @Override
  public DoubleArrayList merge(DoubleArrayList a, DoubleArrayList b) {
    DoubleVectorOpUtils.vectorAdd(a, b);
    return a;
  }

  public void merge(long[] agg, long[] values) {
    int length = agg.length;
    Preconditions.checkState(length == values.length,
        "The two operand arrays are not of the same size! provided %s, %s", length, values.length);
    for (int i = 0; i < length; i++) {
      agg[i] += values[i];
    }
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE_ARRAY;
  }

  private DoubleArrayList convertStepsBitmapsToDoubleArrayList(RoaringBitmap[] stepsBitmaps) {
    double[] result = new double[_numSteps];
    if (stepsBitmaps != null) {
      result[0] = stepsBitmaps[0].getCardinality();
      for (int i = 1; i < _numSteps; i++) {
        // intersect this step with previous step
        stepsBitmaps[i].and(stepsBitmaps[i - 1]);
        result[i] = stepsBitmaps[i].getCardinality();
      }
    }
    return DoubleArrayList.wrap(result);
  }

  @Override
  public DoubleArrayList extractFinalResult(DoubleArrayList result) {
    // Add conversion rate %
    double nominator = result.getDouble(result.size() - 1);
    double denominator = result.getDouble(0);
    double cr = denominator == 0 ? 0 : 100d * nominator / denominator;

    DoubleArrayList finalResult = result.clone();
    finalResult.add(cr);
    return finalResult;
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

  private RoaringBitmap[] initStepsBitmaps() {
    final RoaringBitmap[] stepsBitmaps = new RoaringBitmap[_numSteps];
    for (int n = 0; n < _numSteps; n++) {
      stepsBitmaps[n] = new RoaringBitmap();
    }
    return stepsBitmaps;
  }

  private RoaringBitmap[] getStepsBitmapsGroupBySV(GroupByResultHolder groupByResultHolder, int groupKey) {
    RoaringBitmap[] stepsBitmaps = groupByResultHolder.getResult(groupKey);
    if (stepsBitmaps == null) {
      stepsBitmaps = initStepsBitmaps();
      groupByResultHolder.setValueForKey(groupKey, stepsBitmaps);
    }
    return stepsBitmaps;
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

  private void assertPreconditions(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary primaryCorrelationDictionary = blockValSetMap.get(_primaryCorrelationCol).getDictionary();
    if (primaryCorrelationDictionary == null) {
      throw new IllegalArgumentException(
          "PARTITION_BY column in FUNNEL aggregation function not supported, please use a dictionary encoded column.");
    }
  }

  // TODO: How much faster is it? What if we sort by name instead?
  private void sortedCorrelationFunnel(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[] correlationIds = getCorrelationIds(blockValSetMap);

    int[][] steps = new int[_numSteps][];
    for (int n = 0; n < _numSteps; n++) {
      final BlockValSet stepBlockValSet = blockValSetMap.get(_stepExpressions.get(n));
      steps[n] = stepBlockValSet.getIntValuesSV();
    }

    // counts correlated funnel conversion per step
    final long[] stepCounters = new long[_numSteps];

    // tracks whether the step was taken by a given correlation
    final boolean[] correlatedSteps = new boolean[_numSteps];

    int lastCorrelationId = correlationIds[0];
    for (int i = 0; i < length; i++) {
      int correlationId = correlationIds[i];
      if (correlationId == lastCorrelationId) {
        // same correlation as before, keep accumulating.
        for (int n = 0; n < _numSteps; n++) {
          correlatedSteps[n] |= steps[n][i] > 0;
        }
      } else {
        // End off correlation group, calculate funnel conversion counts
        boolean converted = true;
        for (int n = 0; n < _numSteps; n++) {
          converted &= correlatedSteps[n];
          if (!converted) {
            break;
          }
          stepCounters[n]++;
        }

        // initialize next correlation group
        for (int n = 0; n < _numSteps; n++) {
          correlatedSteps[n] = steps[n][i] > 0;
        }
      }
      lastCorrelationId = correlationId;
    }
    // TODO: What to do with the last open correlation group? For now we ignore it.

    final long[] aggStepCounters = aggregationResultHolder.getResult();
    if (aggStepCounters == null) {
      aggregationResultHolder.setValue(stepCounters);
    } else {
      merge(aggStepCounters, stepCounters);
    }
  }

  private DoubleArrayList convertLongArrayToDoubleArrayList(long[] intermediateResult) {
    double[] result = new double[_numSteps];
    if (intermediateResult != null) {
      for (int i = 0; i < _numSteps; i++) {
        result[i] = intermediateResult[i];
      }
    }
    return DoubleArrayList.wrap(result);
  }

  enum Option {
    STEPS("steps"),
    CORRELATE_BY("correlateby"),
    SEQUENCE_BY("sequenceby");

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

    List<ExpressionContext> getArguments(List<ExpressionContext> expressions) {
      return this.find(expressions).map(exp -> exp.getFunction().getArguments())
          .orElseThrow(() -> new IllegalStateException("FUNNEL requires " + _name));
    }

    public List<ExpressionContext> getInputExpressions(List<ExpressionContext> expressions) {
      return this.getArguments(expressions);
    }
  }
}
