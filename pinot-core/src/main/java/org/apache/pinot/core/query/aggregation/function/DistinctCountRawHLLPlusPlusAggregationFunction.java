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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.SerializedHLLPlusPlus;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class DistinctCountRawHLLPlusPlusAggregationFunction
    extends BaseSingleInputAggregationFunction<HyperLogLogPlus, SerializedHLLPlusPlus> {
  private final DistinctCountHLLPlusPlusAggregationFunction _distinctCountHLLPlusPlusAggregationFunction;

  public DistinctCountRawHLLPlusPlusAggregationFunction(List<ExpressionContext> arguments) {
    this(arguments.get(0), new DistinctCountHLLPlusPlusAggregationFunction(arguments));
  }

  DistinctCountRawHLLPlusPlusAggregationFunction(ExpressionContext expression,
      DistinctCountHLLPlusPlusAggregationFunction distinctCountHLLPlusPlusAggregationFunction) {
    super(expression);
    _distinctCountHLLPlusPlusAggregationFunction = distinctCountHLLPlusPlusAggregationFunction;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLLPLUSPLUS;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _distinctCountHLLPlusPlusAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _distinctCountHLLPlusPlusAggregationFunction
        .createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLPlusPlusAggregationFunction
        .aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLPlusPlusAggregationFunction
        .aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLPlusPlusAggregationFunction
        .aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public HyperLogLogPlus extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctCountHLLPlusPlusAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public HyperLogLogPlus extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _distinctCountHLLPlusPlusAggregationFunction
        .extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public HyperLogLogPlus merge(HyperLogLogPlus intermediateResult1, HyperLogLogPlus intermediateResult2) {
    return _distinctCountHLLPlusPlusAggregationFunction
        .merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _distinctCountHLLPlusPlusAggregationFunction.isIntermediateResultComparable();
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return _distinctCountHLLPlusPlusAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public SerializedHLLPlusPlus extractFinalResult(HyperLogLogPlus intermediateResult) {
    return new SerializedHLLPlusPlus(intermediateResult);
  }
}
