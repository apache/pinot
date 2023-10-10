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
import org.apache.pinot.segment.local.customobject.SerializedHLLPlus;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class DistinctCountRawHLLPlusAggregationFunction
    extends BaseSingleInputAggregationFunction<HyperLogLogPlus, SerializedHLLPlus> {
  private final DistinctCountHLLPlusAggregationFunction _distinctCountHLLPlusAggregationFunction;

  public DistinctCountRawHLLPlusAggregationFunction(List<ExpressionContext> arguments) {
    this(arguments.get(0), new DistinctCountHLLPlusAggregationFunction(arguments));
  }

  DistinctCountRawHLLPlusAggregationFunction(ExpressionContext expression,
      DistinctCountHLLPlusAggregationFunction distinctCountHLLPlusAggregationFunction) {
    super(expression);
    _distinctCountHLLPlusAggregationFunction = distinctCountHLLPlusAggregationFunction;
  }

  public DistinctCountHLLPlusAggregationFunction getDistinctCountHLLPlusAggregationFunction() {
    return _distinctCountHLLPlusAggregationFunction;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLLPLUS;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _distinctCountHLLPlusAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _distinctCountHLLPlusAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLPlusAggregationFunction.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLPlusAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder,
        blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLPlusAggregationFunction.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder,
        blockValSetMap);
  }

  @Override
  public HyperLogLogPlus extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctCountHLLPlusAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public HyperLogLogPlus extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _distinctCountHLLPlusAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public HyperLogLogPlus merge(HyperLogLogPlus intermediateResult1, HyperLogLogPlus intermediateResult2) {
    return _distinctCountHLLPlusAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return _distinctCountHLLPlusAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public SerializedHLLPlus extractFinalResult(HyperLogLogPlus intermediateResult) {
    return new SerializedHLLPlus(intermediateResult);
  }
}
