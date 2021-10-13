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

import java.util.List;
import java.util.Map;
import org.apache.datasketches.hll.HllSketch;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.SerializedHLLSketch;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class DistinctCountRawHLLSketchAggregationFunction
    extends BaseSingleInputAggregationFunction<HllSketch, SerializedHLLSketch> {
  private final DistinctCountHLLSketchAggregationFunction _distinctCountHLLSketchAggregationFunction;

  public DistinctCountRawHLLSketchAggregationFunction(List<ExpressionContext> arguments) {
    this(arguments.get(0), new DistinctCountHLLSketchAggregationFunction(arguments));
  }

  DistinctCountRawHLLSketchAggregationFunction(ExpressionContext expression,
      DistinctCountHLLSketchAggregationFunction distinctCountHLLSketchAggregationFunction) {
    super(expression);
    _distinctCountHLLSketchAggregationFunction = distinctCountHLLSketchAggregationFunction;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLLSKETCH;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _distinctCountHLLSketchAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _distinctCountHLLSketchAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLSketchAggregationFunction.aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLSketchAggregationFunction
        .aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    _distinctCountHLLSketchAggregationFunction
        .aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public HllSketch extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctCountHLLSketchAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public HllSketch extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _distinctCountHLLSketchAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public HllSketch merge(HllSketch intermediateResult1, HllSketch intermediateResult2) {
    return _distinctCountHLLSketchAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _distinctCountHLLSketchAggregationFunction.isIntermediateResultComparable();
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return _distinctCountHLLSketchAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public SerializedHLLSketch extractFinalResult(HllSketch intermediateResult) {
    return new SerializedHLLSketch(intermediateResult);
  }
}
