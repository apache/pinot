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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.SerializedHLL;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class DistinctCountRawHLLAggregationFunction implements AggregationFunction<HyperLogLog, SerializedHLL> {
  private final DistinctCountHLLAggregationFunction _distinctCountHLLAggregationFunction;

  public DistinctCountRawHLLAggregationFunction() {
    this(new DistinctCountHLLAggregationFunction());
  }

  DistinctCountRawHLLAggregationFunction(DistinctCountHLLAggregationFunction distinctCountHLLAggregationFunction) {
    _distinctCountHLLAggregationFunction = distinctCountHLLAggregationFunction;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLL;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLL.getName() + "_" + column;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    _distinctCountHLLAggregationFunction.accept(visitor);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _distinctCountHLLAggregationFunction.createAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _distinctCountHLLAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    _distinctCountHLLAggregationFunction.aggregate(length, aggregationResultHolder, blockValSets);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    _distinctCountHLLAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSets);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    _distinctCountHLLAggregationFunction.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSets);
  }

  @Override
  public HyperLogLog extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctCountHLLAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Override
  public HyperLogLog extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return _distinctCountHLLAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public HyperLogLog merge(HyperLogLog intermediateResult1, HyperLogLog intermediateResult2) {
    return _distinctCountHLLAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _distinctCountHLLAggregationFunction.isIntermediateResultComparable();
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return _distinctCountHLLAggregationFunction.getIntermediateResultColumnType();
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public SerializedHLL extractFinalResult(HyperLogLog intermediateResult) {
    return SerializedHLL.of(intermediateResult);
  }
}
