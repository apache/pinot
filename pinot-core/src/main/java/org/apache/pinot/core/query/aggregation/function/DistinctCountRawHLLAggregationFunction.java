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
import javax.annotation.Nonnull;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.SerializedHLL;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class DistinctCountRawHLLAggregationFunction implements AggregationFunction<HyperLogLog, SerializedHLL> {

  private final DistinctCountHLLAggregationFunction _distinctCountHLLAggregationFunction = new DistinctCountHLLAggregationFunction();

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLL;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.DISTINCTCOUNTRAWHLL.getName() + "_" + column;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    _distinctCountHLLAggregationFunction.accept(visitor);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return _distinctCountHLLAggregationFunction.createAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return _distinctCountHLLAggregationFunction.createGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    _distinctCountHLLAggregationFunction.aggregate(length, aggregationResultHolder, blockValSets);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    _distinctCountHLLAggregationFunction.aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSets);
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    _distinctCountHLLAggregationFunction.aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSets);
  }

  @Nonnull
  @Override
  public HyperLogLog extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    return _distinctCountHLLAggregationFunction.extractAggregationResult(aggregationResultHolder);
  }

  @Nonnull
  @Override
  public HyperLogLog extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    return _distinctCountHLLAggregationFunction.extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Nonnull
  @Override
  public HyperLogLog merge(@Nonnull HyperLogLog intermediateResult1, @Nonnull HyperLogLog intermediateResult2) {
    return _distinctCountHLLAggregationFunction.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return _distinctCountHLLAggregationFunction.isIntermediateResultComparable();
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return _distinctCountHLLAggregationFunction.getIntermediateResultColumnType();
  }

  @Nonnull
  @Override
  public SerializedHLL extractFinalResult(@Nonnull HyperLogLog intermediateResult) {
    return SerializedHLL.of(intermediateResult);
  }
}