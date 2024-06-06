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
package org.apache.pinot.core.query.aggregation.function.array;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class SumArrayLongAggregationFunction extends BaseSingleInputAggregationFunction<LongArrayList, LongArrayList> {

  public SumArrayLongAggregationFunction(List<ExpressionContext> arguments) {
    super(verifySingleArgument(arguments, "SUM_ARRAY"));
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUMARRAYLONG;
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
    long[][] values = blockValSetMap.get(_expression).getLongValuesMV();
    if (aggregationResultHolder.getResult() == null) {
      aggregationResultHolder.setValue(new LongArrayList());
    }
    LongArrayList result = aggregationResultHolder.getResult();
    for (int i = 0; i < length; i++) {
      long[] value = values[i];
      aggregateMerge(value, result);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[][] valuesArray = blockValSetMap.get(_expression).getLongValuesMV();
    for (int i = 0; i < length; i++) {
      long[] values = valuesArray[i];
      int groupKey = groupKeyArray[i];
      setGroupByResult(groupByResultHolder, values, groupKey);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    long[][] valuesArray = blockValSetMap.get(_expression).getLongValuesMV();
    for (int i = 0; i < length; i++) {
      long[] values = valuesArray[i];
      int[] groupKeys = groupKeysArray[i];
      for (int groupKey : groupKeys) {
        setGroupByResult(groupByResultHolder, values, groupKey);
      }
    }
  }

  private void setGroupByResult(GroupByResultHolder groupByResultHolder, long[] values, int groupKey) {
    LongArrayList sumList = groupByResultHolder.getResult(groupKey);
    if (sumList == null) {
      sumList = new LongArrayList();
      groupByResultHolder.setValueForKey(groupKey, sumList);
    }
    aggregateMerge(values, sumList);
  }

  private void aggregateMerge(long[] values, LongArrayList sumList) {
    for (int j = sumList.size(); j < values.length; j++) {
      sumList.add(0L);
    }
    for (int j = 0; j < values.length; j++) {
      sumList.set(j, sumList.getLong(j) + values[j]);
    }
  }

  @Override
  public LongArrayList extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public LongArrayList extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public LongArrayList merge(LongArrayList intermediateResult1, LongArrayList intermediateResult2) {
    if (intermediateResult1.size() < intermediateResult2.size()) {
      for (int i = 0; i < intermediateResult1.size(); i++) {
        intermediateResult2.set(i, intermediateResult1.getLong(i) + intermediateResult2.getLong(i));
      }
      return intermediateResult2;
    }
    for (int i = 0; i < intermediateResult2.size(); i++) {
      intermediateResult1.set(i, intermediateResult1.getLong(i) + intermediateResult2.getLong(i));
    }
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.LONG_ARRAY;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG_ARRAY;
  }

  @Override
  public LongArrayList extractFinalResult(LongArrayList result) {
    return result;
  }
}
