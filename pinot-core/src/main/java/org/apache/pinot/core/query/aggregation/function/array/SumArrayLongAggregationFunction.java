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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class SumArrayLongAggregationFunction extends BaseSingleInputAggregationFunction<long[], long[]> {

  public SumArrayLongAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "SUM_ARRAY"), nullHandlingEnabled);
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
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[][] values = blockValSet.getLongValuesMV();
    // The accumulator is created inside the range, so a block with no non-null row leaves the holder untouched and
    // extractFinalResult sees the null that means nothing was aggregated
    forEachNotNull(length, blockValSet, (from, to) -> {
      long[] sums = aggregationResultHolder.getResult();
      for (int i = from; i < to; i++) {
        sums = add(sums, values[i]);
      }
      aggregationResultHolder.setValue(sums);
    });
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[][] valuesArray = blockValSet.getLongValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        setGroupByResult(groupByResultHolder, valuesArray[i], groupKeyArray[i]);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[][] valuesArray = blockValSet.getLongValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        long[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupByResultHolder, values, groupKey);
        }
      }
    });
  }

  private void setGroupByResult(GroupByResultHolder groupByResultHolder, long[] values, int groupKey) {
    long[] sums = groupByResultHolder.getResult(groupKey);
    long[] newSums = add(sums, values);
    if (newSums != sums) {
      groupByResultHolder.setValueForKey(groupKey, newSums);
    }
  }

  /// Adds the values to the sums and returns the sums, which is a new array when there are none yet or the values are
  /// longer.
  private static long[] add(@Nullable long[] sums, long[] values) {
    if (sums == null) {
      return values.clone();
    }
    if (sums.length < values.length) {
      sums = Arrays.copyOf(sums, values.length);
    }
    for (int i = 0; i < values.length; i++) {
      sums[i] += values[i];
    }
    return sums;
  }

  @Nullable
  @Override
  public long[] extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public long[] extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public long[] merge(long[] intermediateResult1, long[] intermediateResult2) {
    if (intermediateResult1.length < intermediateResult2.length) {
      for (int i = 0; i < intermediateResult1.length; i++) {
        intermediateResult2[i] += intermediateResult1[i];
      }
      return intermediateResult2;
    }
    for (int i = 0; i < intermediateResult2.length; i++) {
      intermediateResult1[i] += intermediateResult2[i];
    }
    return intermediateResult1;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(long[] sums) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.LongArrayList.getValue(),
        ObjectSerDeUtils.LONG_ARRAY_SER_DE.serialize(sums));
  }

  @Override
  public long[] deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.LONG_ARRAY_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    // AggregationResultsBlock#setIntermediateResult and AggregationFunctionUtils#getIntermediateResult only support
    // INT/LONG/DOUBLE/STRING/OBJECT stored types, so the sum array must use OBJECT to piggyback on the custom ser-de
    // implemented in ObjectSerDeUtils.
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG_ARRAY;
  }

  @Nullable
  @Override
  public long[] extractFinalResult(@Nullable long[] sums) {
    return sums;
  }

  @Override
  public long[] mergeFinalResult(long[] finalResult1, long[] finalResult2) {
    long[] mergedResult = Arrays.copyOf(finalResult1, Math.max(finalResult1.length, finalResult2.length));
    for (int i = 0; i < finalResult2.length; i++) {
      mergedResult[i] += finalResult2[i];
    }
    return mergedResult;
  }
}
