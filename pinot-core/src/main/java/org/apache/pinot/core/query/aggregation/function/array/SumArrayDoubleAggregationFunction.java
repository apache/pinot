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


public class SumArrayDoubleAggregationFunction extends BaseSingleInputAggregationFunction<double[], double[]> {

  public SumArrayDoubleAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "SUM_ARRAY"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUMARRAYDOUBLE;
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
    double[][] values = blockValSet.getDoubleValuesMV();
    // The accumulator is created inside the range, so a block with no non-null row leaves the holder untouched and
    // extractFinalResult sees the null that means nothing was aggregated
    forEachNotNull(length, blockValSet, (from, to) -> {
      double[] sums = aggregationResultHolder.getResult();
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
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
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
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        double[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupByResultHolder, values, groupKey);
        }
      }
    });
  }

  private void setGroupByResult(GroupByResultHolder groupByResultHolder, double[] values, int groupKey) {
    double[] sums = groupByResultHolder.getResult(groupKey);
    double[] newSums = add(sums, values);
    if (newSums != sums) {
      groupByResultHolder.setValueForKey(groupKey, newSums);
    }
  }

  /// Adds the values to the sums and returns the sums, which is a new array when there are none yet or the values are
  /// longer.
  private static double[] add(@Nullable double[] sums, double[] values) {
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
  public double[] extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public double[] extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public double[] merge(double[] intermediateResult1, double[] intermediateResult2) {
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
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    // AggregationResultsBlock#setIntermediateResult and AggregationFunctionUtils#getIntermediateResult only serialize
    // intermediate results whose stored type is INT/LONG/DOUBLE/STRING/OBJECT, so we need the OBJECT/custom ser-de path
    // for the sum array.
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(double[] sums) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.DoubleArrayList.getValue(),
        ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE.serialize(sums));
  }

  @Override
  public double[] deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.DOUBLE_ARRAY_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE_ARRAY;
  }

  @Nullable
  @Override
  public double[] extractFinalResult(@Nullable double[] sums) {
    return sums;
  }

  @Override
  public double[] mergeFinalResult(double[] finalResult1, double[] finalResult2) {
    double[] mergedResult = Arrays.copyOf(finalResult1, Math.max(finalResult1.length, finalResult2.length));
    for (int i = 0; i < finalResult2.length; i++) {
      mergedResult[i] += finalResult2[i];
    }
    return mergedResult;
  }
}
