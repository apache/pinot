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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class MaxStringAggregationFunction extends NullableSingleInputAggregationFunction<String, String> {

  public MaxStringAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAX2;
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
    String[] values = blockValSet.getStringValuesSV();

    String max = foldNotNull(length, blockValSet, null, (accum, from, to) -> {
      String innerMax = values[from];
      for (int i = from + 1; i < to; i++) {
        innerMax = innerMax.compareTo(values[i]) < 0 ? values[i] : innerMax;
      }
      return accum == null ? innerMax : innerMax.compareTo(accum) < 0 ? accum : innerMax;
    });

    if (max != null) {
      String otherMax = aggregationResultHolder.getResult();
      if (otherMax == null) {
        // If the other max is null, we set the value directly
        aggregationResultHolder.setValue(max);
      } else if (max.compareTo(otherMax) < 0) {
        aggregationResultHolder.setValue(max);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] valueArray = blockValSet.getStringValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) ->
          aggregateBySV(valueArray, groupKeyArray, groupByResultHolder, from, to));
    } else {
      aggregateBySV(valueArray, groupKeyArray, groupByResultHolder, 0, length);
    }
  }

  private void aggregateBySV(
      String[] valueArray, int[] groupKeyArray, GroupByResultHolder groupByResultHolder, int from, int to) {
    for (int i = from; i < to; i++) {
      String value = valueArray[i];
      int groupKey = groupKeyArray[i];
      String result = groupByResultHolder.getResult(groupKey);
      if (result == null || value.compareTo(result) > 0) {
        groupByResultHolder.setValueForKey(groupKey, value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] valueArray = blockValSet.getStringValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) ->
        aggregateByMV(groupKeysArray, groupByResultHolder, valueArray, from, to));
    } else {
      aggregateByMV(groupKeysArray, groupByResultHolder, valueArray, 0, length);
    }
  }

  private static void aggregateByMV(
      int[][] groupKeysArray, GroupByResultHolder groupByResultHolder, String[] valueArray, int from, int to) {
    for (int i = from; i < to; i++) {
      String value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        String result = groupByResultHolder.getResult(groupKey);
        if (result == null || value.compareTo(result) > 0) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Nullable
  @Override
  public String extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public String extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Nullable
  @Override
  public String merge(@Nullable String intermediateResult1, @Nullable String intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }
    if (intermediateResult1.compareTo(intermediateResult2) > 0) {
      return intermediateResult1;
    }
    return intermediateResult2;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.STRING;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.STRING;
  }

  @Nullable
  @Override
  public String extractFinalResult(@Nullable String s) {
    return s;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(String s) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.String.getValue(),
        ObjectSerDeUtils.STRING_SER_DE.serialize(s));
  }

  @Override
  public String deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.STRING_SER_DE.deserialize(customObject.getBuffer());
  }
}
