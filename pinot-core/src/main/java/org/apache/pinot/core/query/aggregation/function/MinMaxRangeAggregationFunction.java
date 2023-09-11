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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class MinMaxRangeAggregationFunction extends BaseSingleInputAggregationFunction<MinMaxRangePair, Double> {

  public MinMaxRangeAggregationFunction(List<ExpressionContext> arguments) {
    super(verifyArguments(arguments));
  }

  private static ExpressionContext verifyArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(arguments.size() == 1, "MIN_MAX_RANGE expects 1 argument, got: %s", arguments.size());
    return arguments.get(0);
  }

  protected MinMaxRangeAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINMAXRANGE;
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
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        if (value < min) {
          min = value;
        }
        if (value > max) {
          max = value;
        }
      }
    } else {
      // Serialized MinMaxRangePair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        MinMaxRangePair minMaxRangePair = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
        double minValue = minMaxRangePair.getMin();
        double maxValue = minMaxRangePair.getMax();
        if (minValue < min) {
          min = minValue;
        }
        if (maxValue > max) {
          max = maxValue;
        }
      }
    }
    setAggregationResult(aggregationResultHolder, min, max);
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double min, double max) {
    MinMaxRangePair minMaxRangePair = aggregationResultHolder.getResult();
    if (minMaxRangePair == null) {
      aggregationResultHolder.setValue(new MinMaxRangePair(min, max));
    } else {
      minMaxRangePair.apply(min, max);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, value, value);
      }
    } else {
      // Serialized MinMaxRangePair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        MinMaxRangePair minMaxRangePair = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i], groupByResultHolder, minMaxRangePair.getMin(), minMaxRangePair.getMax());
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, value);
        }
      }
    } else {
      // Serialized MinMaxRangePair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        MinMaxRangePair minMaxRangePair = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
        double min = minMaxRangePair.getMin();
        double max = minMaxRangePair.getMax();
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, min, max);
        }
      }
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double min, double max) {
    MinMaxRangePair minMaxRangePair = groupByResultHolder.getResult(groupKey);
    if (minMaxRangePair == null) {
      groupByResultHolder.setValueForKey(groupKey, new MinMaxRangePair(min, max));
    } else {
      minMaxRangePair.apply(min, max);
    }
  }

  @Override
  public MinMaxRangePair extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    MinMaxRangePair minMaxRangePair = aggregationResultHolder.getResult();
    if (minMaxRangePair == null) {
      return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return minMaxRangePair;
    }
  }

  @Override
  public MinMaxRangePair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    MinMaxRangePair minMaxRangePair = groupByResultHolder.getResult(groupKey);
    if (minMaxRangePair == null) {
      return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return minMaxRangePair;
    }
  }

  @Override
  public MinMaxRangePair merge(MinMaxRangePair intermediateResult1, MinMaxRangePair intermediateResult2) {
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(MinMaxRangePair intermediateResult) {
    return intermediateResult.getMax() - intermediateResult.getMin();
  }
}
