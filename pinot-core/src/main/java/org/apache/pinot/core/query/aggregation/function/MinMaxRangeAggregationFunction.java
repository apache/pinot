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

import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;


public class MinMaxRangeAggregationFunction implements AggregationFunction<MinMaxRangePair, Double> {

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINMAXRANGE;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.MINMAXRANGE.getName() + "_" + column;
  }

  @Override
  public String getResultColumnName(String column) {
    return AggregationFunctionType.MINMAXRANGE.getName() + "(" + column + ")";
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    if (blockValSets[0].getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSets[0].getDoubleValuesSV();
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
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
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
      BlockValSet... blockValSets) {
    if (blockValSets[0].getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSets[0].getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, value, value);
      }
    } else {
      // Serialized MinMaxRangePair
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        MinMaxRangePair minMaxRangePair = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i], groupByResultHolder, minMaxRangePair.getMin(), minMaxRangePair.getMax());
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    if (blockValSets[0].getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSets[0].getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, value);
        }
      }
    } else {
      // Serialized MinMaxRangePair
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
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
  public boolean isIntermediateResultComparable() {
    return true;
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
