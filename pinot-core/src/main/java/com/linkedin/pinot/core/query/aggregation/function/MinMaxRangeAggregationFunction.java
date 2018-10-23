/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.ObjectSerDeUtils;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import javax.annotation.Nonnull;


public class MinMaxRangeAggregationFunction implements AggregationFunction<MinMaxRangePair, Double> {

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINMAXRANGE;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.MINMAXRANGE.getName() + "_" + column;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < length; i++) {
          double value = valueArray[i];
          if (value < min) {
            min = value;
          }
          if (value > max) {
            max = value;
          }
        }
        setAggregationResult(aggregationResultHolder, min, max);
        break;
      case BYTES:
        // Serialized MinMaxRangePair
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < length; i++) {
          MinMaxRangePair value = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
          if (value.getMin() < min) {
            min = value.getMin();
          }
          if (value.getMax() > max) {
            max = value.getMax();
          }
        }
        setAggregationResult(aggregationResultHolder, min, max);
        break;
      default:
        throw new IllegalStateException("Illegal data type for MIN_MAX_RANGE aggregation function: " + valueType);
    }
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, double min,
      double max) {
    MinMaxRangePair minMaxRangePair = aggregationResultHolder.getResult();
    if (minMaxRangePair == null) {
      aggregationResultHolder.setValue(new MinMaxRangePair(min, max));
    } else {
      minMaxRangePair.apply(min, max);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = valueArray[i];
          setGroupByResult(groupKeyArray[i], groupByResultHolder, value, value);
        }
        break;
      case BYTES:
        // Serialized MinMaxRangePair
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          MinMaxRangePair value = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
          setGroupByResult(groupKeyArray[i], groupByResultHolder, value.getMin(), value.getMax());
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for MIN_MAX_RANGE aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value, value);
          }
        }
        break;
      case BYTES:
        // Serialized MinMaxRangePair
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          MinMaxRangePair value = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value.getMin(), value.getMax());
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for MIN_MAX_RANGE aggregation function: " + valueType);
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, double min,
      double max) {
    MinMaxRangePair minMaxRangePair = groupByResultHolder.getResult(groupKey);
    if (minMaxRangePair == null) {
      groupByResultHolder.setValueForKey(groupKey, new MinMaxRangePair(min, max));
    } else {
      minMaxRangePair.apply(min, max);
    }
  }

  @Nonnull
  @Override
  public MinMaxRangePair extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    MinMaxRangePair minMaxRangePair = aggregationResultHolder.getResult();
    if (minMaxRangePair == null) {
      return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return minMaxRangePair;
    }
  }

  @Nonnull
  @Override
  public MinMaxRangePair extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    MinMaxRangePair minMaxRangePair = groupByResultHolder.getResult(groupKey);
    if (minMaxRangePair == null) {
      return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return minMaxRangePair;
    }
  }

  @Nonnull
  @Override
  public MinMaxRangePair merge(@Nonnull MinMaxRangePair intermediateResult1,
      @Nonnull MinMaxRangePair intermediateResult2) {
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Nonnull
  @Override
  public Double extractFinalResult(@Nonnull MinMaxRangePair intermediateResult) {
    return intermediateResult.getMax() - intermediateResult.getMin();
  }
}
