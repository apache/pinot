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
import com.linkedin.pinot.core.query.aggregation.function.customobject.AvgPair;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import javax.annotation.Nonnull;


public class AvgAggregationFunction implements AggregationFunction<AvgPair, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVG;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.AVG.getName() + "_" + column;
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
        double sum = 0.0;
        for (int i = 0; i < length; i++) {
          sum += valueArray[i];
        }
        setAggregationResult(aggregationResultHolder, sum, (long) length);
        break;
      case BYTES:
        // Serialized AvgPair
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        sum = 0.0;
        long count = 0L;
        for (int i = 0; i < length; i++) {
          AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
          sum += value.getSum();
          count += value.getCount();
        }
        setAggregationResult(aggregationResultHolder, sum, count);
        break;
      default:
        throw new IllegalStateException("Illegal data type for AVG aggregation function: " + valueType);
    }
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, double sum,
      long count) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      aggregationResultHolder.setValue(new AvgPair(sum, count));
    } else {
      avgPair.apply(sum, count);
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
          setGroupByResult(groupKeyArray[i], groupByResultHolder, valueArray[i], 1L);
        }
        break;
      case BYTES:
        // Serialized AvgPair
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
          setGroupByResult(groupKeyArray[i], groupByResultHolder, value.getSum(), value.getCount());
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for AVG aggregation function: " + valueType);
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
            setGroupByResult(groupKey, groupByResultHolder, value, 1L);
          }
        }
        break;
      case BYTES:
        // Serialized AvgPair
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
          double sum = value.getSum();
          long count = value.getCount();
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, sum, count);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for AVG aggregation function: " + valueType);
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, double sum,
      long count) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      groupByResultHolder.setValueForKey(groupKey, new AvgPair(sum, count));
    } else {
      avgPair.apply(sum, count);
    }
  }

  @Nonnull
  @Override
  public AvgPair extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      return new AvgPair(0.0, 0L);
    } else {
      return avgPair;
    }
  }

  @Nonnull
  @Override
  public AvgPair extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      return new AvgPair(0.0, 0L);
    } else {
      return avgPair;
    }
  }

  @Nonnull
  @Override
  public AvgPair merge(@Nonnull AvgPair intermediateResult1, @Nonnull AvgPair intermediateResult2) {
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
  public Double extractFinalResult(@Nonnull AvgPair intermediateResult) {
    long count = intermediateResult.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      return intermediateResult.getSum() / count;
    }
  }
}
