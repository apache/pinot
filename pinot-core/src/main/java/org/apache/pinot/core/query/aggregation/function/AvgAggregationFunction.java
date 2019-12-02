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

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;


public class AvgAggregationFunction implements AggregationFunction<AvgPair, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVG;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.AVG.getName() + "_" + column;
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
    if (blockValSets[0].getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSets[0].getDoubleValuesSV();
      double sum = 0.0;
      for (int i = 0; i < length; i++) {
        sum += doubleValues[i];
      }
      setAggregationResult(aggregationResultHolder, sum, (long) length);
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
      double sum = 0.0;
      long count = 0L;
      for (int i = 0; i < length; i++) {
        AvgPair value = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        sum += value.getSum();
        count += value.getCount();
      }
      setAggregationResult(aggregationResultHolder, sum, count);
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double sum, long count) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      aggregationResultHolder.setValue(new AvgPair(sum, count));
    } else {
      avgPair.apply(sum, count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    if (blockValSets[0].getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSets[0].getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        setGroupByResult(groupKeyArray[i], groupByResultHolder, doubleValues[i], 1L);
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair.getSum(), avgPair.getCount());
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
          setGroupByResult(groupKey, groupByResultHolder, value, 1L);
        }
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        double sum = avgPair.getSum();
        long count = avgPair.getCount();
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, sum, count);
        }
      }
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double sum, long count) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      groupByResultHolder.setValueForKey(groupKey, new AvgPair(sum, count));
    } else {
      avgPair.apply(sum, count);
    }
  }

  @Override
  public AvgPair extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      return new AvgPair(0.0, 0L);
    } else {
      return avgPair;
    }
  }

  @Override
  public AvgPair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      return new AvgPair(0.0, 0L);
    } else {
      return avgPair;
    }
  }

  @Override
  public AvgPair merge(AvgPair intermediateResult1, AvgPair intermediateResult2) {
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
  public Double extractFinalResult(AvgPair intermediateResult) {
    long count = intermediateResult.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      return intermediateResult.getSum() / count;
    }
  }
}
