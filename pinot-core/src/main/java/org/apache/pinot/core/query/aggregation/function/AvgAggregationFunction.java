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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class AvgAggregationFunction extends BaseSingleInputAggregationFunction<AvgPair, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  public AvgAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVG;
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

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      double sum = 0.0;
      for (int i = 0; i < length; i++) {
        sum += doubleValues[i];
      }
      setAggregationResult(aggregationResultHolder, sum, length);
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
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
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      for (int i = 0; i < length; i++) {
        setGroupByResult(groupKeyArray[i], groupByResultHolder, doubleValues[i], 1L);
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        AvgPair avgPair = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair.getSum(), avgPair.getCount());
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
          setGroupByResult(groupKey, groupByResultHolder, value, 1L);
        }
      }
    } else {
      // Serialized AvgPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
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
