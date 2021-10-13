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

import java.util.Arrays;
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
import org.apache.pinot.segment.local.customobject.LastWithTimePair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LastWithTimeAggregationFunction extends BaseSingleInputAggregationFunction<LastWithTimePair, Double> {
  private final ExpressionContext _timeCol;

  public LastWithTimeAggregationFunction(ExpressionContext dataCol, ExpressionContext timeCol) {
    super(dataCol);
    _timeCol = timeCol;
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return Arrays.asList(_expression, _timeCol);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.LASTWITHTIME;
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
    double lastData = Double.POSITIVE_INFINITY;
    long lastTime = Long.MIN_VALUE;

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    BlockValSet blockTimeSet = blockValSetMap.get(_timeCol);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      long[] timeValues = blockTimeSet.getLongValuesSV();
      for (int i = 0; i < length; i++) {
        double data = doubleValues[i];
        long time = timeValues[i];
        if (time >= lastTime) {
          lastTime = time;
          lastData = data;
        }
      }
    } else {
      // Serialized LastPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        LastWithTimePair lastWithTimePair = ObjectSerDeUtils.LAST_WITH_TIME_PAIR_SER_DE.deserialize(bytesValues[i]);
        double data = lastWithTimePair.getData();
        long time = lastWithTimePair.getTime();
        if (time >= lastTime) {
          lastTime = time;
          lastData = data;
        }
      }
    }
    setAggregationResult(aggregationResultHolder, lastData, lastTime);
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, double data, long time) {
    LastWithTimePair lastWithTimePair = aggregationResultHolder.getResult();
    if (lastWithTimePair == null) {
      aggregationResultHolder.setValue(new LastWithTimePair(data, time));
    } else {
      lastWithTimePair.apply(data, time);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    BlockValSet timeValSet = blockValSetMap.get(_timeCol);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      long[] timeValues = timeValSet.getLongValuesSV();
      for (int i = 0; i < length; i++) {
        double data = doubleValues[i];
        long time = timeValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, data, time);
      }
    } else {
      // Serialized LastPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        LastWithTimePair lastWithTimePair = ObjectSerDeUtils.LAST_WITH_TIME_PAIR_SER_DE.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i], groupByResultHolder, lastWithTimePair.getData(), lastWithTimePair.getTime());
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    BlockValSet timeValSet = blockValSetMap.get(_timeCol);
    if (blockValSet.getValueType() != DataType.BYTES) {
      double[] doubleValues = blockValSet.getDoubleValuesSV();
      long[] timeValues = timeValSet.getLongValuesSV();
      for (int i = 0; i < length; i++) {
        double value = doubleValues[i];
        long time = timeValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, time);
        }
      }
    } else {
      // Serialized LastWithTimePair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        LastWithTimePair lastWithTimePair = ObjectSerDeUtils.LAST_WITH_TIME_PAIR_SER_DE.deserialize(bytesValues[i]);
        double data = lastWithTimePair.getData();
        long time = lastWithTimePair.getTime();
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, data, time);
        }
      }
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, double data, long time) {
    LastWithTimePair lastWithTimePair = groupByResultHolder.getResult(groupKey);
    if (lastWithTimePair == null) {
      groupByResultHolder.setValueForKey(groupKey, new LastWithTimePair(data, time));
    } else {
      lastWithTimePair.apply(data, time);
    }
  }

  @Override
  public LastWithTimePair extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    LastWithTimePair lastWithTimePair = aggregationResultHolder.getResult();
    if (lastWithTimePair == null) {
      return new LastWithTimePair(Double.POSITIVE_INFINITY, Long.MIN_VALUE);
    } else {
      return lastWithTimePair;
    }
  }

  @Override
  public LastWithTimePair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    LastWithTimePair lastWithTimePair = groupByResultHolder.getResult(groupKey);
    if (lastWithTimePair == null) {
      return new LastWithTimePair(Double.POSITIVE_INFINITY, Long.MIN_VALUE);
    } else {
      return lastWithTimePair;
    }
  }

  @Override
  public LastWithTimePair merge(LastWithTimePair intermediateResult1, LastWithTimePair intermediateResult2) {
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
  public Double extractFinalResult(LastWithTimePair intermediateResult) {
    return intermediateResult.getData();
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + "," + _timeCol + ")";
  }
}
