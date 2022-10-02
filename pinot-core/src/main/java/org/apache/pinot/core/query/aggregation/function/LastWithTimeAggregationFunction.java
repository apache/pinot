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
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * This function is used for LastWithTime calculations.
 * <p>The function can be used as LastWithTime(dataExpression, timeExpression, 'dataType')
 * <p>Following arguments are supported:
 * <ul>
 *   <li>dataExpression: expression that contains the column to be calculated last on</li>
 *   <li>timeExpression: expression that contains the column to be used to decide which data is last, can be any
 *   Numeric column</li>
 *   <li>dataType: the data type of data column</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class LastWithTimeAggregationFunction<V extends Comparable<V>>
    extends BaseSingleInputAggregationFunction<ValueLongPair<V>, V> {
  protected final ExpressionContext _timeCol;
  private final ObjectSerDeUtils.ObjectSerDe<? extends ValueLongPair<V>> _objectSerDe;

  public LastWithTimeAggregationFunction(ExpressionContext dataCol,
      ExpressionContext timeCol,
      ObjectSerDeUtils.ObjectSerDe<? extends ValueLongPair<V>> objectSerDe) {
    super(dataCol);
    _timeCol = timeCol;
    _objectSerDe = objectSerDe;
  }

  public abstract ValueLongPair<V> constructValueLongPair(V value, long time);

  public abstract ValueLongPair<V> getDefaultValueTimePair();

  public abstract void aggregateResultWithRawData(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, BlockValSet timeValSet);

  public abstract void aggregateGroupResultWithRawDataSv(int length,
      int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet,
      BlockValSet timeValSet);

  public abstract void aggregateGroupResultWithRawDataMv(int length,
      int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet,
      BlockValSet timeValSet);

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

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    BlockValSet blockTimeSet = blockValSetMap.get(_timeCol);
    if (blockValSet.getValueType() != DataType.BYTES) {
      aggregateResultWithRawData(length, aggregationResultHolder, blockValSet, blockTimeSet);
    } else {
      ValueLongPair<V> defaultValueLongPair = getDefaultValueTimePair();
      V lastData = defaultValueLongPair.getValue();
      long lastTime = defaultValueLongPair.getTime();
      // Serialized LastPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        ValueLongPair<V> lastWithTimePair = _objectSerDe.deserialize(bytesValues[i]);
        V data = lastWithTimePair.getValue();
        long time = lastWithTimePair.getTime();
        if (time >= lastTime) {
          lastTime = time;
          lastData = data;
        }
      }
      setAggregationResult(aggregationResultHolder, lastData, lastTime);
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, V data, long time) {
    ValueLongPair lastWithTimePair = aggregationResultHolder.getResult();
    if (lastWithTimePair == null || time >= lastWithTimePair.getTime()) {
      aggregationResultHolder.setValue(constructValueLongPair(data, time));
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    BlockValSet timeValSet = blockValSetMap.get(_timeCol);
    if (blockValSet.getValueType() != DataType.BYTES) {
      aggregateGroupResultWithRawDataSv(length, groupKeyArray, groupByResultHolder,
          blockValSet, timeValSet);
    } else {
      // Serialized LastPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        ValueLongPair<V> lastWithTimePair = _objectSerDe.deserialize(bytesValues[i]);
        setGroupByResult(groupKeyArray[i],
            groupByResultHolder,
            lastWithTimePair.getValue(),
            lastWithTimePair.getTime());
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    BlockValSet timeValSet = blockValSetMap.get(_timeCol);
    if (blockValSet.getValueType() != DataType.BYTES) {
      aggregateGroupResultWithRawDataMv(length, groupKeysArray, groupByResultHolder, blockValSet, timeValSet);
    } else {
      // Serialized ValueTimePair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        ValueLongPair<V> lastWithTimePair = _objectSerDe.deserialize(bytesValues[i]);
        V data = lastWithTimePair.getValue();
        long time = lastWithTimePair.getTime();
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, data, time);
        }
      }
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, V data, long time) {
    ValueLongPair lastWithTimePair = groupByResultHolder.getResult(groupKey);
    if (lastWithTimePair == null || time >= lastWithTimePair.getTime()) {
      groupByResultHolder.setValueForKey(groupKey, constructValueLongPair(data, time));
    }
  }

  @Override
  public ValueLongPair<V> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    ValueLongPair lastWithTimePair = aggregationResultHolder.getResult();
    if (lastWithTimePair == null) {
      return getDefaultValueTimePair();
    } else {
      return lastWithTimePair;
    }
  }

  @Override
  public ValueLongPair<V> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    ValueLongPair<V> lastWithTimePair = groupByResultHolder.getResult(groupKey);
    if (lastWithTimePair == null) {
      return getDefaultValueTimePair();
    } else {
      return lastWithTimePair;
    }
  }

  @Override
  public ValueLongPair<V> merge(ValueLongPair<V> intermediateResult1, ValueLongPair<V> intermediateResult2) {
    if (intermediateResult1.getTime() >= intermediateResult2.getTime()) {
      return intermediateResult1;
    } else {
      return intermediateResult2;
    }
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return Arrays.asList(_expression, _timeCol);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public V extractFinalResult(ValueLongPair<V> intermediateResult) {
    return intermediateResult.getValue();
  }
}
