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
import org.apache.pinot.segment.local.customobject.IntLongPair;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.IntIterator;


/**
 * This function is used for FirstWithTime calculations.
 * <p>The function can be used as FirstWithTime(dataExpression, timeExpression, 'dataType')
 * <p>Following arguments are supported:
 * <ul>
 *   <li>dataExpression: expression that contains the column to be calculated first on</li>
 *   <li>timeExpression: expression that contains the column to be used to decide which data is first, can be any
 *   Numeric column</li>
 *   <li>dataType: the data type of data column</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class FirstWithTimeAggregationFunction<V extends Comparable<V>>
    extends NullableSingleInputAggregationFunction<ValueLongPair<V>, V> {
  protected final ExpressionContext _timeCol;
  private final ObjectSerDeUtils.ObjectSerDe<? extends ValueLongPair<V>> _objectSerDe;

  public FirstWithTimeAggregationFunction(ExpressionContext dataCol, ExpressionContext timeCol,
      ObjectSerDeUtils.ObjectSerDe<? extends ValueLongPair<V>> objectSerDe, boolean nullHandlingEnabled) {
    super(dataCol, nullHandlingEnabled);
    _timeCol = timeCol;
    _objectSerDe = objectSerDe;
  }

  public abstract ValueLongPair<V> constructValueLongPair(V value, long time);

  public abstract ValueLongPair<V> getDefaultValueTimePair();

  public abstract V readCell(BlockValSet block, int docId);

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
    return AggregationFunctionType.FIRSTWITHTIME;
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
      IntLongPair defaultPair = new IntLongPair(Integer.MIN_VALUE, Long.MAX_VALUE);
      long[] timeValues = blockTimeSet.getLongValuesSV();

      IntIterator nullIdxIterator = orNullIterator(blockValSet, blockTimeSet);
      IntLongPair bestPair = foldNotNull(length, nullIdxIterator, defaultPair, (pair, from, to) -> {
        IntLongPair actualPair = pair;
        for (int i = from; i < to; i++) {
          long time = timeValues[i];
          if (time <= actualPair.getTime()) {
            actualPair = new IntLongPair(i, time);
          }
        }
        return actualPair;
      });
      V bestValue;
      if (bestPair.getValue() < 0) {
        bestValue = getDefaultValueTimePair().getValue();
      } else {
        bestValue = readCell(blockValSet, bestPair.getValue());
      }
      setAggregationResult(aggregationResultHolder, bestValue, bestPair.getTime());
    } else {
      // We assume bytes contain the binary serialization of FirstPair
      ValueLongPair<V> defaultValueLongPair = getDefaultValueTimePair();

      ValueLongPair<V> result = constructValueLongPair(defaultValueLongPair.getValue(), defaultValueLongPair.getTime());
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          ValueLongPair<V> firstWithTimePair = _objectSerDe.deserialize(bytesValues[i]);
          long time = firstWithTimePair.getTime();
          if (time < result.getTime()) {
            result.setTime(time);
            result.setValue(firstWithTimePair.getValue());
          }
        }
      });

      setAggregationResult(aggregationResultHolder, result.getValue(), result.getTime());
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, V data, long time) {
    ValueLongPair firstWithTimePair = aggregationResultHolder.getResult();
    if (firstWithTimePair == null || time <= firstWithTimePair.getTime()) {
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
      // Serialized FirstPair
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          ValueLongPair<V> firstWithTimePair = _objectSerDe.deserialize(bytesValues[i]);
          setGroupByResult(groupKeyArray[i],
              groupByResultHolder,
              firstWithTimePair.getValue(),
              firstWithTimePair.getTime());
        }
      });
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
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          ValueLongPair<V> firstWithTimePair = _objectSerDe.deserialize(bytesValues[i]);
          V data = firstWithTimePair.getValue();
          long time = firstWithTimePair.getTime();
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, data, time);
          }
        }
      });
    }
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, V data, long time) {
    ValueLongPair firstWithTimePair = groupByResultHolder.getResult(groupKey);
    if (firstWithTimePair == null || time <= firstWithTimePair.getTime()) {
      groupByResultHolder.setValueForKey(groupKey, constructValueLongPair(data, time));
    }
  }

  @Override
  public ValueLongPair<V> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    ValueLongPair firstWithTimePair = aggregationResultHolder.getResult();
    if (firstWithTimePair == null) {
      return getDefaultValueTimePair();
    } else {
      return firstWithTimePair;
    }
  }

  @Override
  public ValueLongPair<V> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    ValueLongPair<V> firstWithTimePair = groupByResultHolder.getResult(groupKey);
    if (firstWithTimePair == null) {
      return getDefaultValueTimePair();
    } else {
      return firstWithTimePair;
    }
  }

  @Override
  public ValueLongPair<V> merge(ValueLongPair<V> intermediateResult1, ValueLongPair<V> intermediateResult2) {
    if (intermediateResult1.getTime() <= intermediateResult2.getTime()) {
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
