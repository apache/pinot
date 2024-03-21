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

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.DoubleLongPair;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.roaringbitmap.IntIterator;


/**
 * This function is used for LastWithTime calculations for data column with double type.
 * <p>The function can be used as LastWithTime(dataExpression, timeExpression, 'double')
 * <p>Following arguments are supported:
 * <ul>
 *   <li>dataExpression: expression that contains the double data column to be calculated last on</li>
 *   <li>timeExpression: expression that contains the column to be used to decide which data is last, can be any
 *   Numeric column</li>
 * </ul>
 */
public class LastDoubleValueWithTimeAggregationFunction extends LastWithTimeAggregationFunction<Double> {
  private final static ValueLongPair<Double> DEFAULT_VALUE_TIME_PAIR = new DoubleLongPair(Double.NaN, Long.MIN_VALUE);

  public LastDoubleValueWithTimeAggregationFunction(ExpressionContext dataCol, ExpressionContext timeCol,
      boolean nullHandlingEnabled) {
    super(dataCol, timeCol, ObjectSerDeUtils.DOUBLE_LONG_PAIR_SER_DE, nullHandlingEnabled);
  }

  @Override
  public ValueLongPair<Double> constructValueLongPair(Double value, long time) {
    return new DoubleLongPair(value, time);
  }

  @Override
  public ValueLongPair<Double> getDefaultValueTimePair() {
    return DEFAULT_VALUE_TIME_PAIR;
  }

  @Override
  public Double readCell(BlockValSet block, int docId) {
    return block.getDoubleValuesSV()[docId];
  }

  @Override
  public void aggregateGroupResultWithRawDataSv(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    double[] doubleValues = blockValSet.getDoubleValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    IntIterator nullIdxIterator = orNullIterator(blockValSet, timeValSet);
    forEachNotNull(length, nullIdxIterator, (from, to) -> {
      for (int i = from; i < to; i++) {
        double data = doubleValues[i];
        long time = timeValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, data, time);
      }
    });
  }

  @Override
  public void aggregateGroupResultWithRawDataMv(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    double[] doubleValues = blockValSet.getDoubleValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    IntIterator nullIdxIterator = orNullIterator(blockValSet, timeValSet);
    forEachNotNull(length, nullIdxIterator, (from, to) -> {
      for (int i = from; i < to; i++) {
        double value = doubleValues[i];
        long time = timeValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, time);
        }
      }
    });
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + "," + _timeCol + ",'DOUBLE')";
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }
}
