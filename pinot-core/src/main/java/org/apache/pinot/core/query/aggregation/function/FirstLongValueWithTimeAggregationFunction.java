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
import org.apache.pinot.segment.local.customobject.LongLongPair;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.roaringbitmap.IntIterator;


/**
 * This function is used for FirstWithTime calculations for data column with long type.
 * <p>The function can be used as FirstWithTime(dataExpression, timeExpression, 'long')
 * <p>Following arguments are supported:
 * <ul>
 *   <li>dataExpression: expression that contains the long data column to be calculated first on</li>
 *   <li>timeExpression: expression that contains the column to be used to decide which data is first, can be any
 *   Numeric column</li>
 * </ul>
 */
public class FirstLongValueWithTimeAggregationFunction extends FirstWithTimeAggregationFunction<Long> {
  private final static ValueLongPair<Long> DEFAULT_VALUE_TIME_PAIR = new LongLongPair(Long.MIN_VALUE, Long.MAX_VALUE);

  public FirstLongValueWithTimeAggregationFunction(ExpressionContext dataCol, ExpressionContext timeCol,
      boolean nullHandlingEnabled) {
    super(dataCol, timeCol, ObjectSerDeUtils.LONG_LONG_PAIR_SER_DE, nullHandlingEnabled);
  }

  @Override
  public ValueLongPair<Long> constructValueLongPair(Long value, long time) {
    return new LongLongPair(value, time);
  }

  @Override
  public ValueLongPair<Long> getDefaultValueTimePair() {
    return DEFAULT_VALUE_TIME_PAIR;
  }

  @Override
  public Long readCell(BlockValSet block, int docId) {
    return block.getLongValuesSV()[docId];
  }

  @Override
  public void aggregateGroupResultWithRawDataSv(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    long[] longValues = blockValSet.getLongValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    IntIterator nullIdxIterator = orNullIterator(blockValSet, timeValSet);
    forEachNotNull(length, nullIdxIterator, (from, to) -> {
      for (int i = from; i < to; i++) {
        long data = longValues[i];
        long time = timeValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, data, time);
      }
    });
  }

  @Override
  public void aggregateGroupResultWithRawDataMv(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    long[] longValues = blockValSet.getLongValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    IntIterator nullIdxIterator = orNullIterator(blockValSet, timeValSet);
    forEachNotNull(length, nullIdxIterator, (from, to) -> {
      for (int i = from; i < to; i++) {
        long value = longValues[i];
        long time = timeValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, time);
        }
      }
    });
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + "," + _timeCol + ",'LONG')";
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }
}
