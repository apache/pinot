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

import java.math.BigDecimal;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.BigDecimalLongPair;
import org.apache.pinot.segment.local.customobject.ValueLongPair;


/// This function is used for LastWithTime calculations for data column with big decimal type.
///
/// The function can be used as LastWithTime(dataExpression, timeExpression, 'big_decimal')
///
/// Following arguments are supported:
///
/// - dataExpression: expression that contains the big decimal data column to be calculated last on
/// - timeExpression: expression that contains the column to be used to decide which data is last, can be any
///   Numeric column
public class LastBigDecimalValueWithTimeAggregationFunction extends LastWithTimeAggregationFunction<BigDecimal> {
  private final static ValueLongPair<BigDecimal> DEFAULT_VALUE_TIME_PAIR =
      new BigDecimalLongPair(BigDecimal.ZERO, Long.MIN_VALUE);

  public LastBigDecimalValueWithTimeAggregationFunction(ExpressionContext dataCol, ExpressionContext timeCol,
      boolean nullHandlingEnabled) {
    super(dataCol, timeCol, ObjectSerDeUtils.BIG_DECIMAL_LONG_PAIR_SER_DE, nullHandlingEnabled);
  }

  @Override
  public ValueLongPair<BigDecimal> constructValueLongPair(BigDecimal value, long time) {
    return new BigDecimalLongPair(value, time);
  }

  @Override
  public ValueLongPair<BigDecimal> getDefaultValueTimePair() {
    return DEFAULT_VALUE_TIME_PAIR;
  }

  @Override
  public BigDecimal readCell(BlockValSet block, int docId) {
    return block.getBigDecimalValuesSV()[docId];
  }

  @Override
  public void aggregateGroupResultWithRawDataSv(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    forEachNotNull(length, blockValSet, timeValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        BigDecimal data = bigDecimalValues[i];
        long time = timeValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, data, time);
      }
    });
  }

  @Override
  public void aggregateGroupResultWithRawDataMv(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    forEachNotNull(length, blockValSet, timeValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        BigDecimal value = bigDecimalValues[i];
        long time = timeValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, time);
        }
      }
    });
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + "," + _timeCol + ",'BIG_DECIMAL')";
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ValueLongPair<BigDecimal> bigDecimalLongPair) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.BigDecimalLongPair.getValue(),
        ObjectSerDeUtils.BIG_DECIMAL_LONG_PAIR_SER_DE.serialize((BigDecimalLongPair) bigDecimalLongPair));
  }

  @Override
  public ValueLongPair<BigDecimal> deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.BIG_DECIMAL_LONG_PAIR_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.BIG_DECIMAL;
  }
}
