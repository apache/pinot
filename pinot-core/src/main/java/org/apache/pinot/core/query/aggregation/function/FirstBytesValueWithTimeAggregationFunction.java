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

import com.google.common.base.Preconditions;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.BytesLongPair;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.apache.pinot.spi.utils.ByteArray;


/// This function is used for FirstWithTime calculations for data column with bytes or UUID type.
///
/// The function can be used as FirstWithTime(dataExpression, timeExpression, 'bytes')
///
/// Following arguments are supported:
///
/// - dataExpression: expression that contains the bytes or UUID data column to be calculated first on
/// - timeExpression: expression that contains the column to be used to decide which data is first, can be any
///   Numeric column
public class FirstBytesValueWithTimeAggregationFunction extends FirstWithTimeAggregationFunction<ByteArray> {
  private final ColumnDataType _resultType;
  private final ValueLongPair<ByteArray> _defaultValueTimePair;

  /// The result type is BYTES or UUID; both are stored as bytes.
  public FirstBytesValueWithTimeAggregationFunction(ExpressionContext dataCol, ExpressionContext timeCol,
      boolean nullHandlingEnabled, ColumnDataType resultType) {
    super(dataCol, timeCol, ObjectSerDeUtils.BYTES_LONG_PAIR_SER_DE, nullHandlingEnabled);
    Preconditions.checkArgument(resultType == ColumnDataType.BYTES || resultType == ColumnDataType.UUID,
        "Unsupported result type: %s", resultType);
    _resultType = resultType;
    // An empty value is not a valid UUID, so the default value is the result type's null placeholder (the nil UUID).
    _defaultValueTimePair = new BytesLongPair((ByteArray) resultType.getNullPlaceholder(), Long.MAX_VALUE);
  }

  @Override
  public ValueLongPair<ByteArray> constructValueLongPair(ByteArray value, long time) {
    return new BytesLongPair(value, time);
  }

  @Override
  public ValueLongPair<ByteArray> getDefaultValueTimePair() {
    return _defaultValueTimePair;
  }

  /// A BYTES data column holds values, not serialized pairs.
  @Override
  protected boolean isSerializedPairInput(BlockValSet blockValSet) {
    return false;
  }

  @Override
  public ByteArray readCell(BlockValSet block, int docId) {
    return new ByteArray(block.getBytesValuesSV()[docId]);
  }

  @Override
  public void aggregateGroupResultWithRawDataSv(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    forEachNotNull(length, blockValSet, timeValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        ByteArray data = new ByteArray(bytesValues[i]);
        long time = timeValues[i];
        setGroupByResult(groupKeyArray[i], groupByResultHolder, data, time);
      }
    });
  }

  @Override
  public void aggregateGroupResultWithRawDataMv(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, BlockValSet timeValSet) {
    byte[][] bytesValues = blockValSet.getBytesValuesSV();
    long[] timeValues = timeValSet.getLongValuesSV();

    forEachNotNull(length, blockValSet, timeValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        ByteArray value = new ByteArray(bytesValues[i]);
        long time = timeValues[i];
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupKey, groupByResultHolder, value, time);
        }
      }
    });
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expression + "," + _timeCol + ",'" + _resultType + "')";
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ValueLongPair<ByteArray> bytesLongPair) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.BytesLongPair.getValue(),
        ObjectSerDeUtils.BYTES_LONG_PAIR_SER_DE.serialize((BytesLongPair) bytesLongPair));
  }

  @Override
  public ValueLongPair<ByteArray> deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.BYTES_LONG_PAIR_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return _resultType;
  }
}
