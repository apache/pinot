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
package org.apache.pinot.segment.local.segment.index.readers;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.math.BigDecimal;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.FALFInterner;


/**
 * Implementation of BYTES dictionary that cache all values on-heap.
 * <p>This is useful for BYTES columns that:
 * <ul>
 *   <li>Has low cardinality BYTES dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 */
public class OnHeapBytesDictionary extends BaseImmutableDictionary {
  private final Object2IntOpenHashMap<ByteArray> _valToDictId;
  private final ByteArray[] _dictIdToVal;

  public OnHeapBytesDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue,
      @Nullable FALFInterner<byte[]> byteInterner) {
    super(dataBuffer, length, numBytesPerValue);

    _valToDictId = new Object2IntOpenHashMap<>(length);
    _valToDictId.defaultReturnValue(Dictionary.NULL_VALUE_INDEX);
    _dictIdToVal = new ByteArray[length];

    for (int dictId = 0; dictId < length; dictId++) {
      ByteArray value = new ByteArray(getBytes(dictId), byteInterner);
      _dictIdToVal[dictId] = value;
      _valToDictId.put(value, dictId);
    }
  }

  @Override
  public DataType getValueType() {
    return DataType.BYTES;
  }

  @Override
  public int indexOf(String stringValue) {
    return _valToDictId.getInt(BytesUtils.toByteArray(stringValue));
  }

  @Override
  public int indexOf(ByteArray bytesValue) {
    return _valToDictId.getInt(bytesValue);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    ByteArray byteArray = BytesUtils.toByteArray(stringValue);
    int index = _valToDictId.getInt(byteArray);
    return (index != Dictionary.NULL_VALUE_INDEX) ? index : Arrays.binarySearch(_dictIdToVal, byteArray);
  }

  @Override
  public byte[] get(int dictId) {
    return _dictIdToVal[dictId].getBytes();
  }

  @Override
  public Object getInternal(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public int getIntValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.deserialize(_dictIdToVal[dictId].getBytes());
  }

  @Override
  public String getStringValue(int dictId) {
    return BytesUtils.toHexString(_dictIdToVal[dictId].getBytes());
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _dictIdToVal[dictId].getBytes();
  }
}
