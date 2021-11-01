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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * Implementation of String dictionary that cache all values on-heap.
 * <p>This is useful for String columns that:
 * <ul>
 *   <li>Has low cardinality string dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of String from byte[], which is expensive as well as creates garbage.
 */
public class OnHeapStringDictionary extends OnHeapDictionary {
  private final byte _paddingByte;
  private final String[] _unpaddedStrings;
  private final Object2IntOpenHashMap<String> _unPaddedStringToIdMap;
  private final String[] _paddedStrings;

  public OnHeapStringDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue, byte paddingByte) {
    super(dataBuffer, length, numBytesPerValue, paddingByte);

    _paddingByte = paddingByte;
    byte[] buffer = new byte[numBytesPerValue];
    _unpaddedStrings = new String[length];
    _unPaddedStringToIdMap = new Object2IntOpenHashMap<>(length);
    _unPaddedStringToIdMap.defaultReturnValue(Dictionary.NULL_VALUE_INDEX);

    for (int i = 0; i < length; i++) {
      String unpaddedString = getUnpaddedString(i, buffer);
      _unpaddedStrings[i] = unpaddedString;
      _unPaddedStringToIdMap.put(unpaddedString, i);
    }

    if (paddingByte == 0) {
      _paddedStrings = null;
    } else {
      _paddedStrings = new String[length];
      for (int i = 0; i < length; i++) {
        _paddedStrings[i] = getPaddedString(i, buffer);
      }
    }
  }

  /**
   * WARNING: With non-zero padding byte, binary search result might not reflect the real insertion index for the value.
   * E.g. with padding byte 'b', if unpadded value "aa" is in the dictionary, and stored as "aab", then unpadded value
   * "a" will be mis-positioned after value "aa"; unpadded value "aab" will return positive value even if value "aab" is
   * not in the dictionary.
   * TODO: Clean up the segments with legacy non-zero padding byte, and remove the support for non-zero padding byte
   */
  @Override
  public int insertionIndexOf(String stringValue) {
    int index = _unPaddedStringToIdMap.getInt(stringValue);
    if (index != Dictionary.NULL_VALUE_INDEX) {
      return index;
    } else {
      return _paddingByte == 0 ? Arrays.binarySearch(_unpaddedStrings, stringValue)
          : Arrays.binarySearch(_paddedStrings, padString(stringValue));
    }
  }

  @Override
  public DataType getValueType() {
    return DataType.STRING;
  }

  @Override
  public int indexOf(String stringValue) {
    return _unPaddedStringToIdMap.getInt(stringValue);
  }

  @Override
  public String get(int dictId) {
    return _unpaddedStrings[dictId];
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(_unpaddedStrings[dictId]);
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(_unpaddedStrings[dictId]);
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(_unpaddedStrings[dictId]);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(_unpaddedStrings[dictId]);
  }

  @Override
  public String getStringValue(int dictId) {
    return _unpaddedStrings[dictId];
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BytesUtils.toBytes(_unpaddedStrings[dictId]);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.toBigDecimal(_unpaddedStrings[dictId]);
  }
}
