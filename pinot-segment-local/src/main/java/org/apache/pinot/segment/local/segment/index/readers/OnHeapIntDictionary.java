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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Implementation of int dictionary that cache all values on-heap.
 * <p>This is useful for int columns that:
 * <ul>
 *   <li>Have low cardinality int dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of int from byte[].
 */
public class OnHeapIntDictionary extends OnHeapDictionary {
  private final Int2IntOpenHashMap _valToDictId;
  private final int[] _dictIdToVal;

  /**
   * Constructor for the class.
   * Populates the value <-> mappings.
   *
   * @param dataBuffer Pinot data buffer
   * @param length Length of the dictionary
   */
  public OnHeapIntDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Integer.BYTES, (byte) 0);

    _valToDictId = new Int2IntOpenHashMap(length);
    _valToDictId.defaultReturnValue(Dictionary.NULL_VALUE_INDEX);
    _dictIdToVal = new int[length];

    for (int dictId = 0; dictId < length; dictId++) {
      int value = getInt(dictId);
      _dictIdToVal[dictId] = value;
      _valToDictId.put(value, dictId);
    }
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int intValue = Integer.parseInt(stringValue);
    int index = _valToDictId.get(intValue);
    return (index != Dictionary.NULL_VALUE_INDEX) ? index : Arrays.binarySearch(_dictIdToVal, intValue);
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public int indexOf(String stringValue) {
    return _valToDictId.get(Integer.parseInt(stringValue));
  }

  @Override
  public Integer get(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public int getIntValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public long getLongValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public float getFloatValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public String getStringValue(int dictId) {
    return Integer.toString(_dictIdToVal[dictId]);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.valueOf(_dictIdToVal[dictId]);
  }
}
