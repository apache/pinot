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

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Implementation of double dictionary that cache all values on-heap.
 * <p>This is useful for double columns that:
 * <ul>
 *   <li>Have low cardinality double dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of double from byte[].
 */
public class OnHeapDoubleDictionary extends OnHeapDictionary {
  private final Double2IntOpenHashMap _valToDictId;
  private final double[] _dictIdToVal;

  /**
   * Constructor for the class.
   * Populates the value <-> mappings.
   *
   * @param dataBuffer Pinot data buffer
   * @param length Length of the dictionary
   */
  public OnHeapDoubleDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Double.BYTES, (byte) 0);

    _valToDictId = new Double2IntOpenHashMap(length);
    _valToDictId.defaultReturnValue(Dictionary.NULL_VALUE_INDEX);
    _dictIdToVal = new double[length];

    for (int dictId = 0; dictId < length; dictId++) {
      double value = getDouble(dictId);
      _dictIdToVal[dictId] = value;
      _valToDictId.put(value, dictId);
    }
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    double doubleValue = Double.parseDouble(stringValue);
    int index = _valToDictId.get(doubleValue);
    return (index != Dictionary.NULL_VALUE_INDEX) ? index : Arrays.binarySearch(_dictIdToVal, doubleValue);
  }

  @Override
  public DataType getValueType() {
    return DataType.DOUBLE;
  }

  @Override
  public int indexOf(String stringValue) {
    return _valToDictId.get(Double.parseDouble(stringValue));
  }

  @Override
  public Double get(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) _dictIdToVal[dictId];
  }

  @Override
  public long getLongValue(int dictId) {
    return (long) _dictIdToVal[dictId];
  }

  @Override
  public float getFloatValue(int dictId) {
    return (float) _dictIdToVal[dictId];
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public String getStringValue(int dictId) {
    return Double.toString(_dictIdToVal[dictId]);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.valueOf(_dictIdToVal[dictId]);
  }
}
