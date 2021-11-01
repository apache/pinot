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


/**
 * Implementation of BigDecimal dictionary that cache all values on-heap.
 * <p>This is useful for BigDecimal columns that:
 * <ul>
 *   <li>Has low cardinality string dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 * <p>This helps avoid creation of String from byte[], which is expensive as well as creates garbage.
 */
public class OnHeapBigDecimalDictionary extends OnHeapDictionary {
  private final Object2IntOpenHashMap<BigDecimal> _valToDictId;
  private final BigDecimal[] _dictIdToVal;

  public OnHeapBigDecimalDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(dataBuffer, length, numBytesPerValue, (byte) 0);

    byte[] buffer = new byte[numBytesPerValue];
    _dictIdToVal = new BigDecimal[length];
    _valToDictId = new Object2IntOpenHashMap<>(length);
    _valToDictId.defaultReturnValue(Dictionary.NULL_VALUE_INDEX);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    BigDecimal bigDecimalValue = BigDecimalUtils.toBigDecimal(stringValue);
    int index = _valToDictId.getInt(bigDecimalValue);
    return (index != Dictionary.NULL_VALUE_INDEX) ? index : Arrays.binarySearch(_dictIdToVal, bigDecimalValue);
  }

  @Override
  public DataType getValueType() {
    return DataType.BIGDECIMAL;
  }

  @Override
  public int indexOf(String stringValue) {
    return _valToDictId.getInt(BigDecimalUtils.toBigDecimal(stringValue));
  }

  @Override
  public BigDecimal get(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public int getIntValue(int dictId) {
    return _dictIdToVal[dictId].intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return _dictIdToVal[dictId].longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return _dictIdToVal[dictId].floatValue();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _dictIdToVal[dictId].doubleValue();
  }

  @Override
  public String getStringValue(int dictId) {
    return _dictIdToVal[dictId].toString();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BigDecimalUtils.serialize(_dictIdToVal[dictId]);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return _dictIdToVal[dictId];
  }
}
