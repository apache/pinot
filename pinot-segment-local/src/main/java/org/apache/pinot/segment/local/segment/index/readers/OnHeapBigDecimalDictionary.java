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

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Implementation of BIG_DECIMAL dictionary that cache all values on-heap.
 * <p>This is useful for BIG_DECIMAL columns that:
 * <ul>
 *   <li>Has low cardinality BIG_DECIMAL dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 */
public class OnHeapBigDecimalDictionary extends BaseImmutableDictionary {
  // NOTE: Always do binary search because BigDecimal's compareTo() is not consistent with equals()
  //       E.g. compareTo(3.0, 3) returns 0 but equals(3.0, 3) returns false
  private final BigDecimal[] _dictIdToVal;

  public OnHeapBigDecimalDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(dataBuffer, length, numBytesPerValue, (byte) 0);

    _dictIdToVal = new BigDecimal[length];
    for (int dictId = 0; dictId < length; dictId++) {
      _dictIdToVal[dictId] = getBigDecimal(dictId);
    }
  }

  @Override
  public DataType getValueType() {
    return DataType.BIG_DECIMAL;
  }

  @Override
  public int indexOf(BigDecimal bigDecimalValue) {
    return normalizeIndex(Arrays.binarySearch(_dictIdToVal, bigDecimalValue));
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return Arrays.binarySearch(_dictIdToVal, new BigDecimal(stringValue));
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
  public BigDecimal getBigDecimalValue(int dictId) {
    return _dictIdToVal[dictId];
  }

  @Override
  public String getStringValue(int dictId) {
    return _dictIdToVal[dictId].toPlainString();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BigDecimalUtils.serialize(_dictIdToVal[dictId]);
  }
}
