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
import org.apache.pinot.spi.data.readers.Vector;


/**
 * Implementation of BIG_DECIMAL dictionary that cache all values on-heap.
 * <p>This is useful for BIG_DECIMAL columns that:
 * <ul>
 *   <li>Has low cardinality BIG_DECIMAL dictionary where memory footprint on-heap is acceptably small</li>
 *   <li>Is heavily queried</li>
 * </ul>
 */
public class OnHeapVectorDictionary extends BaseImmutableDictionary {
  // NOTE: Always do binary search because BigDecimal's compareTo() is not consistent with equals()
  //       E.g. compareTo(3.0, 3) returns 0 but equals(3.0, 3) returns false
  private final Vector[] _dictIdToVal;

  public OnHeapVectorDictionary(PinotDataBuffer dataBuffer, int length, int numBytesPerValue) {
    super(dataBuffer, length, numBytesPerValue);

    _dictIdToVal = new Vector[length];
    for (int dictId = 0; dictId < length; dictId++) {
      _dictIdToVal[dictId] = getVector(dictId);
    }
  }

  @Override
  public DataType getValueType() {
    return DataType.VECTOR;
  }

  @Override
  public int indexOf(Vector vectorValue) {
    return normalizeIndex(Arrays.binarySearch(_dictIdToVal, vectorValue));
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    return Arrays.binarySearch(_dictIdToVal, Vector.fromString(stringValue));
  }

  @Override
  public Vector get(int dictId) {
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
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int dictId) {
    return _dictIdToVal[dictId].toString();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _dictIdToVal[dictId].toBytes();
  }
}
