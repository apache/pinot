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
package org.apache.pinot.core.segment.index.readers;

import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class LongDictionary extends BaseImmutableDictionary {

  public LongDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Long.BYTES, (byte) 0);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    // Convert string to Double and then downcast to long. This allows type conversion from any compatible
    // numerical value represented as string to a long value.
    Double doubleValue = Double.valueOf(stringValue);

    // A value greater than Long.MAX_VALUE will downcast to Long.MAX_VALUE and a value less than Long.MIN_VALUE will
    // downcast to Long.MIN_VALUE. This can cause binary search to return a match if the column actually contains
    // Long.MIN_VALUE or Long.MAX_VALUE. We avoid this error by explicitly checking for overflow and underflow.
    if (doubleValue > Long.MAX_VALUE) {
      // Binary search insert position of value greater than Long.MAX_VALUE
      return -(length()+1);
    }

    if (doubleValue < Long.MIN_VALUE) {
      // Binary search insert position of value less than Long.MIN_VALUE
      return -1;
    }

    return binarySearch(doubleValue.longValue());
  }

  @Override
  public DataType getValueType() {
    return DataType.LONG;
  }

  @Override
  public Long get(int dictId) {
    return getLong(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) getLong(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return getLong(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return getLong(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getLong(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Long.toString(getLong(dictId));
  }
}
