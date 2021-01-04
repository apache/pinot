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


public class FloatDictionary extends BaseImmutableDictionary {

  public FloatDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Float.BYTES, (byte) 0);
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    // First convert string to Double and then downcast to int. This allows type conversion from any compatible
    // numerical value represented as string to an int value.
    Double doubleValue = Double.valueOf(stringValue);

    // A value greater than Float.MAX_VALUE will downcast to Float.MAX_VALUE and a value less than Float.MIN_VALUE
    // will downcast to Float.MIN_VALUE. This can cause binary search to return a match if the column actually contains
    // Float.MIN_VALUE or Float.MAX_VALUE. We avoid this error by explicitly checking for overflow and underflow.
    if (doubleValue > Float.MAX_VALUE) {
      // Binary search insert position of value greater than Float.MAX_VALUE
      return -(length()+1);
    }

    if (doubleValue < Float.MIN_VALUE) {
      // Binary search Insert position of value greater less than Float.MIN_VALUE
      return -1;
    }

    return binarySearch(Double.valueOf(stringValue).floatValue());
  }

  @Override
  public DataType getValueType() {
    return DataType.FLOAT;
  }

  @Override
  public Float get(int dictId) {
    return getFloat(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) getFloat(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return (long) getFloat(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return getFloat(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getFloat(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Float.toString(getFloat(dictId));
  }
}
