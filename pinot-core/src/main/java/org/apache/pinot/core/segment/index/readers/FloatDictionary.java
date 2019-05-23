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

import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class FloatDictionary extends ImmutableDictionaryReader {

  public FloatDictionary(PinotDataBuffer dataBuffer, int length) {
    super(new FixedByteValueReaderWriter(dataBuffer), length, Float.BYTES, (byte) 0);
  }

  @Override
  public int indexOf(Object rawValue) {
    int index = insertionIndexOf(rawValue);
    return (index >= 0) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    float value;
    if (rawValue instanceof String) {
      value = Float.parseFloat((String) rawValue);
    } else {
      value = (Float) rawValue;
    }
    return binarySearch(value);
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
