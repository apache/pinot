/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


public class IntDictionary extends ImmutableDictionaryReader {
  private static final int INT_SIZE_IN_BYTES = Integer.SIZE / Byte.SIZE;

  public IntDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, INT_SIZE_IN_BYTES, (byte) 0);
  }

  @Override
  public int indexOf(Object rawValue) {
    int index = insertionIndexOf(rawValue);
    return (index >= 0) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    int value;
    if (rawValue instanceof String) {
      value = Integer.parseInt((String) rawValue);
    } else {
      value = (Integer) rawValue;
    }
    return binarySearch(value);
  }

  @Override
  public Integer get(int dictId) {
    return getInt(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getInt(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Integer.toString(getInt(dictId));
  }
}
