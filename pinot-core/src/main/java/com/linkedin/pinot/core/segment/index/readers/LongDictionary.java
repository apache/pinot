/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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


public class LongDictionary extends ImmutableDictionaryReader {

  public LongDictionary(PinotDataBuffer dataBuffer, int length) {
    super(dataBuffer, length, Long.BYTES, (byte) 0);
  }

  @Override
  public int indexOf(Object rawValue) {
    int index = insertionIndexOf(rawValue);
    return (index >= 0) ? index : -1;
  }

  @Override
  public int insertionIndexOf(Object rawValue) {
    long value;
    if (rawValue instanceof String) {
      value = Long.parseLong((String) rawValue);
    } else {
      value = (Long) rawValue;
    }
    return binarySearch(value);
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
