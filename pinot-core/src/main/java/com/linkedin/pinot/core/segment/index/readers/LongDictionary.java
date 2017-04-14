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

import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;

public class LongDictionary extends ImmutableDictionaryReader {

  public LongDictionary(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    super(dataBuffer, metadata.getCardinality(), Long.SIZE / 8);
  }

  @Override
  public int indexOf(Object rawValue) {
    Long lookup;
    if (rawValue instanceof String) {
      lookup = Long.parseLong((String) rawValue);
    } else {
      lookup = (Long) rawValue;
    }
    return longIndexOf(lookup);
  }

  @Override
  public Long get(int dictionaryId) {
    return getLong(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return getLong(dictionaryId);
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return getLong(dictionaryId);
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return Long.toString(getLong(dictionaryId));
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (float) getLong(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (int) getLong(dictionaryId);
  }

  private long getLong(int dictionaryId) {
    return dataFileReader.getLong(dictionaryId, 0);
  }

}
