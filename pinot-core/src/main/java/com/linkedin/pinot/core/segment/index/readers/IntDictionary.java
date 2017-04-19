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

public class IntDictionary extends ImmutableDictionaryReader {

  public IntDictionary(PinotDataBuffer dictionaryBuffer, ColumnMetadata metadata) {
    super(dictionaryBuffer, metadata.getCardinality(), Integer.SIZE / 8);
  }

  @Override
  public int indexOf(Object rawValue) {
    Integer lookup;
    if (rawValue instanceof String) {
      lookup = Integer.parseInt((String) rawValue);
    } else {
      lookup = (Integer) rawValue;
    }

    return intIndexOf(lookup);
  }

  @Override
  public Integer get(int dictionaryId) {
    return getInt(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return (long) getInt(dictionaryId);
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return (double) getInt(dictionaryId);
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return Integer.toString(getInt(dictionaryId));
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (float) getInt(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return getInt(dictionaryId);
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    dataFileReader.readIntValues(dictionaryIds, 0 /*column*/, startPos, limit, outValues, outStartPos);
  }

  private int getInt(int dictionaryId) {
    return dataFileReader.getInt(dictionaryId, 0);
  }

}
