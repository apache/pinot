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

public class DoubleDictionary extends ImmutableDictionaryReader  {

  public DoubleDictionary(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata) {
    super(dataBuffer, columnMetadata.getCardinality(), Double.SIZE/8);
  }

  @Override
  public int indexOf(Object rawValue) {
    Double lookup;

    if (rawValue instanceof String) {
      lookup = Double.parseDouble((String) rawValue);
    } else {
      lookup = (Double)rawValue;
    }
    return doubleIndexOf(lookup);
  }

  @Override
  public Double get(int dictionaryId) {
    return getDoubleValue(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return (long) getDoubleValue(dictionaryId);
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return dataFileReader.getDouble(dictionaryId, 0);
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return Double.toString(getDoubleValue(dictionaryId));
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (float) getDoubleValue(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (int) getDoubleValue(dictionaryId);
  }
}
