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

public class FloatDictionary extends ImmutableDictionaryReader {

  public FloatDictionary(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    super(dataBuffer, metadata.getCardinality(), Float.SIZE / 8);
  }

  @Override
  public int indexOf(Object rawValue) {
    Float lookup ;

    if (rawValue instanceof String) {
      lookup = Float.parseFloat((String) rawValue);
    } else {
      lookup = (Float) rawValue;
    }
    return floatIndexOf(lookup);
  }

  @Override
  public Float get(int dictionaryId) {
    return getFloat(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return new Float(getFloat(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return (double) getFloat(dictionaryId);
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return Float.toString(getFloat(dictionaryId));
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return getFloat(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (int) getFloat(dictionaryId);
  }

  public float getFloat(int dictionaryId) {
    return dataFileReader.getFloat(dictionaryId, 0);
  }



}
